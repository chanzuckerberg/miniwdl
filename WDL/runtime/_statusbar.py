# pyre-strict
"""
1. Process-global counters for queued/running tasks
2. Logic for filling out the log stderr "status bar" with that info
"""
import threading
import time
import datetime
import math
from contextlib import contextmanager
from typing import Optional, Callable, List, Iterator
from .._util import ANSI, RepeatTimer
from .. import _util

_counters = {
    "abort": False,
    "tasks_backlogged": 0,
    "tasks_slotted": 0,
    "tasks_running": 0,
    "tasks_running_cpu": 0,
    "tasks_running_mem_bytes": 0,
    "tasks_finished": 0,
}
_counters_lock = threading.Lock()


def task_backlogged() -> None:
    # workflow.py calls this when enqueueing a task onto the thread pool
    with _counters_lock:
        _counters["tasks_backlogged"] += 1


@contextmanager
def task_slotted() -> Iterator[None]:
    # task.py opens this context while a thread has picked up the task
    with _counters_lock:
        _counters["tasks_backlogged"] = max(0, _counters["tasks_backlogged"] - 1)
        _counters["tasks_slotted"] += 1
    try:
        yield
    finally:
        with _counters_lock:
            _counters["tasks_slotted"] -= 1
            _counters["tasks_finished"] += 1


@contextmanager
def task_running(cpu: int, mem_bytes: int) -> Iterator[None]:
    # task.py opens this context while the task container is actually running
    # it's possible for a task to succeed without this occurring (if the container exits instantly)
    with _counters_lock:
        _counters["tasks_running"] += 1
        _counters["tasks_running_cpu"] += cpu
        _counters["tasks_running_mem_bytes"] += mem_bytes
    try:
        yield
    finally:
        with _counters_lock:
            _counters["tasks_running"] -= 1
            _counters["tasks_running_cpu"] -= cpu
            _counters["tasks_running_mem_bytes"] -= mem_bytes


def abort() -> None:
    # called by runner error __init__ methods and misc exception handlers
    _counters["abort"] = True


_KITT: List[str] = [
    "▬▬     ",
    "▬▬▬    ",
    "▬▬▬▬   ",
    " ▬▬▬▬  ",
    "  ▬▬▬▬ ",
    "   ▬▬▬▬",
    "    ▬▬▬",
    "     ▬▬",
    "    ▬▬▬",
    "   ▬▬▬▬",
    "  ▬▬▬▬ ",
    " ▬▬▬▬  ",
    "▬▬▬▬   ",
    "▬▬▬    ",
]


@contextmanager
def enable(set_status: Optional[Callable[[List[str]], None]]) -> Iterator[None]:
    # set_status comes from .._util.configure_logger to set the status bar contents
    t0 = time.time()

    def update() -> None:
        if set_status:
            elapsed = time.time() - t0
            elapsed5 = int(elapsed * 5)
            spinner = ["       "]
            if not (_counters["abort"] or _util._terminating):
                spinner = [ANSI.RED, _KITT[elapsed5 % len(_KITT)], ANSI.RESET]
            elif (elapsed5 % 5) <= 2:
                # reaching into _util._terminating like that feels bad, but lets us provide this
                # feedback sooner:
                spinner = [ANSI.BRED, " ABORT ", ANSI.RESET]
            msg = (
                [
                    "    ",
                    ANSI.BOLD,
                    f"{datetime.timedelta(seconds=int(elapsed))} elapsed    ",
                    ANSI.RESET,
                ]
                + spinner
                + [
                    ANSI.BOLD,
                    "    ",
                    "tasks finished: " + str(_counters["tasks_finished"]),
                    ", ready: "
                    + str(
                        _counters["tasks_backlogged"]
                        + _counters["tasks_slotted"]
                        - _counters["tasks_running"]
                    ),
                    ", running: " + str(_counters["tasks_running"]),
                ]
            )
            if _counters["tasks_running_cpu"] or _counters["tasks_running_mem_bytes"]:
                msg += [
                    "    reserved CPUs: " + str(_counters["tasks_running_cpu"]),
                    ", RAM: "
                    + str(math.ceil(_counters["tasks_running_mem_bytes"] / (2 ** 30)))
                    + "GiB",
                ]
            set_status(msg)

    timer = RepeatTimer(0.2, update)
    timer.start()
    try:
        yield
    finally:
        set_status = None
        timer.cancel()
