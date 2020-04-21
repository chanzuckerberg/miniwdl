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


def task_slotted() -> None:
    # task.py calls this when a thread starts run_local_task
    with _counters_lock:
        _counters["tasks_backlogged"] = max(0, _counters["tasks_backlogged"] - 1)
        _counters["tasks_slotted"] += 1


@contextmanager
def task_running(cpu: int, mem_bytes: int) -> Iterator[None]:
    # task.py opens this context while the task container is actually running
    with _counters_lock:
        assert _counters["tasks_slotted"]
        _counters["tasks_slotted"] -= 1
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
            _counters["tasks_finished"] += 1


def abort() -> None:
    # called by runner error __init__ methods and misc exception handlers
    _counters["abort"] = True


_SPINNER: List[str] = ["●▫▫▫▫", "▫●▫▫▫", "▫▫●▫▫", "▫▫▫●▫", "▫▫▫▫●", "▫▫▫●▫", "▫▫●▫▫", "▫●▫▫▫"]


@contextmanager
def enable(set_status: Optional[Callable[[str], None]]) -> Iterator[None]:
    # set_status comes from .._util.install_coloredlogs to set the status bar contents
    t0 = time.time()

    def update() -> None:
        if set_status:
            elapsed = time.time() - t0
            spinner = _SPINNER[int(5 * elapsed) % 8]
            if _counters["abort"] or _util._terminating:
                spinner = "\033[1;91mABORT" if (int(5 * elapsed) % 5) <= 2 else "     "
            msg = [
                "   ",
                spinner,
                f"   {datetime.timedelta(seconds=int(elapsed))} elapsed",
                "  ",
                "tasks finished:",
                str(_counters["tasks_finished"]) + ",",
                "ready:",
                str(_counters["tasks_backlogged"] + _counters["tasks_slotted"]) + ",",
                "running:",
                str(_counters["tasks_running"]),
            ]
            if _counters["tasks_running"]:
                msg += [
                    "   reserved CPUs:",
                    str(_counters["tasks_running_cpu"]) + ",",
                    "RAM:",
                    str(math.ceil(_counters["tasks_running_mem_bytes"] / (2 ** 30))),
                    "GiB",
                ]
            msg += ["  ", spinner]
            set_status(" ".join(msg))

    timer = _util.RepeatTimer(0.2, update)
    timer.start()
    try:
        yield
    finally:
        set_status = None
        timer.cancel()
