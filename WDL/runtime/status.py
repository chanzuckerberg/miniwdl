# pyre-strict
import threading
import time
import datetime
import math
from contextlib import contextmanager
from typing import Optional, Callable, List, Iterator
from .._util import RepeatTimer

_SPINNER: List[str] = ["▹▹▹▹", "▸▹▹▹", "▹▸▹▹", "▹▹▸▹", "▹▹▹▸"]

_set_status: Optional[Callable[[str], None]] = None
_t0: float = 0.0
_run_name: str = ""
_counters = {
    "tasks_runnable": 0,
    "tasks_running": 0,
    "tasks_running_cpu": 0,
    "tasks_running_mem_bytes": 0,
    "tasks_finished": 0,
}
_counters_lock = threading.Lock()


def update() -> None:
    if _set_status:
        elapsed = time.time() - _t0
        spinner = _SPINNER[int(5 * elapsed) % 5]
        msg = [
            " ",
            spinner,
            f" {datetime.timedelta(seconds=int(elapsed))} elapsed ",
            spinner,
            "",
            str(_counters["tasks_finished"]),
            "tasks finished,",
            str(_counters["tasks_runnable"] - _counters["tasks_running"]),
            "runnable,",
            str(_counters["tasks_running"]),
            "running",
        ]
        if _counters["tasks_running"]:
            msg += [
                "on",
                str(_counters["tasks_running_cpu"]),
                "CPUs &",
                str(math.ceil(_counters["tasks_running_mem_bytes"] / (2 ** 30))),
                "GiB RAM reserved",
            ]
        _set_status(" ".join(msg))


@contextmanager
def enable(set_status: Callable[[str], None], run_name: str) -> Iterator[None]:
    global _set_status, _t0, _run_name
    _t0 = time.time()
    _set_status = set_status
    _run_name = run_name
    timer = RepeatTimer(0.2, update)
    timer.start()
    try:
        yield
    finally:
        _set_status = None
        timer.cancel()


@contextmanager
def task_runnable() -> Iterator[None]:
    with _counters_lock:
        _counters["tasks_runnable"] += 1
    try:
        yield
    finally:
        with _counters_lock:
            _counters["tasks_runnable"] -= 1
            _counters["tasks_finished"] += 1


@contextmanager
def task_running(cpu: int, mem_bytes: int) -> Iterator[None]:
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
