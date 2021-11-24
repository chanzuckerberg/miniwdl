import os
import time
import psutil
import logging
import threading
import contextlib
import subprocess
import multiprocessing
from typing import Callable, List, Tuple, Dict, Optional
from abc import abstractmethod, abstractproperty
from ..._util import PygtailLogger
from ..._util import StructuredLogMessage as _
from .. import config, _statusbar
from ..error import Terminated
from ..task_container import TaskContainer


class SubprocessBase(TaskContainer):
    """
    Abstract base class for TaskContainer implementations that call out to a CLI subprocess (such
    as `docker run`, `singularity run`, `podman run`). Subclasses take care of formulating the
    exact command line arguments for the respective implementation.
    """

    _resource_limits: Optional[Dict[str, int]] = None
    _bind_input_files: bool = True
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def detect_resource_limits(cls, cfg: config.Loader, logger: logging.Logger) -> Dict[str, int]:
        if not cls._resource_limits:
            cls._resource_limits = {
                "cpu": multiprocessing.cpu_count(),
                "mem_bytes": psutil.virtual_memory().total,
            }
            logger.info(
                _(
                    "detected host resources",
                    cpu=cls._resource_limits["cpu"],
                    mem_bytes=cls._resource_limits["mem_bytes"],
                )
            )
            _SubprocessScheduler.global_init(cls._resource_limits)
        return cls._resource_limits

    def _run(self, logger: logging.Logger, terminating: Callable[[], bool], command: str) -> int:
        with contextlib.ExitStack() as cleanup:
            # await cpu & memory availability
            cpu_reservation = self.runtime_values.get("cpu", 0)
            memory_reservation = self.runtime_values.get("memory_reservation", 0)
            scheduler = _SubprocessScheduler(cpu_reservation, memory_reservation)
            cleanup.enter_context(scheduler)
            logger.info(
                _(
                    "provisioned",
                    seconds_waited=scheduler.delay,
                    cpu=cpu_reservation,
                    mem_bytes=memory_reservation,
                )
            )

            # prepare loggers
            cli_log_filename = os.path.join(self.host_dir, f"{self.cli_name}.log.txt")
            cli_log = cleanup.enter_context(open(cli_log_filename, "wb"))
            cli_logger = logger.getChild(self.cli_name)
            poll_cli_log = cleanup.enter_context(
                PygtailLogger(
                    logger,
                    cli_log_filename,
                    lambda msg: cli_logger.info(msg.rstrip()),
                    level=logging.INFO,
                )
            )
            poll_stderr = cleanup.enter_context(
                PygtailLogger(
                    logger,
                    self.host_stderr_txt(),
                    callback=self.stderr_callback,
                )
            )

            # prepare command
            with open(os.path.join(self.host_dir, "command"), "w") as outfile:
                outfile.write(command)

            # start subprocess
            invocation = self._cli_invocation(logger) + [
                "/bin/bash",
                "-c",
                "bash ../command >> ../stdout.txt 2>> ../stderr.txt",
            ]
            proc = subprocess.Popen(
                invocation, stdout=cli_log, stderr=subprocess.STDOUT, cwd=self.host_dir
            )
            logger.notice(  # pyre-ignore
                _(f"{self.cli_name} run", pid=proc.pid, log=cli_log_filename)
            )
            cleanup.enter_context(
                _statusbar.task_running(
                    self.runtime_values.get("cpu", 0),
                    self.runtime_values.get("memory_reservation", 0),
                )
            )

            # long-poll for completion
            exit_code = None
            while exit_code is None:
                if terminating():
                    proc.terminate()
                try:
                    exit_code = proc.wait(1)
                except subprocess.TimeoutExpired:
                    pass
                poll_stderr()
                cli_log.flush()
                poll_cli_log()
            if terminating():
                raise Terminated()
            assert isinstance(exit_code, int)
            return exit_code

    @abstractproperty
    def cli_name(self) -> str:
        pass

    @abstractmethod
    def _cli_invocation(self, logger: logging.Logger) -> List[str]:
        pass

    def copy_input_files(self, logger: logging.Logger) -> None:
        assert self._bind_input_files
        super().copy_input_files(logger)
        # now that files have been copied into the working dir, it won't be necessary to bind-mount
        # them individually
        self._bind_input_files = False

    def prepare_mounts(self) -> List[Tuple[str, str, bool]]:
        def touch_mount_point(host_path: str) -> None:
            # touching each mount point ensures they'll be owned by invoking user:group
            assert host_path.startswith(self.host_dir + "/")
            if host_path.endswith("/"):
                os.makedirs(host_path, exist_ok=True)
            else:
                os.makedirs(os.path.dirname(host_path), exist_ok=True)
                with open(host_path, "x") as _:
                    pass

        mounts = []
        # mount stdout, stderr, and working directory read/write
        touch_mount_point(self.host_stdout_txt())
        mounts.append(
            (os.path.join(self.container_dir, "stdout.txt"), self.host_stdout_txt(), True)
        )
        touch_mount_point(self.host_stderr_txt())
        mounts.append(
            (os.path.join(self.container_dir, "stderr.txt"), self.host_stderr_txt(), True)
        )
        mounts.append((os.path.join(self.container_dir, "work"), self.host_work_dir(), True))
        # mount command read-only
        mounts.append(
            (
                os.path.join(self.container_dir, "command"),
                os.path.join(self.host_dir, "command"),
                False,
            )
        )
        # mount input files & directories read-only
        if self._bind_input_files:
            for host_path, container_path in self.input_path_map.items():
                assert (not container_path.endswith("/")) or os.path.isdir(host_path.rstrip("/"))
                host_mount_point = os.path.join(
                    self.host_dir, os.path.relpath(container_path.rstrip("/"), self.container_dir)
                )
                if not os.path.exists(host_mount_point):
                    touch_mount_point(
                        host_mount_point + ("/" if container_path.endswith("/") else "")
                    )
                mounts.append((container_path.rstrip("/"), host_path.rstrip("/"), False))
        return mounts


class _SubprocessScheduler(contextlib.AbstractContextManager):
    """
    Logic for scheduling parallel containers to fit host cpu & memory resources
    """

    _lock: threading.Lock = threading.Lock()
    _cv: threading.Condition
    _state: Dict[str, int] = {}
    delay: int = 0

    @classmethod
    def global_init(cls, resource_limits: Dict[str, int]):
        with cls._lock:
            cls._cv = threading.Condition(cls._lock)
            cls._state["host_cpu"] = resource_limits["cpu"]
            cls._state["host_memory"] = resource_limits["mem_bytes"]
            cls._state["used_cpu"] = 0
            cls._state["used_memory"] = 0

    def __init__(self, cpu_reservation: int, memory_reservation: int):
        assert self._cv
        assert 0 <= cpu_reservation <= self._state["host_cpu"]
        assert 0 <= memory_reservation <= self._state["host_memory"]
        self.cpu_reservation = cpu_reservation
        self.memory_reservation = memory_reservation

    def __enter__(self):
        t0 = time.time()
        with self._cv:
            while (
                self._state["used_cpu"] + self.cpu_reservation > self._state["host_cpu"]
                or self._state["used_memory"] + self.memory_reservation > self._state["host_memory"]
            ):
                self._cv.wait()
            self._state["used_cpu"] = self._state["used_cpu"] + self.cpu_reservation
            self._state["used_memory"] = self._state["used_memory"] + self.memory_reservation
        self.delay = int(time.time() - t0)

    def __exit__(self, *exc):
        with self._cv:
            self._state["used_cpu"] = self._state["used_cpu"] - self.cpu_reservation
            assert 0 <= self._state["used_cpu"] <= self._state["host_cpu"]
            self._state["used_memory"] = self._state["used_memory"] - self.memory_reservation
            assert 0 <= self._state["used_memory"] <= self._state["host_memory"]
            self._cv.notify()
