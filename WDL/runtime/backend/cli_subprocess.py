import os
import logging
import threading
import contextlib
import subprocess
from typing import Callable, List, Tuple
from abc import abstractmethod, abstractproperty
from ... import Error
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

    _bind_input_files: bool = True
    _lock = threading.Lock()

    def _run(self, logger: logging.Logger, terminating: Callable[[], bool], command: str) -> int:
        with contextlib.ExitStack() as cleanup:
            # global lock to run one container at a time
            # (to be replaced by resource scheduling logic)
            cleanup.enter_context(self._lock)

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
            proc = subprocess.Popen(invocation, stdout=cli_log, stderr=subprocess.STDOUT)
            logger.notice(  # pyre-ignore
                _(f"{self.cli_name} run", pid=proc.pid, log=cli_log_filename)
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

    # TODO: common resource scheduling logic (accounting for multiple concurrent miniwdl processes?)
    #       use old container-based way of detecting houst resources
