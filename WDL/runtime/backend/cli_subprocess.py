import os
import logging
import subprocess
import contextlib
from typing import Callable, List, Tuple
from abc import abstractmethod, abstractproperty
from ... import Error
from ..._util import chmod_R_plus, PygtailLogger, NOTICE_LEVEL
from ..._util import StructuredLogMessage as _
from .. import config, _statusbar
from ..error import OutputError, Interrupted, Terminated, CommandFailed
from ..task_container import TaskContainer


class SubprocessBase(TaskContainer):
    """
    Abstract base class for TaskContainer implementations that call out to a CLI subprocess (such
    as `docker run`, `singularity run`, `podman run`). Subclasses take care of formulating the
    exact command line arguments for the respective implementation.
    """

    def _run(self, logger: logging.Logger, terminating: Callable[[], bool], command: str) -> int:
        with open(os.path.join(self.host_dir, "command"), "w") as outfile:
            outfile.write(command)

        with contextlib.ExitStack() as cleanup:

            # prepare loggers
            cli_log_filename = os.path.join(self.host_dir, f"{self.cli_name}.log.txt")
            cli_log = cleanup.enter_context(open(cli_log_filename, "wb"))
            cli_logger = logger.getChild(self.cli_name)
            poll_cli_log = cleanup.enter_context(
                PygtailLogger(
                    logger,
                    cli_log_filename,
                    lambda msg: cli_logger.info(msg.rstrip()),
                    level=NOTICE_LEVEL,
                )
            )
            poll_stderr = cleanup.enter_context(
                PygtailLogger(
                    logger,
                    self.host_stderr_txt(),
                    callback=self.stderr_callback,
                )
            )

            # start subprocess
            invocation = self._cli_invocation() + [
                "/bin/bash",
                "-c",
                "bash ../command >> ../stdout.txt 2>> ../stderr.txt",
            ]
            logger.debug(_("invocation", args=invocation))
            proc = subprocess.Popen(invocation, stdout=cli_log, stderr=subprocess.STDOUT)

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
    def _cli_invocation(self) -> List[str]:
        pass

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
        mounts.append(
            (
                os.path.join(self.container_dir, "command"),
                os.path.join(self.host_dir, "command"),
                False,
            )
        )
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
        return mounts

    # TODO: common resource scheduling logic (accounting for multiple concurrent miniwdl processes?)
    #       use old container-based way of detecting houst resources
