"""
Abstract interface for task container runtime + default Docker Swarm backend
"""
import os
import logging
import shutil
import threading
from typing import Callable, Iterable, List, Set, Tuple, Type, Any, Dict, Optional
from abc import ABC, abstractmethod
from contextlib import suppress
from .. import Error
from .._util import (
    TerminationSignalFlag,
    path_really_within,
    rmtree_atomic,
)
from .._util import StructuredLogMessage as _
from . import config
from .error import OutputError, Terminated, CommandFailed


class TaskContainer(ABC):
    """
    Base class for task containers, subclassed by runtime-specific backends (e.g. Docker).
    """

    # class stuff

    @classmethod
    def global_init(cls, cfg: config.Loader, logger: logging.Logger) -> None:
        """
        Perform any necessary one-time initialization of the underlying container backend. Must be
        invoked once per process prior to any instantiation of the class.
        """
        raise NotImplementedError()

    @classmethod
    def detect_resource_limits(cls, cfg: config.Loader, logger: logging.Logger) -> Dict[str, int]:
        """
        Detect the maximum resources (cpu and mem_bytes) that the underlying container backend
        would be able to provision.

        If determining this is at all costly, then backend should memoize (thread-safely and
        perhaps front-loaded in global_init).
        """
        raise NotImplementedError()

    # instance stuff

    run_id: str

    host_dir: str
    """
    :type: str

    The run directory (on the host)
    """

    container_dir: str
    """
    :type: str

    The scratch directory inside the container. The task command's working directory will be
    ``{container_dir}/work/``.
    """

    input_path_map: Dict[str, str]
    """
    :type: Dict[str,str]

    A mapping of host input file/directory paths to in-container mounted paths, maintained by
    ``add_paths``. Directory paths are distinguished by trailing slashes on both keys and values;
    the slashes often should be trimmed for use elsewhere.
    """

    input_path_map_rev: Dict[str, str]

    try_counter: int
    """
    :type: int

    Counter for number of retries; starts at 1 on the first attempt. On subsequent attempts, the
    names (on the host) of the working directory, stdout.txt, and stderr.txt will incorporate the
    count, to ensure their uniqueness.
    """

    runtime_values: Dict[str, Any]
    """
    Evaluted task runtime{} section. Typically the TaskContainer backend needs to honor cpu,
    memory_limit, memory_reservation, docker. Resources must have already been fit to
    get_resource_limits(). Retry logic (maxRetries, preemptible) is handled externally.
    """

    stderr_callback: Optional[Callable[[str], None]]
    """
    A function called line-by-line for the task's standard error stream, iff verbose logging is
    enabled. If provided by a plugin then it overrides the default standard error logging, which
    writes each line to the 'stderr' child of the task logger.
    """

    failure_info: Optional[Dict[str, Any]]
    """
    Upon run failure, the implementation may provide additional structured information about what
    went wrong (beyond the exit code and log messages).
    """

    _running: bool

    def __init__(self, cfg: config.Loader, run_id: str, host_dir: str) -> None:
        self.cfg = cfg
        self.run_id = run_id
        self.host_dir = host_dir
        self.container_dir = "/mnt/miniwdl_task_container"
        self.input_path_map = {}
        self.input_path_map_rev = {}
        self.stderr_callback = None
        self.try_counter = 1
        self._running = False
        self.runtime_values = {}
        self.failure_info = None
        os.makedirs(self.host_work_dir())

    def add_paths(self, host_paths: Iterable[str]) -> None:
        """
        Use before running the container to add a list of host paths to mount inside the container
        as inputs. Directory paths should have a trailing slash. The host-to-container path mapping
        is maintained in ``input_path_map``.

        Although ``add_paths`` can be used multiple times, paths should be added together where
        possible, as this allows heuristics for dealing with any name collisions among them.
        """
        assert not self._running

        # partition the files by host directory
        host_paths_by_dir = {}
        for host_path in host_paths:
            host_path_strip = host_path.rstrip("/")
            if host_path not in self.input_path_map and host_path_strip not in self.input_path_map:
                if not os.path.exists(host_path_strip):
                    raise Error.InputError("input path not found: " + host_path)
                host_paths_by_dir.setdefault(os.path.dirname(host_path_strip), set()).add(host_path)

        # for each such partition of files
        # - if there are no basename collisions under input subdirectory 0, then mount them there.
        # - otherwise, mount them in a fresh subdirectory
        for paths in host_paths_by_dir.values():
            based = os.path.join(self.container_dir, "work/_miniwdl_inputs")
            subd = "0"
            for host_path in paths:
                container_path = os.path.join(based, subd, os.path.basename(host_path.rstrip("/")))
                if host_path.endswith("/"):
                    container_path += "/"
                if container_path in self.input_path_map_rev:
                    assert subd == "0"
                    subd = str(len(self.input_path_map) + 1)
            for host_path in paths:
                container_path = os.path.join(based, subd, os.path.basename(host_path.rstrip("/")))
                if host_path.endswith("/"):
                    container_path += "/"
                assert container_path not in self.input_path_map_rev
                self.input_path_map[host_path] = container_path
                self.input_path_map_rev[container_path] = host_path

    def copy_input_files(self, logger: logging.Logger) -> None:
        # After add_paths has been used as needed, copy the input files from their original
        # locations to the appropriate subdirectories of the container working directory. This may
        # not be necessary e.g. if the container backend supports bind-mounting the input
        # files from their original host paths.
        # called once per task run (attempt)
        for host_path, container_path in self.input_path_map.items():
            assert container_path.startswith(self.container_dir)
            host_copy_path = os.path.join(
                self.host_dir, os.path.relpath(container_path.rstrip("/"), self.container_dir)
            )

            logger.info(_("copy host input file", input=host_path, copy=host_copy_path))
            os.makedirs(os.path.dirname(host_copy_path), exist_ok=True)
            if host_path.endswith("/"):
                shutil.copytree(host_path.rstrip("/"), host_copy_path, symlinks=False)
            else:
                shutil.copy(host_path, host_copy_path)

    def run(self, logger: logging.Logger, command: str) -> None:
        """
        1. Container is instantiated with the configured mounts and resources
        2. The mounted directory and all subdirectories have u+rwx,g+rwx permission bits; all files
           within have u+rw,g+rw permission bits.
        3. Command is executed in host_work_dir() which is mounted to {container_dir}/work inside
           the container.
        4. Standard output is written to host_stdout_txt()
        5. Standard error is written to host_stderr_txt() and logged at VERBOSE level
        6. Raises CommandFailed for nonzero exit code
        7. Raises Terminated if TerminationSignalFlag detected, or Interrupted if the backend
           cancels on us for some reason that isn't our fault.

        The container is torn down in any case, including SIGTERM/SIGHUP signal which is trapped.
        """
        # container-specific logic should be in _run(). this wrapper traps signals

        assert not self._running
        if command.strip():  # if the command is empty then don't bother with any of this
            with TerminationSignalFlag(logger) as terminating:
                if terminating():
                    raise Terminated(quiet=True)
                self._running = True
                try:
                    exit_code = self._run(logger, terminating, command)
                finally:
                    self._running = False

                if not self.success_exit_code(exit_code):
                    raise CommandFailed(
                        exit_code,
                        self.host_stderr_txt(),
                        self.host_stdout_txt(),
                        more_info=self.failure_info,
                    ) if not terminating() else Terminated()

    @abstractmethod
    def _run(self, logger: logging.Logger, terminating: Callable[[], bool], command: str) -> int:
        """
        Implementation-specific: run command in container & return exit status.

        Take care to write informative log messages for any backend-specific errors. Miniwdl's
        outer exception handler will only emit a brief, generic log message about the run failing.
        """
        # run command in container & return exit status
        raise NotImplementedError()

    def success_exit_code(self, exit_code: int) -> bool:
        if "returnCodes" not in self.runtime_values:
            return exit_code == 0
        rcv = self.runtime_values["returnCodes"]
        if isinstance(rcv, str) and rcv == "*":
            return True
        return exit_code in (rcv if isinstance(rcv, list) else [rcv])

    def delete_work(self, logger: logging.Logger, delete_streams: bool = False) -> None:
        """
        After the container exits, delete all filesystem traces of it except for task.log. That
        includes successful output files!

        delete_streams: if True, delete stdout.txt and stderr.txt as well
        """
        to_delete = [self.host_work_dir(), os.path.join(self.host_dir, "write_")]
        to_delete.append(os.path.join(self.host_dir, "command"))
        if delete_streams:
            to_delete.append(self.host_stdout_txt())
            to_delete.append(self.host_stderr_txt())
            to_delete.append(self.host_stderr_txt() + ".offset")
        deleted = []
        for p in to_delete:
            if os.path.isdir(p):
                rmtree_atomic(p)
                deleted.append(p)
            elif os.path.isfile(p):
                with suppress(FileNotFoundError):
                    os.unlink(p)
                deleted.append(p)
        if deleted:
            logger.info(_("deleted task work artifacts", artifacts=deleted))

    def reset(self, logger: logging.Logger) -> None:
        """
        After a container/command failure, reset the working directory state so that
        copy_input_files() and run() can be retried.
        """
        self.try_counter += 1
        os.makedirs(self.host_work_dir())

    def host_path(self, container_path: str, inputs_only: bool = False) -> Optional[str]:
        """
        Map the in-container path of an output File/Directory to a host path under ``host_dir``.
        Directory paths should be given a trailing "/". Return None if the path does not exist.

        SECURITY: except for inputs, this method must only return host paths under ``host_dir``
        and prevent any reference to other host files (e.g. /etc/passwd), including via symlinks.
        """
        if os.path.isabs(container_path):
            # handle output of std{out,err}.txt
            if container_path == os.path.join(self.container_dir, "stdout.txt"):
                return self.host_stdout_txt()
            if container_path == os.path.join(self.container_dir, "stderr.txt"):
                return self.host_stderr_txt()
            # handle output of an input File or Directory
            if container_path in self.input_path_map_rev:
                return self.input_path_map_rev[container_path]
            # handle output of a File or subDirectory found within an input Directory
            container_path_components = container_path.strip("/").split("/")
            for i in range(len(container_path_components) - 1, 5, -1):
                # 5 == len(['mnt', 'miniwdl_task_container', 'work', '_miniwdl_inputs', '0'])
                container_path_prefix = "/" + "/".join(container_path_components[:i]) + "/"
                if container_path_prefix in self.input_path_map_rev:
                    ans = self.input_path_map_rev[container_path_prefix]
                    ans += "/".join(container_path_components[i:])
                    if container_path.endswith("/"):
                        ans += "/"
                    assert path_really_within(ans, self.input_path_map_rev[container_path_prefix])
                    return ans
            if inputs_only:
                raise Error.InputError(
                    "task inputs attempted to use a non-input or non-existent path "
                    + container_path
                )
            # relativize the path to the provisioned working directory
            container_relpath = os.path.relpath(
                container_path, os.path.join(self.container_dir, "work")
            )
            if container_path.endswith("/") and not container_relpath.endswith("/"):
                container_relpath += "/"
            container_path = container_relpath

        ans = os.path.join(self.host_work_dir(), container_path)
        if container_path.endswith("/") and not ans.endswith("/"):
            ans += "/"
        if not (
            (container_path.endswith("/") and os.path.isdir(ans))
            or (not container_path.endswith("/") and os.path.isfile(ans))
        ):
            return None
        if not path_really_within(ans, self.host_work_dir()):
            raise OutputError(
                "task outputs attempted to use a path outside its working directory: "
                + container_path
            )
        if (
            ans.endswith("/")
            and self.input_path_map
            and (
                path_really_within(self.host_work_dir(), ans[:-1])
                or path_really_within(
                    ans[:-1], os.path.join(self.host_work_dir(), "_miniwdl_inputs")
                )
            )
        ):
            # prevent output of an input mount point
            raise OutputError("unusable output directory: " + container_path)
        return ans

    def host_work_dir(self):
        return os.path.join(
            self.host_dir, f"work{self.try_counter if self.try_counter > 1 else ''}"
        )

    def host_stdout_txt(self):
        return os.path.join(
            self.host_dir, f"stdout{self.try_counter if self.try_counter > 1 else ''}.txt"
        )

    def host_stderr_txt(self):
        return os.path.join(
            self.host_dir, f"stderr{self.try_counter if self.try_counter > 1 else ''}.txt"
        )


_backends: Dict[str, Type[TaskContainer]] = dict()
_backends_lock: threading.Lock = threading.Lock()


def new(cfg: config.Loader, logger: logging.Logger, run_id: str, host_dir: str) -> TaskContainer:
    """
    Instantiate a TaskContainer from the configured backend, including any necessary global
    initialization.
    """
    global _backends
    with _backends_lock:
        if not _backends:
            for plugin_name, plugin_cls in config.load_plugins(cfg, "container_backend"):
                _backends[plugin_name] = plugin_cls  # pyre-fixme
        backend_cls = _backends[cfg["scheduler"]["container_backend"]]
        if not getattr(backend_cls, "_global_init", False):
            backend_cls.global_init(cfg, logger)
            setattr(backend_cls, "_global_init", True)
        ans = backend_cls(cfg, run_id, host_dir)
        assert isinstance(ans, TaskContainer)
        return ans
