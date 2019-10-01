# pyre-strict
"""
Local task runner
"""
import sys
import logging
import os
import json
import copy
import traceback
import glob
import signal
from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Optional
from types import FrameType

from pygtail import Pygtail
from requests.exceptions import ReadTimeout
import docker
from .. import Error, Type, Env, Expr, Value, StdLib, Tree, _util
from .._util import write_values_json, provision_run_dir, LOGGING_FORMAT
from .error import *


class TaskContainer(ABC):
    """
    Base class for task containers, subclassed by runtime-specific
    implementations (e.g. Docker).
    """

    run_id: str

    host_dir: str
    """
    :type: str

    The host path to the scratch directory that will be mounted inside the
    container.
    """

    container_dir: str
    """
    :type: str

    The scratch directory's mounted path inside the container. The task
    command's working directory will be ``{container_dir}/work/``.
    """

    input_file_map: Dict[str, str]
    """
    :type: Dict[str,str]

    A mapping of host input file paths to in-container mounted paths,
    maintained by ``add_files``.
    """

    _running: bool
    _terminate: bool

    def __init__(self, run_id: str, host_dir: str) -> None:
        self.run_id = run_id
        self.host_dir = host_dir
        self.container_dir = "/mnt/miniwdl_task_container"
        self.input_file_map = {}
        self._running = False
        self._terminate = False

    def add_files(self, host_files: List[str]) -> None:
        """
        Use before running the container to add a list of host files to mount
        inside the container as inputs. The host-to-container path mapping is
        maintained in ``input_file_map``.

        Although ``add_files`` can be used multiple times, files should be
        added together where possible, as this allows heuristics for dealing
        with any name collisions among them.
        """
        assert not self._running

        # partition the files by host directory
        host_files_by_dir = {}
        for host_file in host_files:
            host_files_by_dir.setdefault(os.path.dirname(host_file), set()).add(host_file)

        # map the files in each host directory into a unique container
        # directory. ensuring that (i) there will be no name collisions, and
        # (ii) files co-located in the same host directory will be mounted into
        # the same container directory.
        for files in host_files_by_dir.values():
            dn = str(len(self.input_file_map))
            for host_file in files:
                self.input_file_map[host_file] = os.path.join(
                    self.container_dir, "inputs", dn, os.path.basename(host_file)
                )

    def run(self, logger: logging.Logger, command: str) -> None:
        """
        1. Container is instantiated
        2. Command is executed in ``{host_dir}/work/`` (where {host_dir} is mounted to {container_dir} inside the container)
        3. Standard output is written to ``{host_dir}/stdout.txt``
        4. Standard error is written to ``{host_dir}/stderr.txt`` and logged at INFO level
        5. Raises CommandFailure for nonzero exit code, or any other error

        The container is torn down in any case, including SIGTERM/SIGHUP signal which is trapped.
        """
        # container-specific logic should be in _run(). this wrapper traps SIGTERM/SIGHUP
        # and sets self._terminate

        assert not (self._running or self._terminate)
        if command.strip():  # if the command is empty then don't bother with any of this
            signals = [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGPIPE, signal.SIGALRM]

            def handle_signal(
                signal: int,
                frame: FrameType,
                self: TaskContainer = self,
                logger: logging.Logger = logger,
            ) -> None:
                logger.critical("received termination signal {}".format(signal))
                self._terminate = True

            restore_signal_handlers = dict(
                (sig, signal.signal(sig, handle_signal)) for sig in signals
            )

            self._running = True
            try:
                exit_status = self._run(logger, command)
            finally:
                self._running = False
                for sig, handler in restore_signal_handlers.items():
                    signal.signal(sig, handler)

            if self._terminate:
                raise Terminated()
            if exit_status != 0:
                raise CommandFailure(exit_status, os.path.join(self.host_dir, "stderr.txt"))

    @abstractmethod
    def _run(self, logger: logging.Logger, command: str) -> int:
        # run command in container & return exit status
        raise NotImplementedError()

    def host_file(self, container_file: str, inputs_only: bool = False) -> str:
        """
        Map an output file's in-container path under ``container_dir`` to a host path under
        ``host_dir``

        SECURITY: this method must only return host paths under ``host_dir`` and prevent any
        reference to other host files (e.g. /etc/passwd), including via sneaky symlinks
        """
        if os.path.isabs(container_file):
            # handle output of std{out,err}.txt
            if container_file in [
                os.path.join(self.container_dir, pipe_file)
                for pipe_file in ["stdout.txt", "stderr.txt"]
            ]:
                return os.path.join(self.host_dir, os.path.basename(container_file))
            # handle output of an input file
            host_input_files = [
                host_input_file
                for (host_input_file, container_input_file) in self.input_file_map.items()
                if container_input_file == container_file
            ]
            if host_input_files:
                return host_input_files[0]
            if inputs_only:
                raise Error.InputError(
                    "task inputs attempted to use a non-input or non-existent file "
                    + container_file
                )
            # relativize the path to the provisioned working directory
            container_file = os.path.relpath(
                container_file, os.path.join(self.container_dir, "work")
            )

        host_workdir = os.path.join(self.host_dir, "work")
        ans = os.path.realpath(os.path.join(host_workdir, container_file))
        assert os.path.isabs(ans) and "/../" not in ans
        if os.path.isfile(ans):
            if ans.startswith(host_workdir + "/"):
                return ans
            raise OutputError(
                "task outputs attempted to use a file outside its working directory: "
                + container_file
            )
        raise OutputError("task output file not found: " + container_file)


class TaskDockerContainer(TaskContainer):
    """
    TaskContainer docker runtime
    """

    image_tag: str = "ubuntu:18.04"
    """
    :type: str

    docker image tag (set as desired before running)
    """

    def _run(self, logger: logging.Logger, command: str) -> int:
        with open(os.path.join(self.host_dir, "command"), "x") as outfile:
            outfile.write(command)
        pipe_files = ["stdout.txt", "stderr.txt"]
        for touch_file in pipe_files:
            with open(os.path.join(self.host_dir, touch_file), "x") as outfile:
                pass

        volumes = {}
        # mount input files and command read-only
        for host_path, container_path in self.input_file_map.items():
            volumes[host_path] = {"bind": container_path, "mode": "ro"}
        volumes[os.path.join(self.host_dir, "command")] = {
            "bind": os.path.join(self.container_dir, "command"),
            "mode": "ro",
        }
        # mount stdout, stderr, and working directory read/write
        for pipe_file in pipe_files:
            volumes[os.path.join(self.host_dir, pipe_file)] = {
                "bind": os.path.join(self.container_dir, pipe_file),
                "mode": "rw",
            }
        volumes[os.path.join(self.host_dir, "work")] = {
            "bind": os.path.join(self.container_dir, "work"),
            "mode": "rw",
        }
        logger.debug("docker volume map: " + str(volumes))

        # connect to dockerd
        client = docker.from_env()
        try:
            container = None
            exit_info = None
            stderr_file = os.path.join(self.host_dir, "stderr.txt")
            pygtail = Pygtail(stderr_file, full_lines=True)
            pygtail_exn = False
            if ":" not in self.image_tag:
                # seems we need to do this explicitly under some configurations -- issue #232
                self.image_tag += ":latest"
            try:
                # run container
                logger.info("docker starting image {}".format(self.image_tag))
                container = client.containers.run(
                    self.image_tag,
                    command=[
                        "/bin/bash",
                        "-c",
                        "/bin/bash ../command >> ../stdout.txt 2>> ../stderr.txt",
                    ],
                    detach=True,
                    auto_remove=True,
                    working_dir=os.path.join(self.container_dir, "work"),
                    volumes=volumes,
                )
                logger.debug(
                    "docker container name = {}, id = {}".format(container.name, container.id)
                )
                # long-poll for container exit
                while exit_info is None:
                    try:
                        exit_info = container.wait(timeout=1)
                    except Exception as exn:
                        if self._terminate:
                            raise Terminated() from None
                        # workaround for docker-py not throwing the exception class
                        # it's supposed to
                        s_exn = str(exn)
                        if "timed out" not in s_exn and "Timeout" not in s_exn:
                            raise
                    # stream stderr into log
                    if not pygtail_exn:
                        try:
                            for line in pygtail:
                                logger.verbose(f"2| {line.rstrip()}")
                        except:
                            pygtail_exn = True
                            # cf. https://github.com/bgreenlee/pygtail/issues/48
                            logger.info(
                                "task standard error log is incomplete due to the following exception; see %s",
                                stderr_file,
                                exc_info=sys.exc_info(),
                            )
                logger.info("container exit info = " + str(exit_info))
            except:
                # make sure to stop & clean up the container if we're stopping due
                # to SIGTERM or something. Most other cases should be handled by
                # auto_remove.
                if container:
                    try:
                        container.remove(force=True)
                        logger.info("force-removed docker container")
                    except Exception as exn:
                        logger.exception("failed to remove docker container")
                raise

            # retrieve and check container exit status
            assert exit_info
            if "StatusCode" not in exit_info:
                raise CommandFailure(
                    (-sys.maxsize - 1),
                    os.path.join(self.host_dir, "stderr.txt"),
                    "docker finished without reporting exit status in: " + str(exit_info),
                )
            return exit_info["StatusCode"]
        finally:
            try:
                client.close()
            except:
                logger.exception("failed to close docker-py client")
            # log the final stderr lines
            if not pygtail_exn:
                try:
                    for line in Pygtail(stderr_file, full_lines=False):
                        logger.verbose(f"2| {line.rstrip()}")
                except:
                    # cf. https://github.com/bgreenlee/pygtail/issues/48
                    logger.info(
                        "task standard error log is incomplete due to the following exception; see %s",
                        stderr_file,
                        exc_info=sys.exc_info(),
                    )


def run_local_task(
    task: Tree.Task,
    posix_inputs: Env.Bindings[Value.Base],
    run_id: Optional[str] = None,
    run_dir: Optional[str] = None,
) -> Tuple[str, Env.Bindings[Value.Base]]:
    """
    Run a task locally.

    Inputs shall have been typechecked already. File inputs are presumed to be local POSIX file
    paths that can be mounted into a container.

    :param run_id: unique ID for the run, defaults to workflow name
    :param run_dir: outputs and scratch will be stored in this directory if it doesn't already
                    exist; if it does, a timestamp-based subdirectory is created and used (defaults
                    to current working directory)
    """

    run_id = run_id or task.name
    run_dir = provision_run_dir(task.name, run_dir)
    logger = logging.getLogger("wdl-task:" + run_id)
    fh = logging.FileHandler(os.path.join(run_dir, "task.log"))
    fh.setFormatter(logging.Formatter(LOGGING_FORMAT))
    logger.addHandler(fh)
    _util.install_coloredlogs(logger)
    logger.notice(  # pyre-fixme
        "starting task %s (%s Ln %d Col %d) in %s",
        task.name,
        task.pos.uri,
        task.pos.line,
        task.pos.column,
        run_dir,
    )
    write_values_json(posix_inputs, os.path.join(run_dir, "inputs.json"))

    try:
        # create appropriate TaskContainer
        container = TaskDockerContainer(run_id, run_dir)

        # evaluate input/postinput declarations, including mapping from host to
        # in-container file paths
        container_env = _eval_task_inputs(logger, task, posix_inputs, container)

        # evaluate runtime.docker
        image_tag_expr = task.runtime.get("docker", None)
        if image_tag_expr:
            assert isinstance(image_tag_expr, Expr.Base)
            container.image_tag = image_tag_expr.eval(container_env).value

        # interpolate command
        command = _util.strip_leading_whitespace(
            task.command.eval(container_env, stdlib=InputStdLib(container)).value
        )[1]
        logger.debug("command:\n%s", command.rstrip())

        # start container & run command
        container.run(logger, command)

        # evaluate output declarations
        outputs = _eval_task_outputs(logger, task, container_env, container)

        write_values_json(outputs, os.path.join(run_dir, "outputs.json"))
        logger.notice("done")  # pyre-fixme
        return (run_dir, outputs)
    except Exception as exn:
        logger.debug(traceback.format_exc())
        wrapper = TaskFailure(task, run_id, run_dir)
        msg = str(wrapper)
        if hasattr(exn, "job_id"):
            msg += " evaluating " + getattr(exn, "job_id")
        msg += ": " + exn.__class__.__name__
        if str(exn):
            msg += ", " + str(exn)
        logger.error(msg)
        logger.info("run directory: %s", run_dir)
        raise wrapper from exn


def _eval_task_inputs(
    logger: logging.Logger,
    task: Tree.Task,
    posix_inputs: Env.Bindings[Value.Base],
    container: TaskContainer,
) -> Env.Bindings[Value.Base]:
    # Map all the provided input Files to in-container paths
    # First make a pass to collect all the host paths and pass them to the
    # container as a group (so that it can deal with any basename collisions)
    host_files = []

    def collect_host_files(v: Value.Base) -> None:
        if isinstance(v, Value.File):
            host_files.append(v.value)
        for ch in v.children:
            collect_host_files(ch)

    for binding in posix_inputs:
        collect_host_files(binding.value)
    container.add_files(host_files)

    # copy posix_inputs with all Files mapped to their in-container paths
    def map_files(v: Value.Base) -> Value.Base:
        if isinstance(v, Value.File):
            v.value = container.input_file_map[v.value]
        for ch in v.children:
            map_files(ch)
        return v

    container_inputs = posix_inputs.map(
        lambda binding: Env.Binding(binding.name, map_files(copy.deepcopy(binding.value)))
    )

    # initialize value environment with the inputs
    container_env = Env.Bindings()
    for b in container_inputs:
        assert isinstance(b, Env.Binding)
        v = b.value
        assert isinstance(v, Value.Base)
        container_env = container_env.bind(b.name, v)
        vj = json.dumps(v.json)
        logger.info("input {} -> {}".format(b.name, vj if len(vj) < 4096 else "(large)"))

    # collect remaining declarations requiring evaluation.
    decls_to_eval = []
    for decl in (task.inputs or []) + (task.postinputs or []):
        if not container_env.has_binding(decl.name):
            decls_to_eval.append(decl)

    # topsort them according to internal dependencies. prior static validation
    # should have ensured they're acyclic.
    decls_by_id, decls_adj = Tree._decl_dependency_matrix(decls_to_eval)
    decls_to_eval = [decls_by_id[did] for did in _util.topsort(decls_adj)]
    assert len(decls_by_id) == len(decls_to_eval)

    # evaluate each declaration in that order
    # note: the write_* functions call container.add_files as a side-effect
    stdlib = InputStdLib(container)
    for decl in decls_to_eval:
        assert isinstance(decl, Tree.Decl)
        v = Value.Null()
        if decl.expr:
            try:
                v = decl.expr.eval(container_env, stdlib=stdlib).coerce(decl.type)
            except Error.RuntimeError as exn:
                setattr(exn, "job_id", decl.workflow_node_id)
                raise exn
            except Exception as exn:
                exn2 = Error.EvalError(decl, str(exn))
                setattr(exn2, "job_id", decl.workflow_node_id)
                raise exn2 from exn
        else:
            assert decl.type.optional
        vj = json.dumps(v.json)
        logger.info("eval {} -> {}".format(decl.name, vj if len(vj) < 4096 else "(large)"))
        container_env = container_env.bind(decl.name, v)

    return container_env


def _eval_task_outputs(
    logger: logging.Logger, task: Tree.Task, env: Env.Bindings[Value.Base], container: TaskContainer
) -> Env.Bindings[Value.Base]:

    stdlib = OutputStdLib(container)
    outputs = Env.Bindings()
    for decl in task.outputs:
        assert decl.expr
        try:
            v = decl.expr.eval(env, stdlib=stdlib).coerce(decl.type)
        except Error.RuntimeError as exn:
            setattr(exn, "job_id", decl.workflow_node_id)
            raise exn
        except Exception as exn:
            exn2 = Error.EvalError(decl, str(exn))
            setattr(exn2, "job_id", decl.workflow_node_id)
            raise exn2 from exn
        logger.info("output {} -> {}".format(decl.name, json.dumps(v.json)))
        outputs = outputs.bind(decl.name, v)
        env = env.bind(decl.name, v)

    # map Files from in-container paths to host paths
    def map_files(v: Value.Base) -> Value.Base:
        if isinstance(v, Value.File):
            host_file = container.host_file(v.value)
            logger.debug("container output file %s -> host %s", v.value, host_file)
            v.value = host_file
        for ch in v.children:
            map_files(ch)
        return v

    return outputs.map(
        lambda binding: Env.Binding(binding.name, map_files(copy.deepcopy(binding.value)))
    )


class _StdLib(StdLib.Base):
    container: TaskContainer
    inputs_only: bool  # if True then only permit access to input files

    def __init__(self, container: TaskContainer, inputs_only: bool) -> None:
        super().__init__(write_dir=os.path.join(container.host_dir, "write_"))
        self.container = container
        self.inputs_only = inputs_only

    def _devirtualize_filename(self, filename: str) -> str:
        # check allowability of reading this file, & map from in-container to host
        return self.container.host_file(filename, inputs_only=self.inputs_only)

    def _virtualize_filename(self, filename: str) -> str:
        # register new file with container input_file_map
        self.container.add_files([filename])
        return self.container.input_file_map[filename]


class InputStdLib(_StdLib):
    # StdLib for evaluation of task inputs and command
    def __init__(self, container: TaskContainer) -> None:
        super().__init__(container, True)


class OutputStdLib(_StdLib):
    # StdLib for evaluation of task outputs
    def __init__(self, container: TaskContainer) -> None:
        super().__init__(container, False)

        setattr(
            self,
            "stdout",
            StdLib.StaticFunction(
                "stdout",
                [],
                Type.File(),
                lambda: Value.File(os.path.join(self.container.container_dir, "stdout.txt")),
            ),
        )
        setattr(
            self,
            "stderr",
            StdLib.StaticFunction(
                "stderr",
                [],
                Type.File(),
                lambda: Value.File(os.path.join(self.container.container_dir, "stderr.txt")),
            ),
        )

        def _glob(pattern: Value.String, lib: OutputStdLib = self) -> Value.Array:
            pat = pattern.coerce(Type.String()).value
            if not pat:
                raise OutputError("empty glob() pattern")
            assert isinstance(pat, str)
            if pat[0] == "/":
                raise OutputError("glob() pattern must be relative to task working directory")
            if pat.startswith("..") or "/.." in pat:
                raise OutputError("glob() pattern must not use .. uplevels")
            if pat.startswith("./"):
                pat = pat[2:]
            # glob the host directory
            pat = os.path.join(lib.container.host_dir, "work", pat)
            host_files = sorted(fn for fn in glob.glob(pat) if os.path.isfile(fn))
            # convert the host filenames to in-container filenames
            container_files = []
            for hf in host_files:
                dstrip = lib.container.host_dir
                dstrip += "" if dstrip.endswith("/") else "/"
                assert hf.startswith(dstrip)
                container_files.append(os.path.join(lib.container.container_dir, hf[len(dstrip) :]))
            return Value.Array(Type.File(), [Value.File(fn) for fn in container_files])

        setattr(
            self,
            "glob",
            StdLib.StaticFunction("glob", [Type.String()], Type.Array(Type.File()), _glob),
        )
