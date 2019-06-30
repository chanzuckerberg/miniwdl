# pyre-strict
import logging
import os
import tempfile
import json
import copy
import traceback
import glob
import signal
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Optional, Callable, BinaryIO
from requests.exceptions import ReadTimeout
import docker
import WDL


class CommandError(WDL.Error.RuntimeError):
    pass


class Terminated(WDL.Error.RuntimeError):
    pass


class OutputError(WDL.Error.RuntimeError):
    pass


class TaskFailure(WDL.Error.RuntimeError):
    task_name: str
    task_id: str

    def __init__(self, task_name: str, task_id: str) -> None:
        super().__init__("task {} ({}) failed".format(task_name, task_id))
        self.task_name = task_name
        self.task_id = task_id


class TaskContainer(ABC):
    """
    Base class for task containers, subclassed by runtime-specific
    implementations (e.g. Docker).
    """

    task_id: str

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

    def __init__(self, task_id: str, host_dir: str) -> None:
        self.task_id = task_id
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
        5. Raises CommandError for nonzero exit code, or any other error

        The container is torn down in any case, including SIGTERM/SIGHUP signal which is trapped.
        """
        # container-specific logic should be in _run(). this wrapper traps SIGTERM/SIGHUP
        # and sets self._terminate

        assert not (self._running or self._terminate)
        if command.strip():  # if the command is empty then don't bother with any of this
            signals = [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGPIPE, signal.SIGALRM]
            def handle_signal(signal, frame, self: TaskContainer=self, logger: logging.Logger=logger):
                logger.critical("received termination signal {}".format(signal))
                self._terminate = True
            restore_signal_handlers = dict((sig, signal.signal(sig, handle_signal)) for sig in signals)

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
                raise CommandError("command exit status = " + str(exit_status))

    @abstractmethod
    def _run(self, logger: logging.Logger, command: str) -> int:
        # run command in container & return exit status
        raise NotImplementedError()

    def host_file(self, container_file: str, inputs_only: bool = False) -> str:
        """
        Map an output file's in-container path under ``container_dir`` to a
        host path.
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
                raise WDL.Error.InputError(
                    "task inputs attempted to use a non-input or non-existent file "
                    + container_file
                )
            # otherwise make sure the file is in/under the working directory
            dpfx = os.path.join(self.container_dir, "work") + "/"
            if not container_file.startswith(dpfx):
                raise OutputError(
                    "task outputs attempted to use a file outside its working directory: "
                    + container_file
                )
            # turn it into relative path
            container_file = container_file[len(dpfx) :]
        if container_file.startswith("..") or "/.." in container_file:
            raise OutputError(
                "task outputs attempted to use file path with .. uplevels: " + container_file
            )
        # join the relative path to the host working directory
        ans = os.path.join(self.host_dir, "work", container_file)
        if not os.path.isfile(ans) or os.path.islink(ans):
            raise OutputError("task output file not found: " + container_file)
        return ans


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
                        # TODO: tail stderr.txt into logger
                        if self._terminate:
                            raise Terminated() from None
                        # workaround for docker-py not throwing the exception class
                        # it's supposed to
                        s_exn = str(exn)
                        if "timed out" not in s_exn and "Timeout" not in s_exn:
                            raise
                logger.info("container exit info = " + str(exit_info))
            except:
                # make sure to stop & clean up the container if we're stopping due
                # to SIGTERM or something. Most other cases should be handled by
                # auto_remove.
                if container:
                    try:
                        container.remove(force=True)
                    except Exception as exn:
                        logger.error("failed to remove docker container: " + str(exn))
                    logger.info("force-removed docker container")
                raise

            # retrieve and check container exit status
            assert exit_info
            if "StatusCode" not in exit_info:
                raise CommandError(
                    "docker finished without reporting exit status in: " + str(exit_info)
                )
            return exit_info["StatusCode"]
        finally:
            try:
                client.close()
            except:
                logger.error("failed to close docker-py client")


def run_local_task(
    task: WDL.Task,
    posix_inputs: WDL.Env.Values,
    task_id: Optional[str] = None,
    parent_dir: Optional[str] = None,
) -> Tuple[str, WDL.Env.Values]:
    """
    Run a task locally.

    Inputs shall have been typechecked already.

    File inputs are presumed to be local POSIX file paths that can be mounted into a container
    """

    parent_dir = parent_dir or os.getcwd()

    # formulate task ID & provision local directory
    if task_id:
        run_dir = os.path.join(parent_dir, task_id)
        os.makedirs(run_dir, exist_ok=False)
    else:
        now = datetime.today()
        task_id = now.strftime("%Y%m%d_%H%M%S") + "_" + task.name
        try:
            run_dir = os.path.join(parent_dir, task_id)
            os.makedirs(run_dir, exist_ok=False)
        except FileExistsError:
            task_id = now.strftime("%Y%m%d_%H%M%S_") + str(now.microsecond) + "_" + task.name
            run_dir = os.path.join(parent_dir, task_id)
            os.makedirs(run_dir, exist_ok=False)

    # provision logger
    logger = logging.getLogger("miniwdl_task:" + task_id)
    logger.info("starting task")
    logger.debug("task run directory " + run_dir)

    try:
        # create appropriate TaskContainer
        container = TaskDockerContainer(task_id, run_dir)

        # evaluate input/postinput declarations, including mapping from host to
        # in-container file paths
        container_env = _eval_task_inputs(logger, task, posix_inputs, container)

        # evaluate runtime.docker
        image_tag_expr = task.runtime.get("docker", None)
        if image_tag_expr:
            assert isinstance(image_tag_expr, WDL.Expr.Base)
            container.image_tag = image_tag_expr.eval(posix_inputs).value

        # interpolate command
        command = WDL._util.strip_leading_whitespace(
            task.command.eval(container_env, stdlib=InputStdLib(container)).value
        )[1]

        # start container & run command
        container.run(logger, command)

        # evaluate output declarations
        outputs = _eval_task_outputs(logger, task, container_env, container)

        logger.info("done")
        return (run_dir, outputs)
    except Exception as exn:
        logger.debug(traceback.format_exc())
        wrapper = TaskFailure(task.name, task_id)
        msg = "{}: {}".format(str(wrapper), exn.__class__.__name__)
        if str(exn):
            msg += ", " + str(exn)
        logger.error(msg)
        raise wrapper from exn


def _eval_task_inputs(
    logger: logging.Logger, task: WDL.Task, posix_inputs: WDL.Env.Values, container: TaskContainer
) -> WDL.Env.Values:
    # Map all the provided input Files to in-container paths
    # First make a pass to collect all the host paths and pass them to the
    # container as a group (so that it can deal with any basename collisions)
    host_files = []

    def collect_host_files(v: WDL.Value.Base) -> None:
        if isinstance(v, WDL.Value.File):
            host_files.append(v.value)
        for ch in v.children:
            collect_host_files(ch)

    WDL.Env.map(posix_inputs, lambda namespace, binding: collect_host_files(binding.rhs))
    container.add_files(host_files)

    # copy posix_inputs with all Files mapped to their in-container paths
    def map_files(v: WDL.Value.Base) -> WDL.Value.Base:
        if isinstance(v, WDL.Value.File):
            v.value = container.input_file_map[v.value]
        for ch in v.children:
            map_files(ch)
        return v

    container_inputs = WDL.Env.map(
        posix_inputs, lambda namespace, binding: map_files(copy.deepcopy(binding.rhs))
    )

    # initialize value environment with the inputs
    container_env = []
    for b in container_inputs:
        assert isinstance(b, WDL.Env.Binding)
        v = b.rhs
        assert isinstance(v, WDL.Value.Base)
        container_env = WDL.Env.bind(container_env, [], b.name, v)
        vj = json.dumps(v.json)
        logger.info("input {} -> {}".format(b.name, vj if len(vj) < 4096 else "(large)"))

    # collect remaining declarations requiring evaluation.
    decls_to_eval = []
    for decl in (task.inputs or []) + (task.postinputs or []):
        try:
            WDL.Env.resolve(container_env, [], decl.name)
        except KeyError:
            decls_to_eval.append(decl)

    # topsort them according to internal dependencies. prior static validation
    # should have ensured they're acyclic.
    decls_by_id, decls_adj = WDL.Tree._dependency_matrix(decls_to_eval, exclusive=True)
    decls_to_eval = [decls_by_id[did] for did in WDL._util.topsort(decls_adj)]

    # evaluate each declaration in that order
    # note: the write_* functions call container.add_files as a side-effect
    stdlib = InputStdLib(container)
    for decl in decls_to_eval:
        assert isinstance(decl, WDL.Tree.Decl)
        v = WDL.Value.Null()
        if decl.expr:
            try:
                v = decl.expr.eval(container_env, stdlib=stdlib).coerce(decl.type)
            except WDL.Error.RuntimeError:
                raise
            except Exception as exn:
                raise WDL.Error.EvalError(decl, str(exn)) from exn
        else:
            assert decl.type.optional
        vj = json.dumps(v.json)
        logger.info("eval {} -> {}".format(decl.name, vj if len(vj) < 4096 else "(large)"))
        container_env = WDL.Env.bind(container_env, [], decl.name, v)

    return container_env


def _eval_task_outputs(
    logger: logging.Logger, task: WDL.Task, env: WDL.Env.Values, container: TaskContainer
) -> WDL.Env.Values:

    outputs = []
    for decl in task.outputs:
        assert decl.expr
        try:
            v = decl.expr.eval(env, stdlib=OutputStdLib(container)).coerce(decl.type)
        except WDL.Error.RuntimeError:
            raise
        except Exception as exn:
            raise WDL.Error.EvalError(decl, str(exn)) from exn
        logger.info("output {} -> {}".format(decl.name, json.dumps(v.json)))
        outputs = WDL.Env.bind(outputs, [], decl.name, v)
        env = WDL.Env.bind(env, [], decl.name, v)

    # map Files from in-container paths to host paths
    def map_files(v: WDL.Value.Base) -> WDL.Value.Base:
        if isinstance(v, WDL.Value.File):
            host_file = container.host_file(v.value)
            logger.debug("File {} -> {}".format(v.value, host_file))
            v.value = host_file
        for ch in v.children:
            map_files(ch)
        return v

    return WDL.Env.map(outputs, lambda namespace, binding: map_files(copy.deepcopy(binding.rhs)))


class _StdLib(WDL.StdLib.Base):
    # implements the various task-specific standard library functions
    container: TaskContainer
    inputs_only: bool  # if True then only permit access to input files

    def __init__(self, container: TaskContainer, inputs_only: bool) -> None:
        super().__init__()
        self.container = container
        self.inputs_only = inputs_only

        self._override("size", _Size(self))

        def _read_something(
            parse: Callable[[str], WDL.Value.Base], lib: _StdLib = self
        ) -> Callable[[WDL.Value.File], WDL.Value.Base]:
            def _f(
                container_file: WDL.Value.File,
                parse: Callable[[str], WDL.Value.Base] = parse,
                lib: _StdLib = lib,
            ) -> WDL.Value.Base:
                host_file = lib.container.host_file(container_file.value, lib.inputs_only)
                with open(host_file, "r") as infile:
                    return parse(infile.read())

            return _f

        self._override_static("read_string", _read_something(lambda s: WDL.Value.String(s)))
        self._override_static("read_int", _read_something(lambda s: WDL.Value.Int(int(s))))
        self._override_static("read_float", _read_something(lambda s: WDL.Value.Float(float(s))))

        def _parse_boolean(s: str) -> WDL.Value.Boolean:
            s = s.rstrip()
            if s == "true":
                return WDL.Value.Boolean(True)
            if s == "false":
                return WDL.Value.Boolean(False)
            raise ValueError('read_boolean(): file content is not "true" or "false"')

        self._override_static("read_boolean", _read_something(_parse_boolean))

        def parse_lines(s: str) -> WDL.Value.Array:
            ans = []
            if s:
                ans = [
                    WDL.Value.String(line)
                    for line in (s[:-1] if s.endswith("\n") else s).split("\n")
                ]
            return WDL.Value.Array(WDL.Type.Array(WDL.Type.String()), ans)

        self._override_static("read_lines", _read_something(parse_lines))

        def parse_tsv(s: str) -> WDL.Value.Array:
            # TODO: should a blank line parse as [] or ['']?
            ans = [
                WDL.Value.Array(
                    WDL.Type.Array(WDL.Type.String()),
                    [WDL.Value.String(field) for field in line.value.split("\t")],
                )
                for line in parse_lines(s).value
            ]
            # pyre-ignore
            return WDL.Value.Array(WDL.Type.Array(WDL.Type.Array(WDL.Type.String())), ans)

        self._override_static("read_tsv", _read_something(parse_tsv))

        def _write_something(
            serialize: Callable[[WDL.Value.Base, BinaryIO], None], lib: _StdLib = self
        ) -> Callable[[WDL.Value.Base], WDL.Value.File]:
            def _f(
                v: WDL.Value.Base,
                serialize: Callable[[WDL.Value.Base, BinaryIO], None] = serialize,
                lib: _StdLib = lib,
            ) -> WDL.Value.File:
                host_fn = None
                os.makedirs(os.path.join(lib.container.host_dir, "write"), exist_ok=True)
                with tempfile.NamedTemporaryFile(
                    dir=os.path.join(lib.container.host_dir, "write"), delete=False
                ) as outfile:
                    outfile: BinaryIO = outfile  # pyre-ignore
                    serialize(v, outfile)
                    host_fn = outfile.name
                assert os.path.isabs(host_fn)
                lib.container.add_files([host_fn])
                return WDL.Value.File(lib.container.input_file_map[host_fn])

            return _f

        def _serialize_lines(array: WDL.Value.Array, outfile: BinaryIO) -> None:
            for item in array.value:
                assert isinstance(item, WDL.Value.String)
                outfile.write(item.value.encode("utf-8"))
                outfile.write(b"\n")

        self._override_static("write_lines", _write_something(_serialize_lines))  # pyre-ignore

        self._override_static(
            "write_json",
            _write_something(lambda v, outfile: outfile.write(json.dumps(v.json).encode("utf-8"))),
        )

        self._override_static(
            "write_tsv",
            _write_something(
                lambda v, outfile: _serialize_lines(
                    WDL.Value.Array(
                        WDL.Type.Array(WDL.Type.String()),
                        [
                            WDL.Value.String("\t".join([part.value for part in parts.value]))
                            for parts in v.value
                        ],
                    ),
                    outfile,
                )
            ),
        )


class InputStdLib(_StdLib):
    # StdLib for evaluation of task inputs and command
    def __init__(self, container: TaskContainer) -> None:
        super().__init__(container, True)


class OutputStdLib(_StdLib):
    # StdLib for evaluation of task outputs
    def __init__(self, container: TaskContainer) -> None:
        super().__init__(container, False)

        self._override_static(
            "stdout",
            lambda container_dir=self.container.container_dir: WDL.Value.File(
                os.path.join(container_dir, "stdout.txt")
            ),
        )
        self._override_static(
            "stderr",
            lambda container_dir=self.container.container_dir: WDL.Value.File(
                os.path.join(container_dir, "stderr.txt")
            ),
        )

        def _glob(pattern: WDL.Value.String, lib: OutputStdLib = self) -> WDL.Value.Array:
            pat = pattern.coerce(WDL.Type.String()).value
            if not pat:
                raise OutputError("empty glob() pattern")
            assert isinstance(pat, str)
            if pat[0] == "/":
                raise OutputError("glob() pattern must be relative to task working directory")
            if pat.startswith("..") or "/.." in pat:
                raise OutputError("glob() pattern must not use .. uplevels")
            if pat.startswith("./"):
                pat = pat[2:]
            pat = os.path.join(lib.container.host_dir, "work", pat)
            return WDL.Value.Array(
                WDL.Type.Array(WDL.Type.File()),
                [WDL.Value.String(fn) for fn in sorted(glob.glob(pat)) if os.path.isfile(fn)],
            )

        self._override_static("glob", _glob)


class _Size(WDL.StdLib._Size):
    # overrides WDL.StdLib._Size() to perform translation of in-container to host paths

    lib: _StdLib

    def __init__(self, lib: _StdLib) -> None:
        super().__init__()
        self.lib = lib

    def _call_eager(self, expr: WDL.Expr.Apply, arguments: List[WDL.Value.Base]) -> WDL.Value.Base:
        files = arguments[0].coerce(WDL.Type.Array(WDL.Type.File()))
        host_files = [
            WDL.Value.File(
                self.lib.container.host_file(fn_c.value, inputs_only=self.lib.inputs_only)
            )
            for fn_c in files.value
        ]
        # pyre-ignore
        arguments = [WDL.Value.Array(files.type, host_files)] + arguments[1:]
        return super()._call_eager(expr, arguments)
