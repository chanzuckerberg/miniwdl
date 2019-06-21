# pyre-strict
import logging
import os
import tempfile
import json
import copy
import traceback
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Optional
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
        ans = {}
        basenames = {}
        for fn in host_files:
            if fn not in self.input_file_map:
                bn = os.path.basename(fn)
                basenames[bn] = 1 + basenames.get(bn, 0)
                ans[fn] = os.path.join(self.container_dir, "inputs", bn)

        # Error out if any input filenames collide.
        # TODO: assort them into separate subdirectories, also with a heuristic
        # grouping together files that come from the same host directory.
        collisions = [bn for bn, ct in basenames.items() if ct > 1]
        if collisions:
            raise WDL.Error.InputError("input filename collision(s): " + " ".join(collisions))

        for k, v in ans.items():
            assert k not in self.input_file_map
            self.input_file_map[k] = v

    def run(self, logger: logging.Logger, command: str) -> None:
        """
        1. Container is instantiated
        2. Command is executed in ``{host_dir}/work/`` (where {host_dir} is mounted to {container_dir} inside the container)
        3. Standard output is written to ``{host_dir}/stdout.txt``
        4. Standard error is written to ``{host_dir}/stderr.txt`` and logged at INFO level
        5. Raises CommandError for nonzero exit code, or any other error

        The container is torn down in any case, including SIGTERM/SIGHUP signal which is trapped.
        """
        assert not self._running
        self._running = True
        # container-specific logic should be in _run(). this wrapper traps SIGTERM/SIGHUP
        # and sets self._terminate
        return self._run(logger, command)

    @abstractmethod
    def _run(self, logger: logging.Logger, command: str) -> None:
        raise NotImplementedError()

    def host_file(self, container_file: str) -> str:
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

    def _stdlib_base(self) -> WDL.StdLib.Base:
        # - Invocations of write_* will use self.add_files
        ans = WDL.StdLib.Base()

        def _write_lines(array: WDL.Value.Array, self: "TaskContainer" = self) -> WDL.Value.File:
            host_fn = None
            os.makedirs(os.path.join(self.host_dir, "write"), exist_ok=True)
            with tempfile.NamedTemporaryFile(
                prefix="lines_", dir=os.path.join(self.host_dir, "write"), delete=False
            ) as outfile:
                for item in array.value:
                    assert isinstance(item, WDL.Value.String)
                    outfile.write(item.value.encode("utf-8"))
                    outfile.write(b"\n")
                host_fn = outfile.name
            assert os.path.isabs(host_fn)
            self.add_files([host_fn])
            return WDL.Value.File(self.input_file_map[host_fn])

        ans._override_static("write_lines", _write_lines)

        return ans

    def stdlib_input(self) -> WDL.StdLib.Base:
        """
        Produce a StdLib implementation suitable for evaluation of task input
        declarations and command interpolation
        """

        # - Invocations of size(), read_* are permitted only on input files (no string coercions)
        # - forbidden/undefined: stdout, stderr, glob

        ans = self._stdlib_base()
        return ans

    def stdlib_output(self) -> WDL.StdLib.Base:
        """
        Produce a StdLib implementation suitable for evaluation of task output
        expressions
        """

        # - size(), read_* and glob are permitted only on paths in or under the container directory (cdup from working directory)
        # - their argument has to be translated from container to host path to actually execute

        ans = self._stdlib_base()
        ans._override_static(
            "stdout",
            lambda container_dir=self.container_dir: WDL.Value.File(
                os.path.join(container_dir, "stdout.txt")
            ),
        )
        ans._override_static(
            "stderr",
            lambda container_dir=self.container_dir: WDL.Value.File(
                os.path.join(container_dir, "stderr.txt")
            ),
        )

        def _read_string(
            container_file: WDL.Value.File, self: "TaskContainer" = self
        ) -> WDL.Value.String:
            host_file = self.host_file(container_file.value)
            assert host_file.startswith(self.host_dir)
            with open(host_file, "r") as infile:
                return WDL.Value.String(infile.read())

        ans._override_static("read_string", _read_string)

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

    def _run(self, logger: logging.Logger, command: str) -> None:
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
                        if "Read timed out" not in s_exn and "Timeout" not in s_exn:
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
                raise

            # retrieve and check container exit status
            assert exit_info
            if "StatusCode" not in exit_info:
                raise CommandError(
                    "docker finished without reporting exit status in: " + str(exit_info)
                )
            if exit_info["StatusCode"] != 0:
                raise CommandError("command exit status = " + str(exit_info["StatusCode"]))
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
            task.command.eval(container_env, stdlib=container.stdlib_input()).value
        )[1]

        # run container
        container.run(logger, command)

        # evaluate output declarations
        outputs = _eval_task_outputs(logger, task, container_env, container)

        logger.info("done")
        return (run_dir, outputs)
    except Exception as exn:
        logger.debug(traceback.format_exc())
        wrapper = TaskFailure(task.name, task_id)
        logger.error("{}: {}, {}".format(str(wrapper), exn.__class__.__name__, str(exn)))
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

    # TODO: topsort decls_to_eval according to internal dependencies

    # evaluate each declaration in order
    # note: the write_* functions call container.add_files as a side-effect
    stdlib = container.stdlib_input()
    for decl in decls_to_eval:
        v = WDL.Value.Null()
        if decl.expr:
            try:
                v = decl.expr.eval(container_env, stdlib=stdlib).coerce(decl.type)
            except WDL.Error.EvalError:
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
            v = decl.expr.eval(env, stdlib=container.stdlib_output()).coerce(decl.type)
        except WDL.Error.EvalError:
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
