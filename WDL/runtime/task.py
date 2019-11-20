# pyre-strict
"""
Local task runner
"""
import logging
import math
import os
import json
import copy
import traceback
import glob
import time
import multiprocessing
import threading
import shutil
import shlex
import re
from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Optional, Callable, Iterable, Set, Any
import psutil
import docker
from .. import Error, Type, Env, Value, StdLib, Tree, _util
from .._util import (
    write_values_json,
    provision_run_dir,
    LOGGING_FORMAT,
    PygtailLogger,
    TerminationSignalFlag,
    parse_byte_size,
    chmod_R_plus,
    path_really_within,
)
from .._util import StructuredLogMessage as _
from .download import able as downloadable, run as download
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

    input_file_map_rev: Dict[str, str]

    _running: bool

    def __init__(self, run_id: str, host_dir: str) -> None:
        self.run_id = run_id
        self.host_dir = host_dir
        self.container_dir = "/mnt/miniwdl_task_container"
        self.input_file_map = {}
        self.input_file_map_rev = {}
        self._running = False
        os.makedirs(os.path.join(self.host_dir, "work"))

    def add_files(self, host_files: Iterable[str]) -> None:
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
            if host_file not in self.input_file_map:
                if not os.path.isfile(host_file):
                    raise Error.InputError("input file not found: " + host_file)
                host_files_by_dir.setdefault(os.path.dirname(host_file), set()).add(host_file)

        # for each such partition of files
        # - if there are no basename collisions under input subdirectory 0, then mount them there.
        # - otherwise, mount them in a fresh subdirectory
        for files in host_files_by_dir.values():
            based = os.path.join(self.container_dir, "work/_miniwdl_inputs")
            subd = "0"
            for host_file in files:
                container_file = os.path.join(based, subd, os.path.basename(host_file))
                if container_file in self.input_file_map_rev:
                    subd = str(len(self.input_file_map) + 1)
            for host_file in files:
                container_file = os.path.join(based, subd, os.path.basename(host_file))
                assert container_file not in self.input_file_map_rev
                self.input_file_map[host_file] = container_file
                self.input_file_map_rev[container_file] = host_file

    def copy_input_files(self, logger: logging.Logger) -> None:
        # After add_files has been used as needed, copy the input files from their original
        # locations to the appropriate subdirectories of the container working directory. This may
        # not be necessary e.g. if the container implementation supports bind-mounting the input
        # files from their original host paths.
        for host_filename, container_filename in self.input_file_map.items():
            assert container_filename.startswith(self.container_dir)
            host_copy_filename = os.path.join(
                self.host_dir, os.path.relpath(container_filename, self.container_dir)
            )

            logger.info(_("copy host input file", input=host_filename, copy=host_copy_filename))
            os.makedirs(os.path.dirname(host_copy_filename), exist_ok=True)
            shutil.copy(host_filename, host_copy_filename)

    def run(self, logger: logging.Logger, command: str, cpu: int, memory: int) -> None:
        """
        1. Container is instantiated with the configured mounts
        2. The mounted directory and all subdirectories have u+rwx,g+rwx permission bits; all files
           within have u+rw,g+rw permission bits.
        3. Command is executed in ``{host_dir}/work/`` (where {host_dir} is mounted to
           {container_dir} inside the container)
        4. Standard output is written to ``{host_dir}/stdout.txt``
        5. Standard error is written to ``{host_dir}/stderr.txt`` and logged at VERBOSE level
        6. Raises CommandFailed for nonzero exit code, or any other error

        The container is torn down in any case, including SIGTERM/SIGHUP signal which is trapped.
        """
        # container-specific logic should be in _run(). this wrapper traps signals

        assert not self._running
        if command.strip():  # if the command is empty then don't bother with any of this
            with TerminationSignalFlag(logger) as terminating:
                if terminating():
                    raise Terminated()
                self._running = True
                try:
                    exit_status = self._run(logger, terminating, command, cpu, memory)
                finally:
                    self._running = False

                if exit_status != 0:
                    raise CommandFailed(
                        exit_status, os.path.join(self.host_dir, "stderr.txt")
                    ) if not terminating() else Terminated()

    @abstractmethod
    def _run(
        self,
        logger: logging.Logger,
        terminating: Callable[[], bool],
        command: str,
        cpu: int,
        memory: int,
    ) -> int:
        # run command in container & return exit status
        raise NotImplementedError()

    def reset(self, logger: logging.Logger, prev_retries: int) -> None:
        """
        After a container/command failure, reset the working directory state so that
        copy_input_files() and run() can be retried.
        """
        artifacts_dir = os.path.join(self.host_dir, "failed_tries", str(prev_retries))
        artifacts_moved = []
        for artifact in ["work", "command", "stdout.txt", "stderr.txt", "stderr.txt.offset"]:
            src = os.path.join(self.host_dir, artifact)
            if os.path.exists(src):
                os.renames(src, os.path.join(artifacts_dir, artifact))
                artifacts_moved.append(src)
        logger.info(
            _("archived failed task artifacts", artifacts=artifacts_moved, dest=artifacts_dir)
        )
        os.makedirs(os.path.join(self.host_dir, "work"))

    def host_file(self, container_file: str, inputs_only: bool = False) -> Optional[str]:
        """
        Map an output file's in-container path under ``container_dir`` to a host path under
        ``host_dir``. Return None if the designated file does not exist.

        SECURITY: except for input files, this method must only return host paths under
        ``host_dir`` and prevent any reference to other host files (e.g. /etc/passwd), including
        via sneaky symlinks
        """
        if os.path.isabs(container_file):
            # handle output of std{out,err}.txt
            if container_file in [
                os.path.join(self.container_dir, pipe_file)
                for pipe_file in ["stdout.txt", "stderr.txt"]
            ]:
                return os.path.join(self.host_dir, os.path.basename(container_file))
            # handle output of an input file
            if container_file in self.input_file_map_rev:
                return self.input_file_map_rev[container_file]
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
        ans = os.path.join(host_workdir, container_file)
        if os.path.isfile(ans):
            if path_really_within(ans, host_workdir):
                return ans
            raise OutputError(
                "task outputs attempted to use a file outside its working directory: "
                + container_file
            )
        return None


class TaskDockerContainer(TaskContainer):
    """
    TaskContainer docker (swarm) runtime
    """

    image_tag: str = "ubuntu:18.04"
    """
    :type: str

    docker image tag (set as desired before running)
    """

    as_me: bool = False
    """
    :type: bool

    If so then run command inside the container using the uid:gid of the invoking user. Otherwise
    don't override container user (=> it'll often run as root).
    """

    _bind_input_files: Optional[str] = "ro"
    _observed_states: Optional[Set[str]] = None

    def copy_input_files(self, logger: logging.Logger) -> None:
        assert self._bind_input_files
        super().copy_input_files(logger)
        # now that files have been copied, it won't be necessary to bind-mount them
        self._bind_input_files = None

    def _run(
        self,
        logger: logging.Logger,
        terminating: Callable[[], bool],
        command: str,
        cpu: int,
        memory: int,
    ) -> int:
        self._observed_states = set()
        with open(os.path.join(self.host_dir, "command"), "x") as outfile:
            outfile.write(command)

        # prepare docker configuration
        if ":" not in self.image_tag:
            # seems we need to do this explicitly under some configurations -- issue #232
            self.image_tag += ":latest"
        logger.info(_("docker image", tag=self.image_tag))

        mounts = self.prepare_mounts(logger)
        # we want g+rw on files (and g+rwx on directories) under host_dir, to ensure the container
        # command will be able to access them regardless of what user id it runs as (we will
        # configure docker to make the container a member of the invoking user's primary group)
        chmod_R_plus(self.host_dir, file_bits=0o660, dir_bits=0o770)

        resources, user, groups = self.misc_config(logger, cpu, memory)

        # connect to dockerd
        client = docker.from_env()
        svc = None
        try:
            # run container as a transient docker swarm service, letting docker handle the resource
            # scheduling (waiting until requested # of CPUs are available)
            svc = client.services.create(
                self.image_tag,
                command=[
                    "/bin/bash",
                    "-c",
                    "id; ls -Rl ..; bash ../command >> ../stdout.txt 2>> ../stderr.txt",
                ],
                # restart_policy 'none' so that swarm runs the container just once
                restart_policy=docker.types.RestartPolicy("none"),
                workdir=os.path.join(self.container_dir, "work"),
                mounts=mounts,
                resources=resources,
                user=user,
                groups=groups,
                labels={"miniwdl_run_id": self.run_id},
                container_labels={"miniwdl_run_id": self.run_id},
            )
            logger.debug(_("docker service", name=svc.name, id=svc.short_id))

            exit_code = None
            # stream stderr into log
            with PygtailLogger(logger, os.path.join(self.host_dir, "stderr.txt")) as poll_stderr:
                # poll for container exit
                while exit_code is None:
                    time.sleep(1)
                    if terminating():
                        raise Terminated() from None
                    if "running" in self._observed_states:
                        poll_stderr()
                    exit_code = self.poll_service(logger, svc)
                logger.debug(
                    _(
                        "docker service logs",
                        stdout=list(msg.decode().rstrip() for msg in svc.logs(stdout=True)),
                        stderr=list(msg.decode().rstrip() for msg in svc.logs(stderr=True)),
                    )
                )
                logger.info(_("docker exit", code=exit_code))

            # retrieve and check container exit status
            assert isinstance(exit_code, int)
            return exit_code
        finally:
            if svc:
                try:
                    svc.remove()
                except:
                    logger.exception("failed to remove docker service")
                self.chown(logger, client)
            try:
                client.close()
            except:
                logger.exception("failed to close docker-py client")

    def prepare_mounts(self, logger: logging.Logger) -> List[Dict[str, str]]:
        def touch_mount_point(container_file: str) -> None:
            # touching each mount point ensures they'll be owned by invoking user:group
            assert container_file.startswith(self.container_dir + "/")
            host_file = os.path.join(
                self.host_dir, os.path.relpath(container_file, self.container_dir)
            )
            assert host_file.startswith(self.host_dir + "/")
            os.makedirs(os.path.dirname(host_file), exist_ok=True)
            with open(host_file, "x") as outfile:
                pass

        mounts = []
        # mount input files and command
        if self._bind_input_files:
            perm_warn = True
            for host_path, container_path in self.input_file_map.items():
                st = os.stat(host_path)
                if perm_warn and not (
                    (st.st_mode & 4) or (st.st_gid == os.getegid() and (st.st_mode & 0o40))
                ):
                    # file is neither world-readable, nor group-readable for the invoking user's primary group
                    logger.warning(
                        _(
                            "one or more input file(s) could be inaccessible to docker images that don't run as root; it may be necessary to `chmod a+r` them, or set --copy-input-files",
                            example_file=host_path,
                        )
                    )
                    perm_warn = False
                touch_mount_point(container_path)
                mounts.append(f"{host_path}:{container_path}:{self._bind_input_files}")
        mounts.append(
            f"{os.path.join(self.host_dir, 'command')}:{os.path.join(self.container_dir, 'command')}:ro"
        )
        # mount stdout, stderr, and working directory read/write
        for pipe_file in ["stdout.txt", "stderr.txt"]:
            touch_mount_point(os.path.join(self.container_dir, pipe_file))
            mounts.append(
                f"{os.path.join(self.host_dir, pipe_file)}:{os.path.join(self.container_dir, pipe_file)}:rw"
            )
        mounts.append(
            f"{os.path.join(self.host_dir, 'work')}:{os.path.join(self.container_dir, 'work')}:rw"
        )
        logger.debug(_("docker mounts", mounts=mounts))
        return mounts

    def misc_config(
        self, logger: logging.Logger, cpu: int, memory: int
    ) -> Tuple[Optional[Dict[str, str]], Optional[str], List[str]]:
        resources = {}
        if cpu:
            # the cpu unit expected by swarm is "NanoCPUs"
            resources["cpu_limit"] = cpu * 1_000_000_000
            resources["cpu_reservation"] = cpu * 1_000_000_000
        if memory:
            resources["mem_reservation"] = memory
        if resources:
            logger.debug(_("docker resources", **resources))
            resources = docker.types.Resources(**resources)
        else:
            resources = None
        user = None
        if self.as_me:
            user = f"{os.geteuid()}:{os.getegid()}"
            logger.info(_("docker user", uid_gid=user))
            if os.geteuid() == 0:
                logger.warning(
                    "container command will run explicitly as root, since you are root and set --as-me"
                )
        # add invoking user's group to ensure that command can access the mounted working
        # directory even if the docker image assumes some arbitrary uid
        groups = [str(os.getegid())]
        if groups == ["0"]:
            logger.warning(
                "container command will run as a root/wheel group member, since this is your primary group (gid=0)"
            )
        return resources, user, groups

    def poll_service(
        self, logger: logging.Logger, svc: docker.models.services.Service
    ) -> Optional[int]:
        status = {"State": "(UNKNOWN)"}

        svc.reload()
        assert svc.attrs["Spec"]["Labels"]["miniwdl_run_id"] == self.run_id
        tasks = svc.tasks()
        if tasks:
            assert len(tasks) == 1, "docker service should have at most 1 task"
            status = tasks[0]["Status"]
            logger.debug(_("docker task", id=tasks[0]["ID"], status=status))
        else:
            assert (
                len(self._observed_states or []) <= 1
            ), "docker task shouldn't disappear from service"

        # log each new state
        assert isinstance(self._observed_states, set)
        if status["State"] not in self._observed_states:
            logger.info(_("docker task transition", state=status["State"]))
            self._observed_states.add(status["State"])

        # https://docs.docker.com/engine/swarm/how-swarm-mode-works/swarm-task-states/
        # https://github.com/moby/moby/blob/8fbf2598f58fb212230e6ddbcfbde628b0458250/api/types/swarm/task.go#L12
        if "ExitCode" in status.get("ContainerStatus", {}):
            exit_code = status["ContainerStatus"]["ExitCode"]
            assert isinstance(exit_code, int)
            if exit_code != 0 or status["State"] == "complete":
                logger.info(_("docker task exit", state=status["State"], exit_code=exit_code))
                return exit_code

        if status["State"] in ["failed", "rejected", "orphaned", "remove"]:
            raise RuntimeError(
                f"docker task {status['State']}"
                + ((": " + status["Err"]) if "Err" in status else "")
            )

        return None

    def chown(self, logger: logging.Logger, client: docker.DockerClient) -> None:
        """
        After task completion, chown all files in the working directory to the invoking user:group,
        instead of leaving them frequently owned by root or some other arbitrary user id (image-
        dependent). We do this in a funny way via Docker; see GitHub issue #271 for discussion of
        alternatives and their problems.
        """
        if not self.as_me and (os.geteuid() or os.getegid()):
            script = f"""
            chown -RP {os.geteuid()}:{os.getegid()} {shlex.quote(os.path.join(self.container_dir, 'work'))}
            """.strip()
            volumes = {self.host_dir: {"bind": self.container_dir, "mode": "rw"}}
            try:
                logger.debug(_("post-task chown", script=script, volumes=volumes))
                client.containers.run(
                    "alpine:3",
                    command=["/bin/ash", "-c", script],
                    volumes=volumes,
                    auto_remove=True,
                )
            except:
                logger.exception("post-task chown failed")


def run_local_task(
    task: Tree.Task,
    inputs: Env.Bindings[Value.Base],
    run_id: Optional[str] = None,
    run_dir: Optional[str] = None,
    copy_input_files: bool = False,
    max_runtime_cpu: Optional[int] = None,
    max_runtime_memory: Optional[int] = None,
    logger_prefix: Optional[List[str]] = None,
    as_me: bool = False,
) -> Tuple[str, Env.Bindings[Value.Base]]:
    """
    Run a task locally.

    Inputs shall have been typechecked already. File inputs are presumed to be local POSIX file
    paths that can be mounted into a container.

    :param run_id: unique ID for the run, defaults to workflow name
    :param run_dir: directory under which to create a timestamp-named subdirectory for this run
                    (defaults to current working directory).
                    If the final path component is ".", then operate in run_dir directly.
    :param copy_input_files: copy input files and mount them read/write instead of read-only
    :param max_runtime_cpu: maximum effective runtime.cpu value (default: # host CPUs)
    :param max_runtime_memory: maximum effective runtime.memory value in bytes (default: total host
                               memory)
    :param as_me: run container command using the current user uid:gid (may break commands that
                  assume root access, e.g. apt-get)
    """

    run_id = run_id or task.name
    run_dir = provision_run_dir(task.name, run_dir)
    logger_prefix = (logger_prefix or ["wdl"]) + ["t:" + run_id]
    logger = logging.getLogger(".".join(logger_prefix))
    fh = logging.FileHandler(os.path.join(run_dir, "task.log"))
    fh.setFormatter(logging.Formatter(LOGGING_FORMAT))
    logger.addHandler(fh)
    _util.install_coloredlogs(logger)
    logger.notice(  # pyre-fixme
        _(
            "task start",
            name=task.name,
            source=task.pos.uri,
            line=task.pos.line,
            column=task.pos.column,
            dir=run_dir,
        )
    )
    logger.info(_("thread", ident=threading.get_ident()))

    write_values_json(inputs, os.path.join(run_dir, "inputs.json"))

    try:
        # download input files, if needed
        posix_inputs = _download_input_files(logger, logger_prefix, run_dir, inputs)

        # create appropriate TaskContainer
        container = TaskDockerContainer(run_id, run_dir)

        # evaluate input/postinput declarations, including mapping from host to
        # in-container file paths
        container_env = _eval_task_inputs(logger, task, posix_inputs, container)

        # evaluate runtime fields
        runtime = _eval_task_runtime(
            logger, task, container_env, max_runtime_cpu, max_runtime_memory
        )
        container.image_tag = str(runtime.get("docker", container.image_tag))
        container.as_me = as_me

        # interpolate command
        command = _util.strip_leading_whitespace(
            task.command.eval(container_env, stdlib=InputStdLib(logger, container)).value
        )[1]
        logger.debug(_("command", command=command.strip()))

        # start container & run command (and retry if needed)
        _try_task(logger, container, command, runtime, copy_input_files)

        # evaluate output declarations
        outputs = _eval_task_outputs(logger, task, container_env, container)

        # write and link outputs
        from .. import values_to_json

        make_output_links(values_to_json(outputs, namespace=task.name), run_dir)  # pyre-fixme
        write_values_json(outputs, os.path.join(run_dir, "outputs.json"), namespace=task.name)

        # make sure everything will be accessible to downstream tasks
        chmod_R_plus(container.host_dir, file_bits=0o660, dir_bits=0o770)

        logger.notice("done")  # pyre-fixme
        return (run_dir, outputs)
    except Exception as exn:
        logger.debug(traceback.format_exc())
        wrapper = RunFailed(task, run_id, run_dir)
        info = {"error": exn.__class__.__name__}
        if str(exn):
            info["message"] = str(exn)
        if hasattr(exn, "job_id"):
            info["node"] = getattr(exn, "job_id")
        logger.error(_(str(wrapper), **info))
        raise wrapper from exn


def _download_input_files(
    logger: logging.Logger, logger_prefix: List[str], run_dir: str, inputs: Env.Bindings[Value.Base]
) -> Env.Bindings[Value.Base]:
    """
    Find all File values in the inputs (including any nested within compound values) that need
    to / can be downloaded. Download them to some location under run_dir and return a copy of the
    inputs with the URI values replaced by the downloaded filenames.
    """

    downloads = 0
    total_bytes = 0

    def map_files(v: Value.Base) -> Value.Base:
        nonlocal downloads, total_bytes
        if isinstance(v, Value.File):
            if downloadable(v.value):
                logger.info(_("download input file", uri=v.value))
                v.value = download(
                    v.value,
                    run_dir=os.path.join(run_dir, "download", str(downloads)),
                    logger_prefix=logger_prefix + [f"download{downloads}"],
                )
                sz = os.path.getsize(v.value)
                logger.info(_("downloaded input file", uri=v.value, file=v.value, bytes=sz))
                downloads += 1
                total_bytes += sz
        for ch in v.children:
            map_files(ch)
        return v

    ans = inputs.map(
        lambda binding: Env.Binding(binding.name, map_files(copy.deepcopy(binding.value)))
    )
    if downloads:
        logger.notice(  # pyre-fixme
            _("downloaded input files", count=downloads, total_bytes=total_bytes)
        )
    return ans


def _eval_task_inputs(
    logger: logging.Logger,
    task: Tree.Task,
    posix_inputs: Env.Bindings[Value.Base],
    container: TaskContainer,
) -> Env.Bindings[Value.Base]:

    # Map all the provided input Files to in-container paths
    container.add_files(_filenames(posix_inputs))

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
        logger.info(_("input", name=b.name, value=(v.json if len(vj) < 4096 else "(((large)))")))

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
    stdlib = InputStdLib(logger, container)
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
        logger.info(_("eval", name=decl.name, value=(v.json if len(vj) < 4096 else "(((large)))")))
        container_env = container_env.bind(decl.name, v)

    return container_env


def _filenames(env: Env.Bindings[Value.Base]) -> Set[str]:
    "Get the filenames of all File values in the environment"
    ans = set()

    def collector(v: Value.Base) -> None:
        if isinstance(v, Value.File):
            ans.add(v.value)
        for ch in v.children:
            collector(ch)

    for b in env:
        collector(b.value)
    return ans


_host_memory: Optional[int] = None


def _eval_task_runtime(
    logger: logging.Logger,
    task: Tree.Task,
    env: Env.Bindings[Value.Base],
    max_runtime_cpu: Optional[int],
    max_runtime_memory: Optional[int],
) -> Dict[str, Union[int, str]]:
    global _host_memory

    runtime_values = dict((key, expr.eval(env)) for key, expr in task.runtime.items())
    logger.debug(_("runtime values", **dict((key, str(v)) for key, v in runtime_values.items())))
    ans = {}

    if "docker" in runtime_values:
        ans["docker"] = runtime_values["docker"].coerce(Type.String()).value

    if "cpu" in runtime_values:
        cpu_value = runtime_values["cpu"].coerce(Type.Int()).value
        assert isinstance(cpu_value, int)
        cpu = max(1, min(max_runtime_cpu or multiprocessing.cpu_count(), cpu_value))
        if cpu != cpu_value:
            logger.warning(
                _("runtime.cpu adjusted to local limit", original=cpu_value, adjusted=cpu)
            )
        ans["cpu"] = cpu

    if "memory" in runtime_values:
        memory_str = runtime_values["memory"].coerce(Type.String()).value
        assert isinstance(memory_str, str)
        try:
            memory_bytes = parse_byte_size(memory_str)
        except ValueError:
            raise Error.EvalError(
                task.runtime["memory"], "invalid setting of runtime.memory, " + memory_str
            )

        if not max_runtime_memory:
            _host_memory = _host_memory or psutil.virtual_memory().total
            max_runtime_memory = _host_memory
        assert isinstance(max_runtime_memory, int)
        if memory_bytes > max_runtime_memory:
            logger.warning(
                _(
                    "runtime.memory adjusted to local limit",
                    original=memory_bytes,
                    adjusted=max_runtime_memory,
                )
            )
            memory_bytes = max_runtime_memory
        ans["memory"] = memory_bytes

    if "maxRetries" in runtime_values:
        ans["maxRetries"] = max(0, runtime_values["maxRetries"].coerce(Type.Int()).value)

    if ans:
        logger.info(_("effective runtime", **ans))
    unused_keys = list(key for key in runtime_values if key not in ans)
    if unused_keys:
        logger.warning(_("ignored runtime settings", keys=unused_keys))

    return ans


def _try_task(
    logger: logging.Logger,
    container: TaskContainer,
    command: str,
    runtime: Dict[str, Union[int, str]],
    copy_input_files: bool,
) -> None:
    """
    Run the task command in the container, with up to runtime.maxRetries
    """
    maxRetries = runtime.get("maxRetries", 0)
    prevRetries = 0

    while True:
        # copy input files, if needed
        if copy_input_files:
            container.copy_input_files(logger)

        try:
            # start container & run command
            return container.run(
                logger, command, int(runtime.get("cpu", 0)), int(runtime.get("memory", 0))
            )
        except Exception as exn:
            if isinstance(exn, Terminated) or prevRetries >= maxRetries:
                raise
            logger.error(
                _(
                    "task failure will be retried",
                    error=exn.__class__.__name__,
                    message=str(exn),
                    prevRetries=prevRetries,
                    maxRetries=maxRetries,
                )
            )
            container.reset(logger, prevRetries)
            prevRetries += 1


def _eval_task_outputs(
    logger: logging.Logger, task: Tree.Task, env: Env.Bindings[Value.Base], container: TaskContainer
) -> Env.Bindings[Value.Base]:

    # helper to rewrite Files from in-container paths to host paths
    def rewrite_files(v: Value.Base, output_name: str) -> None:
        if isinstance(v, Value.File):
            host_file = container.host_file(v.value)
            if host_file is None:
                logger.warning(
                    _(
                        "output file not found in container (error unless declared type is optional)",
                        name=output_name,
                        file=v.value,
                    )
                )
            else:
                logger.debug(_("output file", container=v.value, host=host_file))
            # We may overwrite File.value with None, which is an invalid state, then we'll fix it
            # up (or abort) below. This trickery is because we don't, at this point, know whether
            # the 'desired' output type is File or File?.
            v.value = host_file
        for ch in v.children:
            rewrite_files(ch, output_name)

    stdlib = OutputStdLib(logger, container)
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
        vj = json.dumps(v.json)
        logger.info(
            _("output", name=decl.name, value=(v.json if len(vj) < 4096 else "(((large)))"))
        )

        # Now, a delicate sequence for postprocessing File outputs (including Files nested within
        # compound values)

        # First bind the value as-is in the environment, so that subsequent output expressions will
        # "see" the in-container path(s) if they use this binding. (Copy it though, because we'll
        # then clobber v)
        env = env.bind(decl.name, copy.deepcopy(v))
        # Rewrite each File.value to either a host path, or None if the file doesn't exist.
        rewrite_files(v, decl.name)
        # File.coerce has a special behavior for us so that, if the value is None:
        #   - produces Value.Null() if the desired type is File?
        #   - raises FileNotFoundError otherwise.
        try:
            v = v.coerce(decl.type)
        except FileNotFoundError:
            exn = OutputError("File not found in task output " + decl.name)
            setattr(exn, "job_id", decl.workflow_node_id)
            raise exn
        outputs = outputs.bind(decl.name, v)

    return outputs


def make_output_links(outputs_json: Dict[str, Any], run_dir: str) -> None:
    """
    Following a successful run, the output files may be scattered throughout a complex directory
    tree used for execution. To help navigating this, generate a subdirectory of the run directory
    containing nicely organized symlinks to the output files.

    Given ``WDL.Env.Bindings[WDL.Value.Base]`` outputs, this expects to receive
    ``WDL.values_to_json(outputs, namespace=targets.name)`` instead of outputs directly. This makes
    it compatible with Cromwell's output JSON too.

    For security reasons, omits any files not inside run_dir (e.g. if the outputs include an input
    file located elsewhere)
    """

    def traverse(v: Any, dn: str) -> None:  # pyre-fixme
        assert isinstance(v, (str, int, float, list, dict)) or v is None
        if (
            isinstance(v, str)
            and v.startswith(run_dir + "/")
            and os.path.isfile(v)
            and path_really_within(v, run_dir)
        ):
            os.makedirs(dn, exist_ok=False)
            os.symlink(v, os.path.join(dn, os.path.basename(v)))
        elif isinstance(v, list) and v:
            d = int(math.ceil(math.log10(len(v))))  # how many digits needed
            for i, elt in enumerate(v):
                traverse(elt, os.path.join(dn, str(i).rjust(d, "0")))
        elif isinstance(v, dict):
            # create a subdirectory for each key, as long as the key names seem to make reasonable
            # path components; otherwise, treat the dict as a list of its values (this is possible
            # in Maps where keys can be arbitrary)
            if (
                sum(1 for key in v if re.fullmatch("[-_a-zA-Z0-9][-_a-zA-Z0-9.]*", key) is None)
                == 0
            ):
                for key, value in v.items():
                    traverse(value, os.path.join(dn, key))
            else:
                traverse(list(v.values()), dn)

    dn0 = os.path.join(run_dir, "output_links")
    os.makedirs(dn0, exist_ok=False)
    traverse(outputs_json, dn0)


class _StdLib(StdLib.Base):
    logger: logging.Logger
    container: TaskContainer
    inputs_only: bool  # if True then only permit access to input files

    def __init__(self, logger: logging.Logger, container: TaskContainer, inputs_only: bool) -> None:
        super().__init__(write_dir=os.path.join(container.host_dir, "write_"))
        self.logger = logger
        self.container = container
        self.inputs_only = inputs_only

    def _devirtualize_filename(self, filename: str) -> str:
        # check allowability of reading this file, & map from in-container to host
        ans = self.container.host_file(filename, inputs_only=self.inputs_only)
        if ans is None:
            raise OutputError("function was passed non-existent file " + filename)
        self.logger.debug(_("read_", container=filename, host=ans))
        return ans

    def _virtualize_filename(self, filename: str) -> str:
        # register new file with container input_file_map
        self.container.add_files([filename])
        self.logger.debug(
            _("write_", host=filename, container=self.container.input_file_map[filename])
        )
        self.logger.info(_("wrote", file=self.container.input_file_map[filename]))
        return self.container.input_file_map[filename]


class InputStdLib(_StdLib):
    # StdLib for evaluation of task inputs and command
    def __init__(self, logger: logging.Logger, container: TaskContainer) -> None:
        super().__init__(logger, container, True)


class OutputStdLib(_StdLib):
    # StdLib for evaluation of task outputs
    def __init__(self, logger: logging.Logger, container: TaskContainer) -> None:
        super().__init__(logger, container, False)

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
