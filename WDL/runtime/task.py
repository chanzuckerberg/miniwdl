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
import random
import multiprocessing
import threading
import shutil
import shlex
import re
import socket
from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Optional, Callable, Iterable, Set, Any
from contextlib import ExitStack
import docker
from .. import Error, Type, Env, Value, StdLib, Tree, _util
from .._util import (
    write_atomic,
    write_values_json,
    provision_run_dir,
    LOGGING_FORMAT,
    PygtailLogger,
    TerminationSignalFlag,
    parse_byte_size,
    chmod_R_plus,
    path_really_within,
    LoggingFileHandler,
    AtomicCounter,
    compose_coroutines,
)
from .._util import StructuredLogMessage as _
from . import config
from .download import able as downloadable, run_cached as download
from .cache import CallCache
from .error import *


class TaskContainer(ABC):
    """
    Base class for task containers, subclassed by runtime-specific implementations (e.g. Docker).
    """

    # class stuff

    @classmethod
    def global_init(cls, cfg: config.Loader, logger: logging.Logger) -> None:
        """
        Perform any necessary one-time initialization of the underlying container runtime. Must be
        invoked once per process prior to any instantiation of the class.
        """
        raise NotImplementedError()

    @classmethod
    def detect_resource_limits(cls, cfg: config.Loader, logger: logging.Logger) -> Dict[str, int]:
        """
        Detect the maximum resources (cpu and mem_bytes) that the underlying container runtime
        would be able to provision.

        If determining this is at all costly, then implementation should memoize (thread-safely and
        perhaps front-loaded in global_init).
        """
        raise NotImplementedError()

    # instance stuff

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

    def __init__(self, cfg: config.Loader, run_id: str, host_dir: str) -> None:
        self.cfg = cfg
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


class SwarmContainer(TaskContainer):
    """
    TaskContainer docker (swarm) runtime
    """

    _limits: Dict[str, int] = {}
    _id_counter: AtomicCounter = AtomicCounter()

    @classmethod
    def global_init(cls, cfg: config.Loader, logger: logging.Logger) -> None:
        client = docker.from_env()
        worker_nodes = []
        try:
            logger.debug("dockerd :: " + json.dumps(client.version())[1:-1])

            # initialize swarm
            state = "(unknown)"
            while True:
                info = client.info()
                if "Swarm" in info and "LocalNodeState" in info["Swarm"]:
                    logger.debug(_("swarm info", **info["Swarm"]))
                    state = info["Swarm"]["LocalNodeState"]

                # https://github.com/moby/moby/blob/e7b5f7dbe98c559b20c0c8c20c0b31a6b197d717/api/types/swarm/swarm.go#L185
                if state == "active":
                    if info["Swarm"]["ControlAvailable"]:
                        worker_nodes = [
                            node
                            for node in client.nodes.list()
                            if node.attrs["Spec"]["Availability"] == "active"
                            and node.attrs["Status"]["State"] == "ready"
                        ]
                        if worker_nodes:
                            break
                    else:
                        logging.warning(
                            "this host is a docker swarm worker but not a manager; WDL task scheduling requires manager access"
                        )
                elif state == "inactive" and cfg["docker_swarm"].get_bool("auto_init"):
                    logger.warning(
                        "docker swarm is inactive on this host; performing `docker swarm init --advertise-addr 127.0.0.1 --listen-addr 127.0.0.1`"
                    )
                    client.swarm.init(advertise_addr="127.0.0.1", listen_addr="127.0.0.1")

                logger.notice(  # pyre-fixme
                    _(
                        "waiting for local docker swarm manager & worker(s)",
                        manager=state,
                        workers=len(worker_nodes),
                    )
                )
                time.sleep(2)

            miniwdl_services = [
                d
                for d in [s.attrs for s in client.services.list()]
                if "Spec" in d and "Labels" in d["Spec"] and "miniwdl_run_id" in d["Spec"]["Labels"]
            ]
            if miniwdl_services and cfg["docker_swarm"].get_bool("auto_init"):
                logger.warning(
                    "docker swarm lists existing miniwdl-related services. This is normal if other miniwdl processes are running concurrently; otherwise, stale state could interfere with this run. To reset it, `docker swarm leave --force`"
                )
        finally:
            client.close()

        # Detect swarm's CPU & memory resources. Even on a localhost swarm, these may be less than
        # multiprocessing.cpu_count() and psutil.virtual_memory().total; in particular on macOS,
        # where Docker containers run in a virtual machine with limited resources.
        resources_max_mem = {}
        total_NanoCPUs = 0
        total_MemoryBytes = 0

        for node in worker_nodes:
            logger.debug(
                _(
                    "swarm worker",
                    ID=node.attrs["ID"],
                    Spec=node.attrs["Spec"],
                    Hostname=node.attrs["Description"]["Hostname"],
                    Resources=node.attrs["Description"]["Resources"],
                    Status=node.attrs["Status"],
                )
            )
            resources = node.attrs["Description"]["Resources"]
            total_NanoCPUs += resources["NanoCPUs"]
            total_MemoryBytes += resources["MemoryBytes"]
            if (
                not resources_max_mem
                or resources["MemoryBytes"] > resources_max_mem["MemoryBytes"]
                or (
                    resources["MemoryBytes"] == resources_max_mem["MemoryBytes"]
                    and resources["NanoCPUs"] > resources_max_mem["NanoCPUs"]
                )
            ):
                resources_max_mem = resources

        max_cpu = int(resources_max_mem["NanoCPUs"] / 1_000_000_000)
        max_mem = resources_max_mem["MemoryBytes"]
        logger.notice(  # pyre-ignore
            _(
                "docker swarm resources",
                workers=len(worker_nodes),
                max_cpus=max_cpu,
                max_mem_bytes=max_mem,
                total_cpus=int(total_NanoCPUs / 1_000_000_000),
                total_mem_bytes=total_MemoryBytes,
            )
        )
        cls._limits = {"cpu": max_cpu, "mem_bytes": max_mem}

    @classmethod
    def detect_resource_limits(cls, cfg: config.Loader, logger: logging.Logger) -> Dict[str, int]:
        assert cls._limits, f"{cls.__name__}.global_init"
        return cls._limits

    image_tag: str = "ubuntu:18.04"
    """
    :type: str

    docker image tag (set as desired before running)
    """

    create_service_kwargs: Optional[Dict[str, Any]] = None
    # override kwargs to docker service create() (may be set by plugins)

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

        # connect to dockerd
        client = docker.from_env(timeout=900)
        resources, user, groups = self.misc_config(logger, client, cpu, memory)
        svc = None
        exit_code = None
        try:
            # run container as a transient docker swarm service, letting docker handle the resource
            # scheduling (waiting until requested # of CPUs are available).
            kwargs = {
                # unique name with some human readability; docker limits to 63 chars (issue #327)
                "name": f"wdl-{os.getpid()}-{SwarmContainer._id_counter.next()}-{self.run_id}"[:63],
                "command": [
                    "/bin/bash",
                    "-c",
                    "id; ls -Rl ..; bash ../command >> ../stdout.txt 2>> ../stderr.txt",
                ],
                # restart_policy 'none' so that swarm runs the container just once
                "restart_policy": docker.types.RestartPolicy("none"),
                "workdir": os.path.join(self.container_dir, "work"),
                "mounts": mounts,
                "resources": resources,
                "user": user,
                "groups": groups,
                "labels": {"miniwdl_run_id": self.run_id},
                "container_labels": {"miniwdl_run_id": self.run_id},
            }
            kwargs.update(self.create_service_kwargs or {})
            logger.debug(_("docker create service kwargs", **kwargs))
            svc = client.services.create(self.image_tag, **kwargs)
            logger.debug(_("docker service", name=svc.name, id=svc.short_id))

            # stream stderr into log
            with PygtailLogger(logger, os.path.join(self.host_dir, "stderr.txt")) as poll_stderr:
                # poll for container exit
                while exit_code is None:
                    # spread out work over the GIL
                    # TODO: adaptive interval before container starts running (poll less frequently
                    # if it's already been waiting a long time)
                    time.sleep(random.uniform(1.0, 2.0))
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

            # retrieve and check container exit status
            assert isinstance(exit_code, int)
            return exit_code
        finally:
            if svc:
                try:
                    svc.remove()
                except:
                    logger.exception("failed to remove docker service")
                self.chown(logger, client, exit_code == 0)
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
                            "one or more input file(s) could be inaccessible to docker images that don't run as root; it may be necessary to `chmod g+r` them, or set --copy-input-files",
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
        return mounts

    def misc_config(
        self, logger: logging.Logger, client: docker.DockerClient, cpu: int, memory: int
    ) -> Tuple[Optional[Dict[str, str]], Optional[str], List[str]]:
        resources = {}
        if cpu > 0:
            # the cpu unit expected by swarm is "NanoCPUs"
            resources["cpu_limit"] = cpu * 1_000_000_000
            resources["cpu_reservation"] = cpu * 1_000_000_000
        if memory > 0:
            resources["mem_reservation"] = memory
        if resources:
            logger.debug(_("docker resources", **resources))
            resources = docker.types.Resources(**resources)
        else:
            resources = None
        user = None
        if self.cfg["task_runtime"].get_bool("as_user"):
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
            logger.debug(_("docker task status", **status))
        else:
            assert (
                len(self._observed_states or []) <= 1
            ), "docker task shouldn't disappear from service"

        # log each new state
        assert isinstance(self._observed_states, set)
        if status["State"] not in self._observed_states:
            loginfo = {"service": svc.short_id}
            if tasks:
                loginfo["task"] = tasks[0]["ID"][:10]
                if "NodeID" in tasks[0]:
                    loginfo["node"] = tasks[0]["NodeID"][:10]
            method = logger.notice if status["State"] == "running" else logger.info  # pyre-fixme
            method(_(f"docker task {status['State']}", **loginfo))
            self._observed_states.add(status["State"])

        # https://docs.docker.com/engine/swarm/how-swarm-mode-works/swarm-task-states/
        # https://github.com/moby/moby/blob/8fbf2598f58fb212230e6ddbcfbde628b0458250/api/types/swarm/task.go#L12
        if "ExitCode" in status.get("ContainerStatus", {}):
            exit_code = status["ContainerStatus"]["ExitCode"]
            assert isinstance(exit_code, int)
            if exit_code != 0 or status["State"] == "complete":
                logger.notice(  # pyre-fixme
                    _("docker task exit", state=status["State"], exit_code=exit_code)
                )
                return exit_code

        if status["State"] in ["failed", "rejected", "orphaned", "remove"]:
            raise RuntimeError(
                f"docker task {status['State']}"
                + ((": " + status["Err"]) if "Err" in status else "")
            )

        return None

    def chown(self, logger: logging.Logger, client: docker.DockerClient, success: bool) -> None:
        """
        After task completion, chown all files in the working directory to the invoking user:group,
        instead of leaving them frequently owned by root or some other arbitrary user id (image-
        dependent). We do this in a funny way via Docker; see GitHub issue #271 for discussion of
        alternatives and their problems.
        """
        if not self.cfg["task_runtime"].get_bool("as_user") and (os.geteuid() or os.getegid()):
            script = f"""
            chown -RP {os.geteuid()}:{os.getegid()} {shlex.quote(os.path.join(self.container_dir, 'work'))}
            """.strip()
            volumes = {self.host_dir: {"bind": self.container_dir, "mode": "rw"}}
            logger.debug(_("post-task chown", script=script, volumes=volumes))
            try:
                chowner = None
                try:
                    chowner = client.containers.run(
                        "alpine:3",
                        name=f"wdl-chown-{os.getpid()}-{SwarmContainer._id_counter.next()}-{self.run_id}"[
                            :63
                        ],
                        command=["/bin/ash", "-c", script],
                        volumes=volumes,
                        detach=True,
                    )
                    chowner_status = chowner.wait()
                    assert (
                        isinstance(chowner_status, dict)
                        and chowner_status.get("StatusCode", -1) == 0
                    ), str(chowner_status)
                finally:
                    if chowner:
                        chowner.remove()
            except:
                if success:
                    raise
                logger.exception("post-task chown also failed")


def run_local_task(
    cfg: config.Loader,
    task: Tree.Task,
    inputs: Env.Bindings[Value.Base],
    run_id: Optional[str] = None,
    run_dir: Optional[str] = None,
    logger_prefix: Optional[List[str]] = None,
    _run_id_stack: Optional[List[str]] = None,
    _cache: Optional[CallCache] = None,
    _plugins: Optional[List[Callable[..., Any]]] = None,
) -> Tuple[str, Env.Bindings[Value.Base]]:
    """
    Run a task locally.

    Inputs shall have been typechecked already. File inputs are presumed to be local POSIX file
    paths that can be mounted into a container.

    :param run_id: unique ID for the run, defaults to workflow name
    :param run_dir: directory under which to create a timestamp-named subdirectory for this run
                    (defaults to current working directory).
                    If the final path component is ".", then operate in run_dir directly.
    """

    # provision run directory and log file
    run_id = run_id or task.name
    _run_id_stack = _run_id_stack or []
    run_dir = provision_run_dir(task.name, run_dir)

    logger_prefix = (logger_prefix or ["wdl"]) + ["t:" + run_id]
    logger = logging.getLogger(".".join(logger_prefix))
    logfile = os.path.join(run_dir, "task.log")
    with ExitStack() as cleanup:
        fh = cleanup.enter_context(LoggingFileHandler(logger, logfile))  # pylint: disable=no-member
        fh.setFormatter(logging.Formatter(LOGGING_FORMAT))
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

        if not _run_id_stack:
            assert not _cache
            cache = cleanup.enter_context(CallCache(cfg, logger))  # pylint: disable=no-member
            cache.flock(logfile, exclusive=True)  # no containing workflow; flock task.log
        else:
            cache = _cache
        assert cache

        try:
            # start plugin coroutines and process inputs through them
            with compose_coroutines(
                [
                    (
                        lambda kwargs, cor=cor: cor(
                            cfg, logger, _run_id_stack + [run_id], run_dir, task, **kwargs
                        )
                    )
                    for cor in (
                        [cor2 for _, cor2 in sorted(config.load_plugins(cfg, "task"))]
                        + (_plugins or [])
                    )
                ],
                {"inputs": inputs},
            ) as plugins:
                recv = next(plugins)
                inputs = recv["inputs"]

                # download input files, if needed
                posix_inputs = _download_input_files(
                    cfg, logger, logger_prefix, run_dir, inputs, cache
                )

                # create appropriate TaskContainer
                container = SwarmContainer(cfg, run_id, run_dir)

                # evaluate input/postinput declarations, including mapping from host to
                # in-container file paths
                container_env = _eval_task_inputs(logger, task, posix_inputs, container)

                # evaluate runtime fields
                runtime = _eval_task_runtime(cfg, logger, task, container_env,)
                container.image_tag = str(runtime.get("docker", container.image_tag))

                # interpolate command
                command = _util.strip_leading_whitespace(
                    task.command.eval(container_env, stdlib=InputStdLib(logger, container)).value
                )[1]
                logger.debug(_("command", command=command.strip()))

                # process command/runtime/container through plugins
                recv = plugins.send(
                    {"command": command, "runtime": runtime, "container": container}
                )
                command, runtime, container = (recv[k] for k in ("command", "runtime", "container"))

                # start container & run command (and retry if needed)
                _try_task(cfg, logger, container, command, runtime)

                # evaluate output declarations
                outputs = _eval_task_outputs(logger, task, container_env, container)

                # create output_links
                outputs = link_outputs(
                    outputs, run_dir, hardlinks=cfg["file_io"].get_bool("output_hardlinks")
                )

                # process outputs through plugins
                recv = plugins.send({"outputs": outputs})
                outputs = recv["outputs"]

                # clean up, if so configured, and make sure output files will be accessible to
                # downstream tasks
                _delete_work(cfg, logger, run_dir, True)
                chmod_R_plus(run_dir, file_bits=0o660, dir_bits=0o770)

                # write outputs.json
                write_values_json(
                    outputs, os.path.join(run_dir, "outputs.json"), namespace=task.name
                )
                logger.notice("done")  # pyre-fixme
                return (run_dir, outputs)
        except Exception as exn:
            logger.debug(traceback.format_exc())
            wrapper = RunFailed(task, run_id, run_dir)
            logger.error(_(str(wrapper), dir=run_dir, **error_json(exn)))
            try:
                write_atomic(
                    json.dumps(error_json(wrapper, cause=exn), indent=2),
                    os.path.join(run_dir, "error.json"),
                )
            except Exception as exn2:
                logger.debug(traceback.format_exc())
                logger.critical(_("failed to write error.json", dir=run_dir, message=str(exn2)))
            try:
                _delete_work(cfg, logger, run_dir, False)
            except:
                logger.exception("delete_work also failed")
            raise wrapper from exn


def _download_input_files(
    cfg: config.Loader,
    logger: logging.Logger,
    logger_prefix: List[str],
    run_dir: str,
    inputs: Env.Bindings[Value.Base],
    cache: CallCache,
) -> Env.Bindings[Value.Base]:
    """
    Find all File values in the inputs (including any nested within compound values) that need
    to / can be downloaded. Download them to some location under run_dir and return a copy of the
    inputs with the URI values replaced by the downloaded filenames.
    """

    downloads = 0
    download_bytes = 0
    cached_hits = 0
    cached_bytes = 0

    def rewriter(uri: str) -> str:
        nonlocal downloads, download_bytes, cached_hits, cached_bytes
        if downloadable(cfg, uri):
            logger.info(_("download input file", uri=uri))
            cached, filename = download(
                cfg,
                logger,
                cache,
                uri,
                run_dir=os.path.join(run_dir, "download", str(downloads), "."),
                logger_prefix=logger_prefix + [f"download{downloads}"],
            )
            sz = os.path.getsize(filename)
            if cached:
                cached_hits += 1
                cached_bytes += sz
            else:
                logger.info(_("downloaded input file", uri=uri, file=filename, bytes=sz))
                downloads += 1
                download_bytes += sz
            return filename
        return uri

    ans = Value.rewrite_env_files(inputs, rewriter)
    if downloads or cached_hits:
        logger.notice(  # pyre-fixme
            _(
                "downloaded input files",
                downloaded=downloads,
                downloaded_bytes=download_bytes,
                cached=cached_hits,
                cached_bytes=cached_bytes,
            )
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


def _eval_task_runtime(
    cfg: config.Loader, logger: logging.Logger, task: Tree.Task, env: Env.Bindings[Value.Base],
) -> Dict[str, Union[int, str]]:
    runtime_values = {}
    for key, v in cfg["task_runtime"].get_dict("defaults").items():
        if isinstance(v, str):
            runtime_values[key] = Value.String(v)
        elif isinstance(v, int):
            runtime_values[key] = Value.Int(v)
        else:
            raise Error.InputError(f"invalid default runtime setting {key} = {v}")
    for key, expr in task.runtime.items():
        runtime_values[key] = expr.eval(env)
    logger.debug(_("runtime values", **dict((key, str(v)) for key, v in runtime_values.items())))
    ans = {}

    if "docker" in runtime_values:
        ans["docker"] = runtime_values["docker"].coerce(Type.String()).value

    host_limits = SwarmContainer.detect_resource_limits(cfg, logger)
    if "cpu" in runtime_values:
        cpu_value = runtime_values["cpu"].coerce(Type.Int()).value
        assert isinstance(cpu_value, int)
        cpu_max = cfg["task_runtime"].get_int("cpu_max")
        if cpu_max == 0:
            cpu_max = host_limits["cpu"]
        cpu = max(1, cpu_value if cpu_value <= cpu_max or cpu_max < 0 else cpu_max,)
        if cpu != cpu_value:
            logger.warning(
                _("runtime.cpu adjusted to host limit", original=cpu_value, adjusted=cpu)
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

        memory_max = cfg["task_runtime"]["memory_max"].strip()
        memory_max = -1 if memory_max == "-1" else parse_byte_size(memory_max)
        if memory_max == 0:
            memory_max = host_limits["mem_bytes"]
        if memory_max > 0 and memory_bytes > memory_max:
            logger.warning(
                _(
                    "runtime.memory adjusted to host limit",
                    original=memory_bytes,
                    adjusted=memory_max,
                )
            )
            memory_bytes = memory_max
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
    cfg: config.Loader,
    logger: logging.Logger,
    container: TaskContainer,
    command: str,
    runtime: Dict[str, Union[int, str]],
) -> None:
    """
    Run the task command in the container, with up to runtime.maxRetries
    """
    maxRetries = runtime.get("maxRetries", 0)
    prevRetries = 0

    while True:
        # copy input files, if needed
        if cfg["file_io"].get_bool("copy_input_files"):
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


def link_outputs(
    outputs: Env.Bindings[Value.Base], run_dir: str, hardlinks: bool = False
) -> Env.Bindings[Value.Base]:
    """
    Following a successful run, the output files may be scattered throughout a complex directory
    tree used for execution. To help navigating this, generate a subdirectory of the run directory
    containing nicely organized symlinks to the output files, and rewrite File values in the
    outputs env to use these symlinks.
    """

    def map_files(v: Value.Base, dn: str) -> Value.Base:
        if isinstance(v, Value.File):
            if os.path.isfile(v.value):
                hardlink = os.path.realpath(v.value)
                assert os.path.isfile(hardlink)
                newlink = os.path.join(dn, os.path.basename(v.value))
                os.makedirs(dn, exist_ok=False)
                if not hardlinks and path_really_within(hardlink, run_dir):
                    # make symlink relative
                    hardlink = os.path.relpath(hardlink, start=os.path.realpath(dn))
                (os.link if hardlinks else os.symlink)(hardlink, newlink)
                v.value = newlink
        # recurse into compound values
        elif isinstance(v, Value.Array) and v.value:
            d = int(math.ceil(math.log10(len(v.value))))  # how many digits needed
            for i in range(len(v.value)):
                v.value[i] = map_files(v.value[i], os.path.join(dn, str(i).rjust(d, "0")))
        elif isinstance(v, Value.Map):
            # create a subdirectory for each key, as long as the key names seem to make reasonable
            # path components; otherwise, treat the dict as a list of its values
            keys_ok = (
                sum(
                    1
                    for b in v.value
                    if re.fullmatch("[-_a-zA-Z0-9][-_a-zA-Z0-9.]*", str(b[0])) is None
                )
                == 0
            )
            d = int(math.ceil(math.log10(len(v.value))))
            for i, b in enumerate(v.value):
                v.value[i] = (
                    b[0],
                    map_files(
                        b[1], os.path.join(dn, str(b[0]) if keys_ok else str(i).rjust(d, "0"))
                    ),
                )
        elif isinstance(v, Value.Pair):
            v.value = (
                map_files(v.value[0], os.path.join(dn, "left")),
                map_files(v.value[1], os.path.join(dn, "right")),
            )
        elif isinstance(v, Value.Struct):
            for key in v.value:
                v.value[key] = map_files(v.value[key], os.path.join(dn, key))
        return v

    os.makedirs(os.path.join(run_dir, "output_links"), exist_ok=False)
    return outputs.map(
        lambda binding: Env.Binding(
            binding.name,
            map_files(
                copy.deepcopy(binding.value), os.path.join(run_dir, "output_links", binding.name),
            ),
        )
    )


def _delete_work(cfg: config.Loader, logger: logging.Logger, run_dir: str, success: bool) -> None:
    opt = cfg["task_runtime"]["delete_work"].strip().lower()
    if opt == "always" or (success and opt == "success") or (not success and opt == "failure"):
        if success and not cfg["file_io"].get_bool("output_hardlinks"):
            logger.warning(
                "ignoring configuration [task_runtime] delete_work because it requires [file_io] output_hardlinks = true"
            )
            return
        for dn in ["write_", "work"]:
            dn = os.path.join(run_dir, dn)
            if os.path.isdir(dn):
                shutil.rmtree(dn)
                logger.info(_("deleted working directory", dir=dn))


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
