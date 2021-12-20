"""
Default TaskContainer implementation using Docker Swarm
"""
import os
import json
import stat
import time
import shlex
import uuid
import base64
import random
import hashlib
import logging
import threading
import traceback
import contextlib
from io import BytesIO
from typing import List, Dict, Set, Optional, Any, Callable, Tuple, Iterable
import docker
from ... import Error
from ..._util import (
    chmod_R_plus,
    PygtailLogger,
)
from ..._util import StructuredLogMessage as _
from .. import config, _statusbar
from ..error import Interrupted, Terminated
from ..task_container import TaskContainer


class SwarmContainer(TaskContainer):
    """
    TaskContainer docker (swarm) runtime
    """

    _limits: Dict[str, int] = {}

    @classmethod
    def global_init(cls, cfg: config.Loader, logger: logging.Logger) -> None:
        client = docker.from_env(version="auto")
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
                            "this host is a docker swarm worker but not a manager; "
                            "WDL task scheduling requires manager access"
                        )
                elif state == "inactive" and cfg["docker_swarm"].get_bool("auto_init"):
                    logger.warning(
                        "docker swarm is inactive on this host; "
                        "performing `docker swarm init --advertise-addr 127.0.0.1 --listen-addr 127.0.0.1`"
                    )
                    try:
                        client.swarm.init(advertise_addr="127.0.0.1", listen_addr="127.0.0.1")
                    except Exception as exn:
                        # smooth over race condition with multiple processes trying to init swarm
                        if "already part of a swarm" not in str(exn):
                            raise exn

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
                    "docker swarm lists existing miniwdl-related services. "
                    "This is normal if other miniwdl processes are running concurrently; "
                    "otherwise, stale state could interfere with this run. To reset it, `docker swarm leave --force`"
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

    create_service_kwargs: Optional[Dict[str, Any]] = None
    # override kwargs to docker service create() (may be set by plugins)

    _bind_input_files: bool = True
    _observed_states: Optional[Set[str]] = None

    def copy_input_files(self, logger: logging.Logger) -> None:
        assert self._bind_input_files
        super().copy_input_files(logger)
        # now that files have been copied into the working dir, it won't be necessary to bind-mount
        # them individually
        self._bind_input_files = False

    def _run(self, logger: logging.Logger, terminating: Callable[[], bool], command: str) -> int:
        self._observed_states = set()
        with open(os.path.join(self.host_dir, "command"), "w") as outfile:
            outfile.write(command)

        # prepare docker configuration
        client = docker.from_env(version="auto", timeout=900)
        if "inlineDockerfile" in self.runtime_values:
            logger.warning(
                "runtime.inlineDockerfile is an experimental extension, subject to change"
            )
            image_tag = self.build_inline_dockerfile(logger.getChild("inlineDockerfile"), client)
        else:
            image_tag = self.resolve_tag(
                logger, client, self.runtime_values.get("docker", "ubuntu:20.04")
            )
        mounts = self.prepare_mounts(logger)
        resources, user, groups = self.misc_config(logger)

        polling_period = self.cfg.get_float("docker_swarm", "polling_period_seconds")
        server_error_retries = self.cfg.get_int("docker_swarm", "server_error_retries")

        # run container as a transient docker swarm service, letting docker handle the resource
        # scheduling (e.g. waiting until requested # of CPUs are available).
        svc = None
        exit_code = None
        try:
            kwargs = {
                # unique name with some human readability; docker limits to 63 chars (issue #327)
                "name": self.unique_service_name(self.run_id),
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
                "env": [f"{k}={v}" for (k, v) in self.runtime_values.get("env", {}).items()],
            }
            if self.runtime_values.get("privileged", False) is True:
                logger.warning("runtime.privileged enabled (security & portability warning)")
                kwargs["cap_add"] = ["ALL"]
            kwargs.update(self.create_service_kwargs or {})
            logger.debug(_("docker create service kwargs", **kwargs))
            svc = client.services.create(image_tag, **kwargs)
            logger.debug(_("docker service", name=svc.name, id=svc.short_id))

            # stream stderr into log
            with contextlib.ExitStack() as cleanup:
                poll_stderr = cleanup.enter_context(
                    PygtailLogger(
                        logger,
                        self.host_stderr_txt(),
                        callback=self.stderr_callback,
                    )
                )

                # poll for container exit
                running_states = {"preparing", "running"}
                was_running = False
                server_errors = 0
                while exit_code is None:
                    # spread out work over the GIL
                    time.sleep(random.uniform(polling_period * 0.5, polling_period * 1.5))
                    if terminating():
                        quiet = not self._observed_states.difference(
                            # reduce log noise if the terminated task only sat in docker's queue
                            {"(UNKNOWN)", "new", "allocated", "pending"}
                        )
                        if not quiet:
                            self.poll_service(logger, svc, verbose=True)
                        raise Terminated(quiet=quiet)
                    try:
                        exit_code = self.poll_service(logger, svc)
                        if server_errors:
                            logger.error("docker service status polling succeeded after retries")
                        server_errors = 0
                    except docker.errors.APIError as exn:
                        logger.debug(traceback.format_exc())
                        logger.error(
                            _(
                                "docker service status polling error",
                                tries_remaining=(server_error_retries - server_errors),
                                exception=str(exn),
                            )
                        )
                        # retry dockerd errors (5xx status code)
                        if not exn.is_server_error() or server_errors >= server_error_retries:
                            raise
                        server_errors += 1
                    if not was_running and self._observed_states.intersection(running_states):
                        # indicate actual container start in status bar
                        # 'preparing' is when docker is pulling and extracting the image, which can
                        # be a lengthy and somewhat intensive operation, so we count it as running.
                        cleanup.enter_context(
                            _statusbar.task_running(
                                self.runtime_values.get("cpu", 0),
                                self.runtime_values.get("memory_reservation", 0),
                            )
                        )
                        was_running = True
                    if "running" in self._observed_states:
                        poll_stderr()

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
                for attempt in range(999):
                    try:
                        svc.remove()
                        if attempt:
                            logger.error("docker service removal succeeded after retries")
                        break
                    except Exception as exn:
                        logger.debug(traceback.format_exc())
                        logger.error(
                            _(
                                "docker service removal error",
                                tries_remaining=(server_error_retries - attempt),
                                exception=str(exn),
                            )
                        )
                        if attempt >= server_error_retries:
                            break
                        time.sleep(polling_period)
            self.chown(
                logger, client, isinstance(exit_code, int) and self.success_exit_code(exit_code)
            )
            client.close()

    def resolve_tag(
        self, logger: logging.Logger, client: docker.DockerClient, image_tag: str
    ) -> str:
        if ":" not in image_tag:
            # seems we need to do this explicitly under some configurations -- issue #232
            image_tag += ":latest"
        # fetch image info
        try:
            image_attrs = client.images.get(image_tag).attrs
        except docker.errors.ImageNotFound:
            try:
                logger.info(_("docker pull", tag=image_tag))
                client.images.pull(image_tag)
                image_attrs = client.images.get(image_tag).attrs
            except docker.errors.ImageNotFound:
                raise Error.RuntimeError("docker image not found: " + image_tag) from None
        image_log = {"tag": image_tag, "id": image_attrs["Id"]}
        # resolve mutable tag to immutable RepoDigest if possible, to ensure identical image will
        # be used across a multi-node swarm
        image_digest = bool(image_attrs.get("RepoDigests"))
        if image_digest and image_tag not in image_attrs["RepoDigests"]:
            image_digest = image_attrs["RepoDigests"][0]
            image_tag = image_digest
        image_log["RepoDigest"] = image_digest
        logger.notice(_("docker image", **image_log))  # pyre-fixme
        return image_tag

    def prepare_mounts(self, logger: logging.Logger) -> List[docker.types.Mount]:
        def touch_mount_point(host_path: str) -> None:
            # touching each mount point ensures they'll be owned by invoking user:group
            assert host_path.startswith(self.host_dir + "/")
            if host_path.endswith("/"):
                os.makedirs(host_path, exist_ok=True)
            else:
                os.makedirs(os.path.dirname(host_path), exist_ok=True)
                with open(host_path, "x") as _:
                    pass
            # providing g+rw on files (and g+rwx on directories) ensures the command will have
            # permission to them regardless of which uid it runs as in the container (since we add
            # the container to the invoking user's primary group)
            chmod_R_plus(host_path.rstrip("/"), file_bits=0o660, dir_bits=0o770)

        def escape(s):
            # docker processes {{ interpolations }}
            return s.replace("{{", '{{"{{"}}')

        mounts = []
        # mount input files/directories and command
        if self._bind_input_files:
            perm_warn = True
            for host_path, container_path in self.input_path_map.items():
                st = os.stat(host_path.rstrip("/"))
                if perm_warn and not (
                    (st.st_mode & stat.S_IROTH)
                    or (st.st_gid == os.getegid() and (st.st_mode & stat.S_IRGRP))
                ):
                    # file is neither world-readable, nor group-readable for the invoking user's primary group
                    logger.warning(
                        _(
                            "one or more input file(s) could be inaccessible to docker images that don't run as root; "
                            "it may be necessary to `chmod g+r` them, or set --copy-input-files",
                            example_file=host_path,
                        )
                    )
                    perm_warn = False
                assert (not container_path.endswith("/")) or stat.S_ISDIR(st.st_mode)
                host_mount_point = os.path.join(
                    self.host_dir, os.path.relpath(container_path.rstrip("/"), self.container_dir)
                )
                if not os.path.exists(host_mount_point):
                    touch_mount_point(
                        host_mount_point + ("/" if container_path.endswith("/") else "")
                    )
                mounts.append(
                    docker.types.Mount(
                        escape(container_path.rstrip("/")),
                        escape(host_path.rstrip("/")),
                        type="bind",
                        read_only=True,
                    )
                )
        mounts.append(
            docker.types.Mount(
                escape(os.path.join(self.container_dir, "command")),
                escape(os.path.join(self.host_dir, "command")),
                type="bind",
                read_only=True,
            )
        )
        # mount stdout, stderr, and working directory read/write
        touch_mount_point(self.host_stdout_txt())
        mounts.append(
            docker.types.Mount(
                escape(os.path.join(self.container_dir, "stdout.txt")),
                escape(self.host_stdout_txt()),
                type="bind",
            )
        )
        touch_mount_point(self.host_stderr_txt())
        mounts.append(
            docker.types.Mount(
                escape(os.path.join(self.container_dir, "stderr.txt")),
                escape(self.host_stderr_txt()),
                type="bind",
            )
        )
        mounts.append(
            docker.types.Mount(
                escape(os.path.join(self.container_dir, "work")),
                escape(self.host_work_dir()),
                type="bind",
            )
        )
        for p in [self.host_work_dir(), os.path.join(self.host_dir, "command")]:
            chmod_R_plus(p, file_bits=0o660, dir_bits=0o770)
        return mounts

    def misc_config(
        self, logger: logging.Logger
    ) -> Tuple[Optional[Dict[str, str]], Optional[str], List[str]]:
        resources = {}
        cpu = self.runtime_values.get("cpu", 0)
        if cpu > 0:
            # the cpu unit expected by swarm is "NanoCPUs"
            resources["cpu_limit"] = cpu * 1_000_000_000
            resources["cpu_reservation"] = cpu * 1_000_000_000
        memory_reservation = self.runtime_values.get("memory_reservation", 0)
        if memory_reservation > 0:
            resources["mem_reservation"] = memory_reservation
        memory_limit = self.runtime_values.get("memory_limit", 0)
        if memory_limit > 0:
            resources["mem_limit"] = memory_limit
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
        self, logger: logging.Logger, svc: docker.models.services.Service, verbose: bool = False
    ) -> Optional[int]:
        state = "(UNKNOWN)"
        status = {}

        svc.reload()
        assert svc.attrs["Spec"]["Labels"]["miniwdl_run_id"] == self.run_id
        tasks = svc.tasks()
        if tasks:
            assert len(tasks) == 1, "docker service should have at most 1 task"
            status = tasks[0]["Status"]
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(_("docker task status", **status))
            state = status["State"]
        else:
            assert (
                len(self._observed_states or []) <= 1
            ), "docker task shouldn't disappear from service"

        # references on docker task states:
        # https://docs.docker.com/engine/swarm/how-swarm-mode-works/swarm-task-states/
        # https://github.com/docker/swarmkit/blob/master/design/task_model.md
        # https://github.com/moby/moby/blob/8fbf2598f58fb212230e6ddbcfbde628b0458250/api/types/swarm/task.go#L12

        # log each new state
        assert isinstance(state, str) and isinstance(self._observed_states, set)
        if state not in self._observed_states:
            loginfo = {"service": svc.short_id}
            if tasks:
                loginfo["task"] = tasks[0]["ID"][:10]
                if "NodeID" in tasks[0]:
                    loginfo["node"] = tasks[0]["NodeID"][:10]
            if status.get("DesiredState") not in (None, state):
                loginfo["desired"] = status["DesiredState"]
            logmsg = status.get("Err", status.get("Message", None))
            if logmsg and logmsg != state:
                loginfo["message"] = logmsg
            method = logger.info
            if state == "running":
                method = logger.notice  # pyre-fixme
            elif state in ["failed", "shutdown", "rejected", "orphaned", "remove"]:
                method = logger.error
            method(_(f"docker task {state}", **loginfo))
            self._observed_states.add(state)

        # determine whether docker task has exited
        exit_code = None
        if "ExitCode" in status.get("ContainerStatus", {}):
            exit_code = status["ContainerStatus"]["ExitCode"]
            assert isinstance(exit_code, int)

        if state in ("complete", "failed"):
            msg = _("docker task exit", state=state, exit_code=exit_code)
            if state == "failed":
                logger.error(msg)
            else:
                logger.notice(msg)  # pyre-fixme
            assert isinstance(exit_code, int) and (exit_code == 0) == (state == "complete")
            return exit_code
        elif {state, status.get("DesiredState")}.intersection(
            {"rejected", "shutdown", "orphaned", "remove"}
        ) or exit_code not in (None, 0):
            # "rejected" state usually arises from nonexistent docker image.
            # if the worker assigned a task goes down, any of the following can manifest:
            #   - exit_code=-1 with state running (or other non-terminal)
            #   - state shutdown, orphaned, remove
            #   - desired_state shutdown
            # also see GitHub issue #374
            raise (Error.RuntimeError if state == "rejected" else Interrupted)(
                f"docker task {state}"
                + (
                    (", desired state " + status["DesiredState"])
                    if status.get("DesiredState") not in (None, state)
                    else ""
                )
                + (f", exit code = {exit_code}" if exit_code not in (None, 0) else "")
                + (f": {status['Err']}" if "Err" in status else "")
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
            paste = shlex.quote(
                os.path.join(
                    self.container_dir, f"work{self.try_counter if self.try_counter > 1 else ''}"
                )
            )
            script = f"""
            (find {paste} -type d -print0 && find {paste} -type f -print0 \
                && find {paste} -type l -print0) \
                | xargs -0 -P 10 chown -Ph {os.geteuid()}:{os.getegid()}
            """.strip()
            volumes = {self.host_dir: {"bind": self.container_dir, "mode": "rw"}}
            logger.debug(_("post-task chown", script=script, volumes=volumes))
            try:
                chowner = None
                try:
                    chowner = client.containers.run(
                        "alpine:3",
                        name=self.unique_service_name("chown-" + self.run_id),
                        command=["/bin/ash", "-eo", "pipefail", "-c", script],
                        volumes=volumes,
                        detach=True,
                    )
                    chowner_status = chowner.wait()
                    assert (
                        isinstance(chowner_status, dict)
                        and chowner_status.get("StatusCode", -1) == 0
                    ), f"post-task chown failed: {chowner_status}"
                finally:
                    if chowner:
                        chowner.remove()
            except Exception as exn:
                logger.debug(traceback.format_exc())
                if success:
                    raise
                logger.error(_("post-task chown also failed", exception=str(exn)))

    def unique_service_name(self, run_id: str) -> str:
        # We need to give each service a name unique on the swarm; collisions cause the service
        # create request to fail. Considerations:
        # 1. [0-9A-Za-z-]{1,63} -- case is remembered, but comparison ignores it.
        # 2. It's useful for the names to be mostly human-readable via `docker service ls` to get a
        #    sense of what's happening on the swarm. Unfortunately, that tool truncates the display
        #    names pretty short, so prefer human-readability of the leftmost part of the name.
        # 3. PID+seqno isn't sufficient because the swarm could receive submissions from miniwdl
        #    running in different hosts/VMs/containers with potentially colliding PIDs.
        # see GitHub issues: 327, 368
        junk = hashlib.sha256()
        junk.update(uuid.uuid1().bytes)
        junk.update(uuid.uuid4().bytes)
        junk = junk.digest()[:15]
        junk = base64.b32encode(junk).decode().lower()
        assert len(junk) == 24
        return f"wdl-{run_id[:34]}-{junk}"  # 4 + 34 + 1 + 24 = 63

    _build_inline_dockerfile_lock: threading.Lock = threading.Lock()

    def build_inline_dockerfile(
        self,
        logger: logging.Logger,
        client: docker.DockerClient,
        tries: Optional[int] = None,
    ) -> str:
        # formulate image tag using digest of dockerfile text
        dockerfile_utf8 = self.runtime_values["inlineDockerfile"].encode("utf8")
        dockerfile_digest = hashlib.sha256(dockerfile_utf8).digest()
        dockerfile_digest = base64.b32encode(dockerfile_digest[:15]).decode().lower()
        tag_part1 = "miniwdl_auto_"
        tag_part3 = ":" + dockerfile_digest
        tag_part2 = self.run_id.lower()
        if "-" in tag_part2:
            tag_part2 = tag_part2.split("-")[1]
        maxtag2 = 63 - len(tag_part1) - len(tag_part3)
        assert maxtag2 > 0
        tag = tag_part1 + tag_part2[:maxtag2] + tag_part3

        # short-circuit if digest-tagged image already exists
        try:
            existing = client.images.get(tag)
            logger.notice(_("docker build cached", tag=tag, id=existing.id))  # pyre-ignore
            return tag
        except docker.errors.ImageNotFound:
            pass

        # prepare to tee docker build log to logger.verbose and a file
        build_logfile = os.path.join(self.host_dir, "inlineDockerfile.log")

        def write_log(stream: Iterable[Dict[str, str]]):
            # tee the log messages to logger.verbose and build_logfile
            with open(build_logfile, "w") as outfile:
                for d in stream:
                    if "stream" in d:
                        for msg in d["stream"].split("\n"):
                            msg = msg.rstrip()
                            if msg:
                                logger.verbose(msg)
                                print(msg, file=outfile)

        # run docker build
        try:
            with SwarmContainer._build_inline_dockerfile_lock:  # one build at a time
                logger.info(_("starting docker build", tag=tag))
                logger.debug(_("Dockerfile", txt=self.runtime_values["inlineDockerfile"]))
                image, build_log = client.images.build(fileobj=BytesIO(dockerfile_utf8), tag=tag)
        except docker.errors.BuildError as exn:
            # potentially retry, if task has runtime.maxRetries
            if isinstance(tries, int):
                tries -= 1
            else:
                tries = self.runtime_values.get("maxRetries", 0)
            if tries > 0:
                logger.error(
                    _("failed docker build will be retried", tries_remaining=tries, msg=exn.msg)
                )
                return self.build_inline_dockerfile(logger, client, tries=tries)
            else:
                write_log(exn.build_log)
                logger.error(_("docker build failed", msg=exn.msg, log=build_logfile))
                raise exn

        write_log(build_log)
        logger.notice(  # pyre-ignore
            _("docker build", tag=image.tags[0], id=image.id, log=build_logfile)
        )
        return tag
