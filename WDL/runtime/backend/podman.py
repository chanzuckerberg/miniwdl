import os
import time
import shlex
import logging
import threading
import subprocess
from typing import List, Callable, Optional
from ...Error import InputError, RuntimeError
from ..._util import StructuredLogMessage as _
from .. import config
from ..error import DownloadFailed
from .cli_subprocess import SubprocessBase


class PodmanContainer(SubprocessBase):
    """
    podman task runtime based on cli_subprocess.SubprocessBase
    """

    _tempdir: Optional[str] = None
    _pull_lock: threading.Lock = threading.Lock()
    _pulled_images = set()

    @classmethod
    def global_init(cls, cfg: config.Loader, logger: logging.Logger) -> None:
        podman_version_cmd = ["podman", "--version"]
        if os.geteuid():
            podman_version_cmd = ["sudo", "-n"] + podman_version_cmd

        try:
            podman_version = subprocess.run(
                podman_version_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )
        except subprocess.CalledProcessError as cpe:
            logger.error(_(" ".join(podman_version_cmd), stderr=cpe.stderr.strip().split("\n")))
            raise RuntimeError(
                "Unable to check `sudo podman --version`; verify Podman installation"
                " and no-password sudo (or run miniwdl as root)"
                if os.geteuid()
                else "Unable to check `podman --version`; verify Podman installation"
            ) from None

        logger.notice(  # pyre-ignore
            _(
                "Podman runtime initialized (BETA)",
                podman_version=podman_version.stdout.strip(),
            )
        )

    @property
    def cli_name(self) -> str:
        return "podman"

    def _cli_invocation(self, logger: logging.Logger) -> List[str]:
        """
        Formulate `podman run` command-line invocation
        """
        image = self._podman_pull(logger)

        ans = ["podman"]
        if os.geteuid():
            ans = ["sudo", "-n"] + ans
        ans += [
            "run",
            "--rm",
            "--workdir",
            os.path.join(self.container_dir, "work"),
        ]

        cpu = self.runtime_values.get("cpu", 0)
        if cpu > 0:
            ans += ["--cpus", str(cpu)]
        memory_limit = self.runtime_values.get("memory_limit", 0)
        if memory_limit > 0:
            ans += ["--memory", str(memory_limit)]

        mounts = self.prepare_mounts()
        logger.info(
            _(
                "podman invocation",
                args=" ".join(shlex.quote(s) for s in (ans + [image])),
                binds=len(mounts),
            )
        )
        for (container_path, host_path, writable) in mounts:
            if ":" in (container_path + host_path):
                raise InputError("Podman input filenames cannot contain ':'")
            ans.append("-v")
            bind_arg = f"{host_path}:{container_path}"
            if not writable:
                bind_arg += ":ro"
            ans.append(bind_arg)
        ans.append(image)
        _sudo_canary()
        return ans

    def _podman_pull(self, logger: logging.Logger) -> str:
        """
        Ensure the needed docker image is cached by podman. Use a global lock so we'll only
        download it once, even if used by many parallel tasks all starting at the same time.
        """
        image = self.runtime_values.get(
            "docker", self.cfg.get_dict("task_runtime", "defaults")["docker"]
        )
        t0 = time.time()
        with self._pull_lock:
            t1 = time.time()

            if image in self._pulled_images:
                logger.info(_("podman image already pulled", image=image))
            else:
                _sudo_canary()
                podman_pull_cmd = ["podman", "pull", image]
                if os.geteuid():
                    podman_pull_cmd = ["sudo", "-n"] + podman_pull_cmd
                logger.info(_("begin podman pull", command=" ".join(podman_pull_cmd)))
                try:
                    subprocess.run(
                        podman_pull_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True,
                        check=True,
                    )
                except subprocess.CalledProcessError as cpe:
                    logger.error(
                        _(
                            "podman pull failed",
                            stderr=cpe.stderr.strip().split("\n"),
                            stdout=cpe.stdout.strip().split("\n"),
                        )
                    )
                    raise DownloadFailed(image) from None
                self._pulled_images.add(image)

        # TODO: log image ID?
        logger.notice(  # pyre-ignore
            _(
                "podman pull",
                image=image,
                seconds_waited=int(t1 - t0),
                seconds_pulling=int(time.time() - t1),
            )
        )
        return image

    def _run(self, logger: logging.Logger, terminating: Callable[[], bool], command: str) -> int:
        """
        Override to chown working directory
        """
        _sudo_canary()
        try:
            return super()._run(logger, terminating, command)
        finally:
            if os.geteuid():
                try:
                    subprocess.run(
                        [
                            "sudo",
                            "-n",
                            "chown",
                            "-RPh",
                            f"{os.geteuid()}:{os.getegid()}",
                            self.host_work_dir(),
                        ],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True,
                        check=True,
                    )
                except subprocess.CalledProcessError as cpe:
                    logger.error(_("post-task chown failed", error=cpe.stderr.strip().split("\n")))


def _sudo_canary():
    if os.geteuid():
        try:
            subprocess.run(
                ["sudo", "-n", "id"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )
        except subprocess.SubprocessError:
            raise RuntimeError(
                "passwordless sudo expired (required for Podman)"
                "; see miniwdl/podman documentation for workarounds"
            )
