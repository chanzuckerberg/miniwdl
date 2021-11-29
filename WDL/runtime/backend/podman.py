import os
import shlex
import logging
import subprocess
from typing import List, Tuple
from contextlib import ExitStack
from ...Error import InputError, RuntimeError
from ..._util import StructuredLogMessage as _
from .. import config
from .cli_subprocess import SubprocessBase


class PodmanContainer(SubprocessBase):
    """
    podman task runtime based on cli_subprocess.SubprocessBase
    """

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

    def _pull_invocation(self, logger: logging.Logger, cleanup: ExitStack) -> Tuple[str, List[str]]:
        image, invocation = super()._pull_invocation(logger, cleanup)
        if os.geteuid():
            invocation = ["sudo", "-n"] + invocation
            _sudo_canary()
        return (image, invocation)

    def _run_invocation(self, logger: logging.Logger, cleanup: ExitStack, image: str) -> List[str]:
        """
        Formulate `podman run` command-line invocation
        """
        ans = ["podman"]
        if os.geteuid():
            ans = ["sudo", "-n"] + ans
            _sudo_canary()
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

        cleanup.callback(lambda: self._chown(logger))
        return ans

    def _chown(self, logger: logging.Logger):
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
