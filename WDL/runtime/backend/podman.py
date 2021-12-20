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

        if self.cfg.get_bool("task_runtime", "as_user"):
            if os.geteuid() == 0:
                logger.warning(
                    "container command will run explicitly as root, since you are root and set --as-me"
                )
            ans += ["--user", f"{os.geteuid()}:{os.getegid()}"]

        if self.runtime_values.get("privileged", False) is True:
            logger.warning("runtime.privileged enabled (security & portability warning)")
            ans.append("--privileged")

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
        if not self.cfg.get_bool("task_runtime", "as_user") and (os.geteuid() or os.getegid()):
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
            try:
                subprocess.run(
                    [
                        "sudo",
                        "-n",
                        "podman",
                        "run",
                        "--rm",
                        "-v",
                        shlex.quote(f"{self.host_dir}:{self.container_dir}"),
                        "alpine:3",
                        "/bin/ash",
                        "-eo",
                        "pipefail",
                        "-c",
                        script,
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
