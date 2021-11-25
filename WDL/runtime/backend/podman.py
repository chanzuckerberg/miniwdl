import os
import time
import shlex
import logging
import itertools
import threading
import subprocess
from typing import List, Callable, Optional
from ...Error import InputError, RuntimeError
from ..._util import StructuredLogMessage as _
from ..._util import rmtree_atomic
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
        if os.geteuid() != 0:
            raise RuntimeError(
                "Podman tasks require `sudo miniwdl run ...` (or `miniwdl run` as root)"
            )
        try:
            podman_version = subprocess.run(
                ["podman", "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                universal_newlines=True,
            )
        except:
            raise RuntimeError("Unable to check `podman --version`; verify Podman installation")
        logger.warning(
            _(
                "Podman runtime is experimental; use with caution",
                version=podman_version.stdout.strip(),
            )
        )
        pass

    @property
    def cli_name(self) -> str:
        return "podman"

    def _cli_invocation(self, logger: logging.Logger) -> List[str]:
        """
        Formulate `podman run` command-line invocation
        """
        image = self._podman_pull(logger)

        ans = ["podman"]
        ans += [
            "run",
            "--rm",
            "--workdir",
            os.path.join(self.container_dir, "work"),
        ]
        # TODO: set --cpus and --memory

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
        return ans

    def _podman_pull(self, logger: logging.Logger) -> str:
        """
        Ensure the needed docker image is cached by singularity. Use a global lock so we'll only
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
                logger.info(_("begin podman pull", image=image))
                puller = subprocess.run(
                    ["podman", "pull", image],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
                if puller.returncode != 0:
                    logger.error(
                        _(
                            "podman pull failed",
                            stderr=puller.stderr.split("\n"),
                            stdout=puller.stdout.split("\n"),
                        )
                    )
                    raise DownloadFailed(image)
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
