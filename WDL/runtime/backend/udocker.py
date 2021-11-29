import os
import time
import shlex
import logging
import tempfile
import threading
import subprocess
from typing import List, Callable, Optional
from ...Error import InputError, RuntimeError
from ..._util import StructuredLogMessage as _
from ..._util import rmtree_atomic
from .. import config
from ..error import DownloadFailed
from .cli_subprocess import SubprocessBase


class UdockerContainer(SubprocessBase):
    """
    udocker task runtime based on cli_subprocess.SubprocessBase
    """

    _pull_lock: threading.Lock = threading.Lock()
    _pulled_images = set()

    @classmethod
    def global_init(cls, cfg: config.Loader, logger: logging.Logger) -> None:
        try:
            udocker_version = subprocess.run(
                ["udocker", "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                universal_newlines=True,
            )
        except:
            raise RuntimeError("Unable to check `udocker --version`; verify udocker installation")
        logger.notice(  # pyre-ignore
            _(
                "udocker runtime initialized (BETA)",
                udocker_version=udocker_version.stdout.strip().split("\n"),
            )
        )

    @property
    def cli_name(self) -> str:
        return "udocker"

    def _cli_invocation(self, logger: logging.Logger) -> List[str]:
        """
        Formulate `udocker run` command-line invocation
        """
        image = self._udocker_pull(logger)

        ans = [
            "udocker",
            "run",
            "--workdir",
            os.path.join(self.container_dir, "work"),
        ]
        ans += self.cfg.get_list("udocker", "cli_options")

        mounts = self.prepare_mounts()
        logger.info(
            _(
                "udocker invocation",
                args=" ".join(shlex.quote(s) for s in (ans + [image])),
                binds=len(mounts),
            )
        )
        for (container_path, host_path, _writable) in mounts:
            if ":" in (container_path + host_path):
                raise InputError("udocker input filenames cannot contain ':'")
            ans.append("-v")
            ans.append(f"{host_path}:{container_path}")
        ans.append(image)
        return ans

    def _udocker_pull(self, logger: logging.Logger) -> str:
        """
        Ensure the needed docker image is cached by udocker. Use a global lock so we'll only
        download it once, even if used by many parallel tasks all starting at the same time.
        """
        image = self.runtime_values.get(
            "docker", self.cfg.get_dict("task_runtime", "defaults")["docker"]
        )
        t0 = time.time()
        with self._pull_lock:
            t1 = time.time()

            if image in self._pulled_images:
                logger.info(_("udocker image already pulled", image=image))
            else:
                udocker_pull_cmd = ["udocker", "pull", image]
                logger.info(_("begin udocker pull", command=" ".join(udocker_pull_cmd)))
                try:
                    subprocess.run(
                        udocker_pull_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True,
                        check=True,
                    )
                except subprocess.CalledProcessError as cpe:
                    logger.error(
                        _(
                            "udocker pull failed",
                            stderr=cpe.stderr.strip().split("\n"),
                            stdout=cpe.stdout.strip().split("\n"),
                        )
                    )
                    raise DownloadFailed(image) from None
                self._pulled_images.add(image)

        logger.notice(  # pyre-ignore
            _(
                "udocker pull",
                image=image,
                seconds_waited=int(t1 - t0),
                seconds_pulling=int(time.time() - t1),
            )
        )
        return image
