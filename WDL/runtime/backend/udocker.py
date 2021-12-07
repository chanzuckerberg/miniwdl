import os
import shlex
import logging
import subprocess
from typing import List
from contextlib import ExitStack
from ...Error import InputError, RuntimeError
from ..._util import StructuredLogMessage as _
from .. import config
from .cli_subprocess import SubprocessBase


class UdockerContainer(SubprocessBase):
    """
    udocker task runtime based on cli_subprocess.SubprocessBase
    """

    @classmethod
    def global_init(cls, cfg: config.Loader, logger: logging.Logger) -> None:
        cmd = cfg.get_list("udocker", "exe") + ["--version"]
        try:
            udocker_version = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                universal_newlines=True,
            )
        except:
            raise RuntimeError(f"Unable to check `{' '.join(cmd)}`; verify udocker installation")
        logger.notice(  # pyre-ignore
            _(
                "udocker runtime initialized (BETA)",
                udocker_version=udocker_version.stdout.strip().split("\n"),
            )
        )

    @property
    def cli_name(self) -> str:
        return "udocker"

    @property
    def cli_exe(self) -> List[str]:
        return self.cfg.get_list("udocker", "exe")

    def _run_invocation(self, logger: logging.Logger, cleanup: ExitStack, image: str) -> List[str]:
        """
        Formulate `udocker run` command-line invocation
        """
        ans = self.cli_exe + [
            "run",
            "--workdir",
            os.path.join(self.container_dir, "work"),
        ]
        ans += self.cfg.get_list("udocker", "run_options")

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
