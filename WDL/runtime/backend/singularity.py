import os
import shlex
import psutil
import shutil
import logging
import tempfile
import subprocess
import multiprocessing
from typing import List, Dict, Callable, Optional
from .. import config
from ...Error import InputError
from ..._util import StructuredLogMessage as _
from ..._util import rmtree_atomic
from .cli_subprocess import SubprocessBase


class SingularityContainer(SubprocessBase):
    """
    Singularity task runtime based on cli_subprocess.SubprocessBase
    """

    _resource_limits: Dict[str, int]
    _tempdir: Optional[str] = None

    @classmethod
    def global_init(cls, cfg: config.Loader, logger: logging.Logger) -> None:
        try:
            singularity_version = subprocess.run(
                ["singularity", "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                universal_newlines=True,
            )
        except:
            assert False, "Unable to check `singularity --version`; verify Singularity installation"
        logger.warning(
            _(
                "Singularity runtime is experimental; use with caution",
                version=singularity_version.stdout.strip(),
            )
        )
        cls._resource_limits = {
            "cpu": multiprocessing.cpu_count(),
            "mem_bytes": psutil.virtual_memory().total,
        }
        logger.info(
            _(
                "detected host resources",
                cpu=cls._resource_limits["cpu"],
                mem_bytes=cls._resource_limits["mem_bytes"],
            )
        )
        pass

    @classmethod
    def detect_resource_limits(cls, cfg: config.Loader, logger: logging.Logger) -> Dict[str, int]:
        return cls._resource_limits

    @property
    def cli_name(self) -> str:
        return "singularity"

    def _cli_invocation(self, logger: logging.Logger) -> List[str]:
        """
        Formulate `singularity run` command-line invocation
        """
        ans = ["singularity"]
        if logger.isEnabledFor(logging.DEBUG):
            ans.append("--verbose")
        ans += [
            "run",
            "--pwd",
            os.path.join(self.container_dir, "work"),
        ]
        ans += self.cfg.get_list("singularity", "cli_options")
        docker_uri = "docker://" + self.runtime_values.get("docker", "ubuntu:20.04")

        mounts = self.prepare_mounts()
        # Also create a scratch directory and mount to /tmp and /var/tmp
        # For context why this is needed:
        #   https://github.com/hpcng/singularity/issues/5718
        self._tempdir = tempfile.mkdtemp(prefix="miniwdl_singularity_")
        os.mkdir(os.path.join(self._tempdir, "tmp"))
        os.mkdir(os.path.join(self._tempdir, "var_tmp"))
        mounts.append(("/tmp", os.path.join(self._tempdir, "tmp"), True))
        mounts.append(("/var/tmp", os.path.join(self._tempdir, "var_tmp"), True))

        logger.info(
            _(
                "singularity invocation",
                args=" ".join(shlex.quote(s) for s in (ans + [docker_uri])),
                binds=len(mounts),
                tmpdir=self._tempdir,
            )
        )
        for (container_path, host_path, writable) in mounts:
            if ":" in (container_path + host_path):
                raise InputError("Singularity input filenames cannot contain ':'")
            ans.append("--bind")
            bind_arg = f"{host_path}:{container_path}"
            if not writable:
                bind_arg += ":ro"
            ans.append(bind_arg)
        ans.append(docker_uri)
        return ans

    def _run(self, logger: logging.Logger, terminating: Callable[[], bool], command: str) -> int:
        """
        Override to clean up aforementioned scratch directory after container exit
        """
        try:
            return super()._run(logger, terminating, command)
        finally:
            if self._tempdir:
                logger.info(_("delete container temporary directory", tmpdir=self._tempdir))
                rmtree_atomic(self._tempdir)
