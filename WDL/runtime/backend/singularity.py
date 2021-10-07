import os
import shlex
import psutil
import logging
import subprocess
import multiprocessing
from typing import List, Dict
from .. import config
from ...Error import InputError
from ..._util import StructuredLogMessage as _
from .cli_subprocess import SubprocessBase


class SingularityContainer(SubprocessBase):
    _resource_limits: Dict[str, int]

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
        logger.info(
            _(
                "singularity base invocation",
                args=" ".join(shlex.quote(s) for s in (ans + [docker_uri])),
                binds=len(mounts),
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
