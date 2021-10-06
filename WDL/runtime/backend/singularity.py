import os
import shlex
import logging
from typing import List, Dict
from .. import config
from ...Error import InputError
from ..._util import StructuredLogMessage as _
from .cli_subprocess import SubprocessBase


class SingularityContainer(SubprocessBase):
    @classmethod
    def global_init(cls, cfg: config.Loader, logger: logging.Logger) -> None:
        pass

    @classmethod
    def detect_resource_limits(cls, cfg: config.Loader, logger: logging.Logger) -> Dict[str, int]:
        return {"cpu": 8, "mem_bytes": 10000000000}

    @property
    def cli_name(self) -> str:
        return "singularity"

    def _cli_invocation(self, logger: logging.Logger) -> List[str]:
        ans = ["singularity"]
        if logger.isEnabledFor(logging.DEBUG):
            ans.append("--verbose")
        ans += [
            "run",
            "--containall",
            "--no-mount",
            "hostfs",
            "--pwd",
            os.path.join(self.container_dir, "work"),
        ]
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
