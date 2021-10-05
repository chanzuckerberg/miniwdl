import os
import logging
from typing import List, Dict
from .. import config
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

    def _cli_invocation(self) -> List[str]:
        ans = [
            "singularity",
            "run",
            "--containall",
            "--no-mount",
            "hostfs",
            "--pwd",
            os.path.join(self.container_dir, "work"),
        ]
        for (container_path, host_path, writable) in self.prepare_mounts():
            ans.append("--bind")
            bind_arg = f"{host_path}:{container_path}"
            if not writable:
                bind_arg += ":ro"
            ans.append(bind_arg)
        ans.append("docker://" + self.runtime_values.get("docker", "ubuntu:20.04"))
        return ans
