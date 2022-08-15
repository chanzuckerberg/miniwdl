# NOTE: this file is excluded from coverage analysis since alternate container backends may not be
#       available in the CI environment. To test locally: prove -v tests/singularity.t
import os
import shlex
import logging
import tempfile
import subprocess
from typing import List, Tuple, Optional
from contextlib import ExitStack
from ...Error import InputError, RuntimeError
from ..._util import StructuredLogMessage as _
from .. import config
from .cli_subprocess import SubprocessBase


class SingularityContainer(SubprocessBase):
    """
    Singularity task runtime based on cli_subprocess.SubprocessBase
    """

    image_cache_dir: Optional[str]

    @classmethod
    def global_init(cls, cfg: config.Loader, logger: logging.Logger) -> None:
        cmd = cfg.get_list("singularity", "exe") + ["--version"]
        try:
            singularity_version = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                universal_newlines=True,
            )
        except:
            raise RuntimeError(
                f"Unable to check `{' '.join(cmd)}`; verify Singularity installation"
            )

        image_cache_dir = cfg.get("singularity", "image_cache")
        if image_cache_dir != "":
            cls.image_cache_dir = os.path.abspath(image_cache_dir)
            os.makedirs(cls.image_cache_dir, exist_ok=True)
        else:
            cls.image_cache_dir = None

        logger.notice(  # pyre-ignore
            _(
                "Singularity runtime initialized (BETA)",
                singularity_version=singularity_version.stdout.strip(),
            )
        )

    @property
    def cli_name(self) -> str:
        return "singularity"

    @property
    def cli_exe(self) -> List[str]:
        return self.cfg.get_list("singularity", "exe")

    def _pull_invocation(self, logger: logging.Logger, cleanup: ExitStack) -> Tuple[str, List[str]]:
        image, invocation = super()._pull_invocation(logger, cleanup)
        docker_uri = "docker://" + image
        pulldir = self.image_cache_dir or cleanup.enter_context(
            tempfile.TemporaryDirectory(prefix="miniwdl_sif_")
        )
        image_name = docker_uri.replace("/", "_").replace(":", "_")
        image_path = os.path.join(pulldir, image_name + ".sif")
        if not os.path.exists(image_path):
            return image_path, self.cli_exe + ["pull", image_path, docker_uri]
        # If path already exists, no need to use a pull invocation.
        logger.info(_("Singularity SIF found in image cache directory", sif=image_path))
        return image_path, []

    def _run_invocation(self, logger: logging.Logger, cleanup: ExitStack, image: str) -> List[str]:
        """
        Formulate `singularity run` command-line invocation
        """

        ans = self.cli_exe
        if logger.isEnabledFor(logging.DEBUG):
            ans.append("--verbose")
        ans += [
            "run",
            "--pwd",
            os.path.join(self.container_dir, "work"),
        ]
        if self.runtime_values.get("privileged", False) is True:
            logger.warning("runtime.privileged enabled (security & portability warning)")
            ans += ["--add-caps", "all"]
        ans += self.cfg.get_list("singularity", "run_options")

        mounts = self.prepare_mounts()
        # Also create a scratch directory and mount to /tmp and /var/tmp
        # For context why this is needed:
        #   https://github.com/hpcng/singularity/issues/5718
        # TODO: provide opt-out for those able to edit /etc/singularity/singularity.conf to
        #       increase sessiondir max size
        tempdir = cleanup.enter_context(
            tempfile.TemporaryDirectory(prefix="_singularity_tmpdir_", dir=self.host_dir)
        )
        os.mkdir(os.path.join(tempdir, "tmp"))
        os.mkdir(os.path.join(tempdir, "var_tmp"))
        mounts.append(("/tmp", os.path.join(tempdir, "tmp"), True))
        mounts.append(("/var/tmp", os.path.join(tempdir, "var_tmp"), True))

        logger.info(
            _(
                "singularity invocation",
                args=" ".join(shlex.quote(s) for s in (ans + [image])),
                binds=len(mounts),
                tmpdir=tempdir,
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
        ans.append(image)
        return ans
