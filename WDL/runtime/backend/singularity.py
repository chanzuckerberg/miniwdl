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


class SingularityContainer(SubprocessBase):
    """
    Singularity task runtime based on cli_subprocess.SubprocessBase
    """

    _tempdir: Optional[str] = None
    _pull_lock: threading.Lock = threading.Lock()
    _pulled_images = set()

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
            raise RuntimeError(
                "Unable to check `singularity --version`; verify Singularity installation"
            )
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
    def docker_uri(self) -> str:
        return "docker://" + self.runtime_values.get(
            "docker", self.cfg.get_dict("task_runtime", "defaults")["docker"]
        )

    def _cli_invocation(self, logger: logging.Logger) -> List[str]:
        """
        Formulate `singularity run` command-line invocation
        """
        self._singularity_pull(logger)

        ans = ["singularity"]
        if logger.isEnabledFor(logging.DEBUG):
            ans.append("--verbose")
        ans += [
            "run",
            "--pwd",
            os.path.join(self.container_dir, "work"),
        ]
        ans += self.cfg.get_list("singularity", "cli_options")

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
                args=" ".join(shlex.quote(s) for s in (ans + [self.docker_uri])),
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
        ans.append(self.docker_uri)
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

    def _singularity_pull(self, logger: logging.Logger):
        """
        Ensure the needed docker image is cached by singularity. Use a global lock so we'll only
        download it once, even if used by many parallel tasks all starting at the same time.
        """
        t0 = time.time()
        with self._pull_lock:
            t1 = time.time()
            docker_uri = self.docker_uri

            if docker_uri in self._pulled_images:
                logger.info(_("singularity image already pulled", uri=docker_uri))
                return

            with tempfile.TemporaryDirectory(prefix="miniwdl_sif_") as pulldir:
                logger.info(_("begin singularity pull", uri=docker_uri, tempdir=pulldir))
                puller = subprocess.run(
                    ["singularity", "pull", docker_uri],
                    cwd=pulldir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
                if puller.returncode != 0:
                    logger.error(
                        _(
                            "singularity pull failed",
                            stderr=puller.stderr.split("\n"),
                            stdout=puller.stdout.split("\n"),
                        )
                    )
                    raise DownloadFailed(docker_uri)
                # The docker image layers are cached in SINGULARITY_CACHEDIR, so we don't need to
                # keep {pulldir}/*.sif

            self._pulled_images.add(docker_uri)

        # TODO: log image sha256sum?
        logger.notice(
            _(
                "singularity pull",
                uri=docker_uri,
                seconds_waited=int(t1 - t0),
                seconds_pulling=int(time.time() - t1),
            )
        )
