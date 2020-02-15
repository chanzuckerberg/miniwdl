"""
framework for caching outputs of calls to tasks (incl. file URI downloader tasks) and workflows
"""

import os
import fcntl
import logging
from typing import Iterator, Iterable, Union, TextIO, BinaryIO, Dict, Any, Optional, Set, List, IO
from contextlib import contextmanager, ExitStack
from urllib.parse import urlparse
from . import config
from .._util import StructuredLogMessage as _


class CallCache:
    cfg: config.Loader
    logger: logging.Logger
    locked_files: Set[str]
    locks: List[ExitStack]

    def __init__(self, cfg: config.Loader):
        self.cfg = cfg
        self.logger = logging.getLogger("miniwdl.CallCache")
        self.locked_files = set()
        self.locks = []

    def _flock(self, filenames: List[str]) -> None:
        """
        open shared flocks on the specified filenames (all or none)
        """
        filenames2 = set(filenames) - self.locked_files
        if filenames2:
            with ExitStack() as stack:
                for fn in filenames2:
                    stack.enter_context(_open_and_flock(fn, nonblocking=True))
                self.locked_files |= filenames2
                self.locks.append(stack.pop_all())

    def __del__(self):
        for lock in self.locks:
            lock.close()
        if self.locks:
            self.logger.debug(_("released flocks", filenames=list(self.locked_files)))

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Resolve cache key to call outputs, if available, or None

        When matching outputs are found, shared flocks are automatically opened on all files
        referenced therein, and held open for the life of the CallCache object.
        """
        return None

    def put(self, key: str, outputs: Dict[str, Any]) -> None:
        pass


class DownloadCache(CallCache):
    """
    specializes for file downloads:

    - keyed by URI, disregarding the downloader task details
    - no sqlite db; directory structure reflects cached URIs
    - expectation is that new downloaded files will be deposited there
    """

    def __init__(self, cfg: config.Loader):
        super().__init__(cfg)
        self.logger = logging.getLogger("miniwdl.DownloadCache")

    def cache_path(self, uri: str) -> Optional[str]:
        """
        Based on the uri, compute the local file path at which the cached copy should exist (or
        None if the uri is not cacheable)
        """
        # check if URI is properly formatted
        parts = urlparse(uri)
        if not (
            parts.scheme
            and parts.netloc
            and (
                self.cfg["download_cache"].get_bool("disregard_query")
                or not (parts.params or parts.query)
            )
        ):
            return None
        # check allow and deny lists
        allow = self.cfg["download_cache"].get_list("allow_prefix")
        deny = self.cfg["download_cache"].get_list("deny_prefix")
        if (allow and not next((pfx for pfx in allow if uri.startswith(pfx)), False)) or next(
            (pfx for pfx in deny if uri.startswith(pfx)), False
        ):
            return None
        # formulate path
        (dn, fn) = os.path.split(parts.path)
        if not fn:
            return None
        dn = dn.strip("/")
        if dn:
            dn = dn.replace("_", "__")
            dn = dn.replace("/", "_")
        return os.path.join(
            self.cfg["download_cache"]["dir"], "files", parts.scheme, parts.netloc, dn, fn
        )

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        p = self.cache_path(key)
        if not (p and os.path.isfile(p)):
            self.logger.debug(_("no download cache hit", uri=key, cache_path=p))
            return None
        try:
            self._flock([p])
            self.logger.info(_("found in download cache and flocked", uri=key, cache_path=p))
            # TODO: touch with os.utime?
            return {"file": p}
        except Exception as exn:
            self.logger.warning(
                _(
                    "download cache hit found but unable to flock",
                    uri=key,
                    cache_path=p,
                    exception=str(exn),
                )
            )
            return None

    def put(self, key: str, outputs: Dict[str, Any]) -> None:
        """
        use put_download specialized for DownloadCache
        """
        raise NotImplementedError()

    def put_download(self, uri: str, filename: str) -> str:
        """
        move the file to the cache location & return the new path
        or if the uri isn't cacheable, return the old path.
        """
        p = self.cache_path(uri)
        if not p:
            return filename
        os.makedirs(os.path.dirname(p), exist_ok=True)
        os.rename(filename, p)
        return p


@contextmanager
def _open_and_flock(
    filename: str, mode: str = "rb", exclusive: bool = False, nonblocking: bool = False
) -> Iterator[IO[Any]]:
    """
    context manager yields an open BinaryIO/TextIO with a flock on the file
    """
    with open(filename, mode) as openfile:
        op = fcntl.LOCK_SH
        if exclusive:
            op = fcntl.LOCK_EX
        if nonblocking:
            op |= fcntl.LOCK_NB
        fcntl.flock(openfile, op)
        try:
            # verify the hardlink didn't change in between our open & flock syscalls
            filename_st = os.stat(filename)
            file_st = os.stat(openfile.fileno())
            if filename_st.st_dev != file_st.st_dev or filename_st.st_ino != file_st.st_ino:
                raise RuntimeError("concurrent file change")
            yield openfile
        finally:
            fcntl.flock(openfile, fcntl.LOCK_UN)
