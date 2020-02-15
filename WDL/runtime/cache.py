"""
framework for caching outputs of calls to tasks (incl. file URI downloader tasks) and workflows
"""

import os
import fcntl
import logging
import threading
from typing import Iterator, Dict, Any, Optional, Set, List, IO
from contextlib import contextmanager, ExitStack
from urllib.parse import urlparse
from . import config
from .._util import StructuredLogMessage as _


class CallCache:
    _cfg: config.Loader
    _lock: threading.Lock
    _flocked_files: Set[str]
    _flocks: List[ExitStack]

    def __init__(self, cfg: config.Loader):
        self._cfg = cfg
        self._lock = threading.Lock()
        self._flocked_files = set()
        self._flocks = []

    def _flock(self, filenames: List[str]) -> None:
        """
        open shared flocks on the specified filenames (all or none)
        """
        filenames2 = set(os.path.realpath(fn) for fn in filenames)
        with self._lock:
            filenames2 = filenames2 - self._flocked_files
            if filenames2:
                with ExitStack() as stack:
                    for fn in filenames2:
                        stack.enter_context(  # pylint: disable=no-member
                            _open_and_flock(fn, nonblocking=True)
                        )
                    self._flocked_files |= filenames2
                    self._flocks.append(stack.pop_all())  # pylint: disable=no-member

    def __del__(self):
        for lock in self._flocks:
            lock.close()
        if self._flocked_files:
            logging.getLogger("miniwdl-run").debug(
                _("released flocks", filenames=list(self._flocked_files))
            )

    def get(self, logger: logging.Logger, key: str) -> Optional[Dict[str, Any]]:
        """
        Resolve cache key to call outputs, if available, or None

        When matching outputs are found, shared flocks are automatically opened on all files
        referenced therein, and held open for the life of the CallCache object.
        """
        raise NotImplementedError()

    def put(self, logger: logging.Logger, key: str, outputs: Dict[str, Any]) -> None:
        raise NotImplementedError()

    # specialized caching logic for file downloads (not sensitive to the downloader task details,
    # and looked up in URI-derived folder structure instead of sqlite db)

    def download_path(self, uri: str) -> Optional[str]:
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
                self._cfg["download_cache"].get_bool("disregard_query")
                or not (parts.params or parts.query)
            )
        ):
            return None
        # check allow and deny lists
        allow = self._cfg["download_cache"].get_list("allow_prefix")
        deny = self._cfg["download_cache"].get_list("deny_prefix")
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
            self._cfg["download_cache"]["dir"], "files", parts.scheme, parts.netloc, dn, fn
        )

    def get_download(self, logger: logging.Logger, uri: str) -> Optional[str]:
        """
        Return filename of the cached download of uri, if available
        """
        p = self.download_path(uri)
        if not (p and os.path.isfile(p)):
            logger.debug(_("no download cache hit", uri=uri, cache_path=p))
            return None
        try:
            self._flock([p])
            logger.info(_("found in download cache", uri=uri, cache_path=p))
            # TODO: touch with os.utime?
            return p
        except Exception as exn:
            logger.warning(
                _(
                    "found in download cache, but unable to flock",
                    uri=uri,
                    cache_path=p,
                    exception=str(exn),
                )
            )
            return None

    def put_download(self, logger: logging.Logger, uri: str, filename: str) -> str:
        """
        Move the downloaded file to the cache location & return the new path
        or if the uri isn't cacheable, return the old path.
        """
        p = self.download_path(uri)
        if not p:
            return filename
        logger.info(_("storing in download cache", uri=uri, cache_path=p))
        os.makedirs(os.path.dirname(p), exist_ok=True)
        os.rename(filename, p)  # this had better be atomic!
        self._flock([p])
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
