"""
Caching outputs of task/workflow calls (incl. file URI downloader tasks) based on source code and
inputs. When cached outputs are found for reuse, opens advisory locks (flocks) on any local files
referenced therein, and updates their access timestamps (atime).
"""

import os
import time
import fcntl
import logging
import threading
from typing import Iterator, Dict, Any, Optional, Set, List, IO
from contextlib import contextmanager, ExitStack
from urllib.parse import urlparse, urlunparse
from fnmatch import fnmatchcase
from . import config
from .. import Env, Value, Type
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
        # open shared flocks on the specified filenames (all or none)
        filenames2 = set(os.path.realpath(fn) for fn in filenames)
        with self._lock:
            filenames2 = filenames2 - self._flocked_files
            if filenames2:
                with ExitStack() as stack:
                    for fn in filenames2:
                        stack.enter_context(_open_and_flock(fn))  # pylint: disable=no-member
                    self._flocked_files |= filenames2
                    self._flocks.append(stack.pop_all())  # pylint: disable=no-member

    def __del__(self):
        with self._lock:
            for lock in self._flocks:
                lock.close()

    def get(
        self, logger: logging.Logger, key: str, output_types: Env.Bindings[Type.Base]
    ) -> Optional[Env.Bindings[Value.Base]]:
        """
        Resolve cache key to call outputs, if available, or None. When matching outputs are found,
        opens shared flocks on all files referenced therein, which will remain for the life of the
        CallCache object.
        """
        raise NotImplementedError()

    def put(self, logger: logging.Logger, key: str, outputs: Env.Bindings[Value.Base]) -> None:
        """
        Store call outputs for future reuse
        """
        raise NotImplementedError()

    # specialized caching logic for file downloads (not sensitive to the downloader task details,
    # and looked up in URI-derived folder structure instead of sqlite db)

    def download_path(self, uri: str) -> Optional[str]:
        """
        Based on the input download uri, compute the local file path at which the cached copy
        should exist (or None if the uri is not cacheable)
        """
        # check if URI is properly formatted & normalize
        parts = urlparse(uri)
        if (
            parts.scheme
            and parts.netloc
            and (
                self._cfg["download_cache"].get_bool("disregard_query")
                or not (parts.params or parts.query or parts.fragment)
            )
        ):
            uri = urlunparse((parts.scheme, parts.netloc, parts.path, "", "", ""))
            # check allow & deny patterns
            allow = self._cfg["download_cache"].get_list("allow_patterns") or ["*"]
            deny = self._cfg["download_cache"].get_list("deny_patterns")
            if next((pat for pat in allow if fnmatchcase(uri, pat)), False) and not next(
                (pat for pat in deny if fnmatchcase(uri, pat)), False
            ):
                (dn, fn) = os.path.split(parts.path)
                if fn:
                    # formulate path
                    dn = dn.strip("/")
                    if dn:
                        dn = dn.replace("_", "__")
                        dn = dn.replace("/", "_")
                    return os.path.join(
                        self._cfg["download_cache"]["dir"],
                        "files",
                        parts.scheme,
                        parts.netloc,
                        dn,
                        fn,
                    )
        return None

    def get_download(self, logger: logging.Logger, uri: str) -> Optional[str]:
        """
        Return filename of the cached download of uri, if available. If so then opens a shared
        flock on the local file, which will remain for the life of the CallCache object.
        """
        p = self.download_path(uri)
        if not (self._cfg["download_cache"].get_bool("get") and p and os.path.isfile(p)):
            logger.debug(_("no download cache hit", uri=uri, cache_path=p))
            return None
        try:
            self._flock([p])
            logger.info(_("found in download cache", uri=uri, cache_path=p))
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
        Move the downloaded file to the cache location & return the new path; or if the uri isn't
        cacheable, return the given path.
        """
        ans = filename
        if self._cfg["download_cache"].get_bool("put"):
            p = self.download_path(uri)
            if p:
                os.makedirs(os.path.dirname(p), exist_ok=True)
                os.rename(filename, p)
                logger.info(_("stored in download cache", uri=uri, cache_path=p))
                ans = p
        self._flock([ans])
        return ans


@contextmanager
def _open_and_flock(
    filename: str, mode: str = "rb", exclusive: bool = False, wait: bool = False
) -> Iterator[IO[Any]]:
    """
    context manager yields an open BinaryIO/TextIO with a flock on the file, also updating atime
    """
    while True:
        with open(filename, mode) as openfile:
            op = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            if not wait:
                op |= fcntl.LOCK_NB
            fcntl.flock(openfile, op)
            # verify the hardlink didn't change in between our open & flock syscalls
            filename_st = os.stat(filename)
            file_st = os.stat(openfile.fileno())
            if filename_st.st_dev == file_st.st_dev and filename_st.st_ino == file_st.st_ino:
                # touch -a
                os.utime(openfile.fileno(), ns=(int(time.time() * 1e9), file_st.st_mtime_ns))
                yield openfile
                return
        # the flock should expire automatically when we close openfile
