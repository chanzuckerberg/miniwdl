"""
Caching outputs of task/workflow calls (incl. file URI downloader tasks) based on source code and
inputs. When cached outputs are found for reuse, opens advisory locks (flocks) on any local files
referenced therein, and updates their access timestamps (atime).
"""

import os
import logging
import threading
from typing import Iterator, Dict, Any, Optional, Set, List, IO
from contextlib import AbstractContextManager
from urllib.parse import urlparse, urlunparse
from fnmatch import fnmatchcase
from . import config
from .. import Env, Value, Type
from .._util import StructuredLogMessage as _, FlockHolder


class CallCache(AbstractContextManager):
    _cfg: config.Loader
    _flocker: FlockHolder
    _logger: logging.Logger

    def __init__(self, cfg: config.Loader, logger: logging.Logger):
        self._cfg = cfg
        self._logger = logger.getChild("CallCache")
        self._flocker = FlockHolder(self._logger)

    def __enter__(self) -> "CallCache":
        self._flocker.__enter__()
        return self

    def __exit__(self, *args) -> None:
        self._flocker.__exit__(*args)

    def get(
        self,
        key: str,
        output_types: Env.Bindings[Type.Base],
        logger: Optional[logging.Logger] = None,
    ) -> Optional[Env.Bindings[Value.Base]]:
        """
        Resolve cache key to call outputs, if available, or None. When matching outputs are found,
        opens shared flocks on all files referenced therein, which will remain for the life of the
        CallCache object.
        """
        raise NotImplementedError()

    def put(
        self, key: str, outputs: Env.Bindings[Value.Base], logger: Optional[logging.Logger] = None
    ) -> None:
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
                self._cfg["download_cache"].get_bool("ignore_query")
                or not (parts.params or parts.query or parts.fragment)
            )
        ):
            uri = urlunparse((parts.scheme, parts.netloc, parts.path, "", "", ""))
            # check enable/disable patterns
            enable = self._cfg["download_cache"].get_list("enable_patterns") or ["*"]
            disable = self._cfg["download_cache"].get_list("disable_patterns")
            if next((pat for pat in enable if fnmatchcase(uri, pat)), False) and not next(
                (pat for pat in disable if fnmatchcase(uri, pat)), False
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

    def get_download(self, uri: str, logger: Optional[logging.Logger] = None) -> Optional[str]:
        """
        Return filename of the cached download of uri, if available. If so then opens a shared
        flock on the local file, which will remain for the life of the CallCache object.
        """
        logger = logger.getChild("CallCache") if logger else self._logger
        p = self.download_path(uri)
        if not (self._cfg["download_cache"].get_bool("get") and p and os.path.isfile(p)):
            logger.debug(_("no download cache hit", uri=uri, cache_path=p))
            return None
        try:
            self.flock(p)
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

    def put_download(
        self, uri: str, filename: str, logger: Optional[logging.Logger] = None,
    ) -> str:
        """
        Move the downloaded file to the cache location & return the new path; or if the uri isn't
        cacheable, return the given path.
        """
        logger = logger.getChild("CallCache") if logger else self._logger
        ans = filename
        if self._cfg["download_cache"].get_bool("put"):
            p = self.download_path(uri)
            if p:
                os.makedirs(os.path.dirname(p), exist_ok=True)
                os.rename(filename, p)
                logger.info(_("stored in download cache", uri=uri, cache_path=p))
                ans = p
        self.flock(ans)
        return ans

    def flock(self, filename: str, exclusive: bool = False) -> None:
        self._flocker.flock(filename, update_atime=True, exclusive=exclusive)
