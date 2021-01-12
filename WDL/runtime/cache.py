"""
Caching outputs of task/workflow calls (incl. file URI downloader tasks) based on source code and
inputs. When cached outputs are found for reuse, opens advisory locks (flocks) on any local files
referenced therein, and updates their access timestamps (atime).
"""
import json
import os
import logging
from pathlib import Path
from typing import Dict, Optional, Union
from contextlib import AbstractContextManager
from urllib.parse import urlparse, urlunparse
from fnmatch import fnmatchcase
from threading import Lock

from . import config

from .. import Env, Value, Type
from .._util import (
    StructuredLogMessage as _,
    FlockHolder,
    write_atomic,
    rmtree_atomic,
)


class CallCache(AbstractContextManager):
    _cfg: config.Loader
    _flocker: FlockHolder
    _logger: logging.Logger

    # URIs->files cached only for the lifetime of this CallCache instance. These are downloaded in
    # the course of the current workflow run, but not eligible for persistent caching in future
    # runs; we just want to remember them for potential reuse later in the current run.
    _workflow_downloads: Dict[str, str]
    _workflow_directory_downloads: Dict[str, str]
    _lock: Lock

    def __init__(self, cfg: config.Loader, logger: logging.Logger):
        self._cfg = cfg
        self._logger = logger.getChild("CallCache")
        self._flocker = FlockHolder(self._logger)
        self._workflow_downloads = {}
        self._workflow_directory_downloads = {}
        self._lock = Lock()
        self.call_cache_dir = cfg["call_cache"]["dir"]

        try:
            os.mkdir(self.call_cache_dir)
        except Exception:
            pass

    def __enter__(self) -> "CallCache":
        self._flocker.__enter__()
        return self

    def __exit__(self, *args) -> None:
        self._flocker.__exit__(*args)

    def get(
        self, key: str, inputs: Env.Bindings[Value.Base], output_types: Env.Bindings[Type.Base]
    ) -> Optional[Env.Bindings[Value.Base]]:
        """
        Resolve cache key to call outputs, if available, or None. When matching outputs are found, check to ensure the
        modification time on any output or input files is older than the modification time for the cache file.
        Opens shared flocks on all files referenced therein, which will remain for the life of the CallCache object.
        """
        from .. import values_from_json

        file_path = os.path.join(self.call_cache_dir, f"{key}.json")

        if not self._cfg["call_cache"].get_bool("get"):
            return None

        cache = None
        try:
            with open(file_path, "rb") as file_reader:
                cache = values_from_json(json.loads(file_reader.read()), output_types)  # pyre-fixme
        except FileNotFoundError:
            self._logger.info(_("call cache miss", cache_file=file_path))
        except Exception as exn:
            self._logger.warning(
                _("call cache entry present, but unreadable", cache_file=file_path, error=str(exn))
            )
        if cache:
            self._logger.notice(_("call cache hit", cache_file=file_path))  # pyre-fixme
            # check that no files/directories referenced by the inputs & cached outputs are newer
            # than the cache file itself
            if _check_files_coherence(
                self._cfg, self._logger, file_path, inputs
            ) and _check_files_coherence(self._cfg, self._logger, file_path, cache):
                return cache
            else:
                # otherwise, clean it up
                try:
                    os.remove(file_path)
                except Exception as exn:
                    self._logger.warning(
                        _(
                            "unable to delete invalidated cache entry",
                            cache_file=file_path,
                            error=str(exn),
                        )
                    )
        return None

    def put(self, key: str, outputs: Env.Bindings[Value.Base]) -> None:
        """
        Store call outputs for future reuse
        """
        from .. import values_to_json

        if self._cfg["call_cache"].get_bool("put"):
            filename = os.path.join(self.call_cache_dir, key + ".json")
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            write_atomic(json.dumps(values_to_json(outputs), indent=2), filename)  # pyre-ignore
            self._logger.info(_("call cache insert", cache_file=filename))

    # specialized caching logic for file downloads (not sensitive to the downloader task details,
    # and looked up folder structure based on URI instead of opaque digests)

    def download_path(self, uri: str, directory: bool = False) -> Optional[str]:
        """
        Based on the input download uri, compute the local file path at which the cached copy
        should exist (or None if the uri is not cacheable)
        """
        # check if URI is properly formatted & normalize
        parts = urlparse(uri.rstrip("/"))
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
                    # formulate local subdirectory of the cache directory in which to put the
                    # cached item, manipulating the URI path to ensure consistent local nesting
                    # depth (that's assumed by clean_download_cache.sh when it's looking for items
                    # to clean up)
                    dn = dn.strip("/")
                    if dn:
                        dn = dn.replace("_", "__")
                        dn = dn.replace("/", "_")
                    dn = "_" + dn
                    return os.path.join(
                        self._cfg["download_cache"]["dir"],
                        ("dirs" if directory else "files"),
                        parts.scheme,
                        parts.netloc,
                        dn,
                        fn,
                    )
        return None

    def get_download(
        self, uri: str, directory: bool = False, logger: Optional[logging.Logger] = None
    ) -> Optional[str]:
        """
        Return filename of the cached download of uri, if available. If so then opens a shared
        flock on the local file/directory, which will remain for the life of the CallCache object.
        """
        if directory:
            uri = uri.rstrip("/")
        with self._lock:
            if directory and uri in self._workflow_directory_downloads:
                return self._workflow_directory_downloads[uri]
            elif not directory and uri in self._workflow_downloads:
                return self._workflow_downloads[uri]
        logger = logger.getChild("CallCache") if logger else self._logger
        p = self.download_path(uri, directory=directory)
        if not (
            self._cfg["download_cache"].get_bool("get")
            and p
            and ((directory and os.path.isdir(p)) or (not directory and os.path.isfile(p)))
        ):
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
        self,
        uri: str,
        filename: str,
        directory: bool = False,
        logger: Optional[logging.Logger] = None,
    ) -> str:
        """
        Move the downloaded file to the cache location & return the new path; or if the uri isn't
        cacheable, memoize the association and return the given path.
        """
        if directory:
            uri = uri.rstrip("/")
        logger = logger.getChild("CallCache") if logger else self._logger
        ans = filename
        p = self.download_cacheable(uri, directory=directory)
        if p:
            # if a file at the cache location has appeared whilst we were downloading, replace it
            # iff we can exclusive-flock it
            with FlockHolder(logger) as replace_flock:
                try:
                    replace_flock.flock(p, mode=os.O_RDONLY, exclusive=True)
                except FileNotFoundError:
                    pass
                except OSError:
                    logger.warning(
                        _(
                            "existing cache entry in use; leaving downloaded in-place",
                            uri=uri,
                            downloaded=filename,
                            cache_path=p,
                        )
                    )
                    p = None
                if p:
                    if directory and os.path.isdir(p):
                        rmtree_atomic(p)
                    os.renames(filename, p)
                    self.flock(p)
                    # the renames() op should be atomic, because the download operation should have
                    # been run under the cache directory (download.py:run_cached)
                    logger.info(_("stored in download cache", uri=uri, cache_path=p))
                    ans = p
        if not p:
            self.memo_download(uri, filename, directory=directory)
        return ans

    def download_cacheable(self, uri: str, directory: bool = False) -> Optional[str]:
        if not self._cfg["download_cache"].get_bool("put"):
            return None
        return self.download_path(uri, directory=directory)

    def memo_download(
        self,
        uri: str,
        filename: str,
        directory: bool = False,
    ) -> None:
        """
        Memoize (for the lifetime of self) that filename is a local copy of uri; flock it as well.
        """
        with self._lock:
            memo = self._workflow_directory_downloads if directory else self._workflow_downloads
            if uri not in memo:
                memo[uri] = filename
                self.flock(filename)

    def flock(self, filename: str, exclusive: bool = False) -> None:
        self._flocker.flock(filename, update_atime=True, exclusive=exclusive)


def _check_files_coherence(
    cfg: config.Loader, logger: logging.Logger, cache_file: str, values: Env.Bindings[Value.Base]
) -> bool:
    """
    Verify that none of the files/directories referenced by values are newer than cache_file itself
    (based on posix mtimes).
    """
    from .download import able as downloadable

    def mtime(path: str) -> float:
        # max mtime of hardlink & symlink pointing to it (if applicable)
        return max(
            os.stat(path, follow_symlinks=False).st_mtime_ns,
            os.stat(path, follow_symlinks=True).st_mtime_ns,
        )

    def raiser(exc):
        raise exc

    cache_file_mtime = mtime(cache_file)

    def check_one(v: Union[Value.File, Value.Directory]):
        assert isinstance(v, (Value.File, Value.Directory))
        if not downloadable(cfg, v.value):
            try:
                if mtime(v.value) > cache_file_mtime:
                    raise StopIteration
                if isinstance(v, Value.Directory):
                    # check everything in directory
                    for root, subdirs, subfiles in os.walk(
                        v.value, onerror=raiser, followlinks=False
                    ):
                        for subdir in subdirs:
                            if mtime(os.path.join(root, subdir)) > cache_file_mtime:
                                raise StopIteration
                        for fn in subfiles:
                            if mtime(os.path.join(root, fn)) > cache_file_mtime:
                                raise StopIteration
            except (FileNotFoundError, NotADirectoryError, StopIteration):
                logger.warning(
                    _(
                        "cache entry invalid due to deleted or modified file/directory",
                        cache_file=cache_file,
                        changed=v.value,
                    )
                )
                raise StopIteration

    try:
        Value.rewrite_env_paths(values, check_one)
        return True
    except StopIteration:
        return False


_backends_lock = Lock()
_backends = {}


def new(cfg: config.Loader, logger: logging.Logger) -> CallCache:
    """
    Instantiate a CallCache, either the built-in implementation or a plugin-defined subclass per
    the configuration.
    """
    global _backends
    with _backends_lock:
        if not _backends:
            for plugin_name, plugin_cls in config.load_plugins(cfg, "cache_backend"):
                _backends[plugin_name] = plugin_cls
        impl_cls = _backends[cfg["call_cache"]["backend"]]
    ans = impl_cls(cfg, logger)
    assert isinstance(ans, CallCache)
    return ans
