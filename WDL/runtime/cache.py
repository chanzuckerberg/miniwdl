"""
Caching outputs of task/workflow calls (incl. file URI downloader tasks) based on source code and
inputs. When cached outputs are found for reuse, opens advisory locks (flocks) on any local files
referenced therein, and updates their access timestamps (atime).
"""
import json
import os
import shutil
import logging
from pathlib import Path
from typing import Dict, Optional, Union
from contextlib import AbstractContextManager, suppress
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
    bump_atime,
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
        if cfg["download_cache"].get_bool("put"):
            os.makedirs(cfg["download_cache"]["dir"], exist_ok=True)
            with open(os.path.join(self._cfg["download_cache"]["dir"], "_miniwdl_flock"), "w"):
                pass
        if cfg["call_cache"].get_bool("put"):
            os.makedirs(cfg["call_cache"]["dir"], exist_ok=True)

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
        """
        from .. import values_from_json

        file_path = os.path.join(self._cfg["call_cache"]["dir"], f"{key}.json")

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
            filename = os.path.join(self._cfg["call_cache"]["dir"], key + ".json")
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
            if self._cfg.get_bool("download_cache", "flock"):
                self.flock(p, directory=directory)
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
        p = self.download_cacheable(uri, directory=directory)
        if not p:
            self.memo_download(uri, filename, directory=directory)
            return filename
        moved = False
        # transient exclusive flock on whole cache directory (serializes entry add/remove)
        with FlockHolder(logger) as transient:
            self.flock(
                os.path.join(self._cfg["download_cache"]["dir"], "_miniwdl_flock"),
                exclusive=True,
                wait=True,
                holder=transient,
            )
            if not os.path.exists(p):
                # this should be atomic, because the download operation should have been run under
                # the cache directory (download.py:run_cached)
                os.renames(filename, p)
                moved = True
                logger.info(_("stored in download cache", uri=uri, cache_path=p))
            if self._cfg.get_bool("download_cache", "flock"):
                self.flock(p, directory=directory)
        if not moved:
            # Cache entry appeared just in the time since this download was initiated, which should
            # be identical to our just-completed download. Regrettably, discard ours to mitigate
            # cache storage space taken.
            logger.warning(
                _(
                    "discarding completed download colliding with a new cache entry",
                    uri=uri,
                    downloaded=filename,
                    cache_path=p,
                )
            )
            if directory:
                rmtree_atomic(filename)
            else:
                with suppress(FileNotFoundError):
                    os.unlink(filename)
        self.memo_download(uri, p, directory=directory)
        return p

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
        Memoize (for the lifetime of self) that filename is a local copy of uri
        """
        with self._lock:
            memo = self._workflow_directory_downloads if directory else self._workflow_downloads
            if uri not in memo:
                memo[uri] = filename

    def flock(
        self,
        filename: str,
        exclusive: bool = False,
        wait: bool = False,
        directory: bool = False,
        holder: Optional[FlockHolder] = None,
    ) -> None:
        flockname = filename
        if directory:
            # Not all filesystems support directory flock, so we flock an adjacent lockfile. This
            # loses some atomicity properties -- for example, moving/renaming the directory won't
            # take the lockfile along with it -- so it's important to handle this under the
            # exclusive flock of the entire cache taken briefly in put_download.
            flockname = os.path.join(
                os.path.dirname(filename), os.path.basename(filename) + "._miniwdl_flock"
            )
            with open(flockname, "w"):
                pass
        if not holder:
            holder = self._flocker
        holder.flock(flockname, exclusive=exclusive, wait=wait)
        bump_atime(filename)  # filename, NOT flockname


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
