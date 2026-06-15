"""
Caching outputs of task/workflow calls (incl. file URI downloader tasks) based on source code and
inputs. When cached outputs are found for reuse, opens advisory locks (flocks) on any local files
referenced therein, and updates their access timestamps (atime).
"""

import json
import os
import logging
import hashlib
import base64
from pathlib import Path
from typing import Dict, Optional, Union, Any, Iterable, Iterator, Set
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

CALL_CACHE_VERSION = 2


class CallCacheAddPaths:
    """
    Local filesystem paths recorded in a cache entry beyond its explicit inputs/outputs.

    Call-cache keys already include declared inputs, and cache hits validate input/output File and
    Directory mtimes. This manifest covers additional local paths that can affect a call result
    without appearing as explicit input or output values.

    The general purpose is future-proofing: if runtime evaluation observes any host path that
    should participate in cache coherence, it can be recorded here without changing the key shape.
    The current concrete use is WDL 1.2 source-relative paths whose filenames may be assembled
    dynamically while evaluating task/workflow declarations, runtime/requirements expressions,
    command placeholders, stdlib/operator path arguments, and call-input bindings. Present paths go
    in ``add_paths``; optional source-relative paths that resolve to ``None`` go in
    ``absent_paths`` so creating them later invalidates the entry.

    Directory paths use miniwdl's existing trailing-slash convention in both sets.
    """

    add_paths: Set[str]
    absent_paths: Set[str]

    def __init__(
        self,
        add_paths: Optional[Iterable[str]] = None,
        absent_paths: Optional[Iterable[str]] = None,
    ) -> None:
        self.add_paths = set(add_paths or [])
        self.absent_paths = set(absent_paths or [])

    def copy(self) -> "CallCacheAddPaths":
        return CallCacheAddPaths(self.add_paths, self.absent_paths)

    def update(self, other: "CallCacheAddPaths") -> None:
        self.add_paths.update(other.add_paths)
        self.absent_paths.update(other.absent_paths)

    def add(self, path: str, absent: bool = False) -> None:
        assert os.path.isabs(path.rstrip("/") or "/"), (
            "CallCacheAddPath given unresolved relative path"
        )
        (self.absent_paths if absent else self.add_paths).add(path)


def digest_inputs(inputs: Env.Bindings[Value.Base]) -> str:
    """
    Digest the call-cache input envelope. Versioning this digest moves new cache formats to new
    filenames, so older miniwdl versions won't encounter entries they can't validate correctly.
    """
    from .. import values_to_json

    key_json = json.dumps(
        {
            "miniwdlCallCacheVersion": CALL_CACHE_VERSION,
            "inputs": values_to_json(inputs),
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    sha256 = hashlib.sha256(key_json.encode("utf-8")).digest()
    return base64.b32encode(sha256[:20]).decode().lower()


def call_cache_key(name: str, digest: str, inputs: Env.Bindings[Value.Base]) -> str:
    return f"{name}/{digest}/{digest_inputs(inputs)}"


class CallCache(AbstractContextManager):
    _cfg: config.Loader
    _download_cache_dir: str
    _call_cache_dir: str
    _flocker: FlockHolder
    _logger: logging.Logger

    # URIs->files cached only for the lifetime of this CallCache instance. These are downloaded in
    # the course of the current workflow run, but not eligible for persistent caching in future
    # runs; we just want to remember them for potential reuse later in the current run.
    _workflow_downloads: Dict[str, str]
    _workflow_directory_downloads: Dict[str, str]
    _entry_add_paths: Dict[str, CallCacheAddPaths]
    _lock: Lock

    def __init__(self, cfg: config.Loader, logger: logging.Logger):
        self._cfg = cfg
        self._logger = logger.getChild("CallCache")
        self._flocker = FlockHolder(self._logger)
        self._workflow_downloads = {}
        self._workflow_directory_downloads = {}
        self._entry_add_paths = {}
        self._lock = Lock()
        self._download_cache_dir = cfg["download_cache"]["dir"]
        self._download_cache_dir = (
            self._download_cache_dir
            if os.path.isabs(self._download_cache_dir)
            else os.path.join(cfg["file_io"]["root"], self._download_cache_dir)
        )
        self._call_cache_dir = cfg["call_cache"]["dir"]
        self._call_cache_dir = (
            self._call_cache_dir
            if os.path.isabs(self._call_cache_dir)
            else os.path.join(cfg["file_io"]["root"], self._call_cache_dir)
        )
        if cfg["download_cache"].get_bool("put"):
            os.makedirs(self._download_cache_dir, exist_ok=True)
            with open(os.path.join(self._download_cache_dir, "_miniwdl_flock"), "w"):
                pass
        if cfg["call_cache"].get_bool("put"):
            os.makedirs(self._call_cache_dir, exist_ok=True)

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

        file_path = os.path.join(self._call_cache_dir, f"{key}.json")

        if not self._cfg["call_cache"].get_bool("get"):
            return None

        cache = None
        run_dir = None
        cache_paths = CallCacheAddPaths()
        try:
            with open(file_path, "rb") as file_reader:
                envelope = json.loads(file_reader.read())
                # should never fail because the version is mixed into the cache key:
                assert envelope.get("miniwdlCallCacheVersion") == CALL_CACHE_VERSION
                run_dir = envelope.get("dir", None)
                cache_paths = CallCacheAddPaths(
                    envelope.get("additionalPaths", []), envelope.get("absentPaths", [])
                )
                cache = values_from_json(envelope["outputs"], output_types)
        except FileNotFoundError:
            self._logger.info(_("call cache miss", cache_file=file_path))
        except Exception as exn:
            self._logger.warning(
                _("call cache entry present, but unreadable", cache_file=file_path, error=str(exn))
            )
        if cache is not None:
            self._logger.notice(
                _(
                    "call cache hit",
                    run_dir=(run_dir if run_dir else "?"),
                    cache_file=file_path,
                )
            )
            # check that no files/directories referenced by the inputs & cached outputs are newer
            # than the cache file itself
            if (
                _check_files_coherence(self._cfg, self._logger, file_path, inputs)
                and _check_files_coherence(self._cfg, self._logger, file_path, cache)
                and _check_add_paths_coherence(self._logger, file_path, cache_paths)
            ):
                self._entry_add_paths[key] = cache_paths.copy()
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

    def put(
        self,
        key: str,
        outputs: Env.Bindings[Value.Base],
        run_dir: Optional[str] = None,
        *,
        inputs: Env.Bindings[Value.Base],
        add_paths: CallCacheAddPaths,
    ) -> None:
        """
        Store call outputs for future reuse. V2 callers must provide the exact inputs used for the
        key digest and the additional-path manifest, even when the manifest is empty.
        """
        from .. import values_to_json

        cache_paths = add_paths.copy()
        if self._cfg["call_cache"].get_bool("put"):
            envelope = {
                "miniwdlCallCacheVersion": CALL_CACHE_VERSION,
                "inputs": values_to_json(inputs),
                "outputs": values_to_json(outputs),
                "additionalPaths": sorted(cache_paths.add_paths),
                "absentPaths": sorted(cache_paths.absent_paths),
            }
            if run_dir:
                envelope["dir"] = run_dir
            filename = os.path.join(self._call_cache_dir, key + ".json")
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            write_atomic(json.dumps(envelope, indent=2), filename)
            self._logger.info(_("call cache insert", cache_file=filename))
        self._entry_add_paths[key] = cache_paths.copy()

    def get_add_paths(self, key: str) -> CallCacheAddPaths:
        """
        Retrieve additional paths remembered for a v2 cache hit/insert during this process.
        """
        return self._entry_add_paths.get(key, CallCacheAddPaths()).copy()

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
                        self._download_cache_dir,
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
                os.path.join(self._download_cache_dir, "_miniwdl_flock"),
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

    cache_file_mtime = _effective_mtime(cache_file)

    def check_one(v: Union[Value.File, Value.Directory]):
        assert isinstance(v, (Value.File, Value.Directory))
        if not downloadable(cfg, v.value):
            try:
                if _path_mtime_after(v.value, cache_file_mtime, isinstance(v, Value.Directory)):
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


def _effective_mtime(path: str) -> float:
    """
    Get the effective mtime used for cache freshness checks, considering both a symlink and its
    referent when applicable.
    """
    return max(
        os.stat(path, follow_symlinks=False).st_mtime_ns,
        os.stat(path, follow_symlinks=True).st_mtime_ns,
    )


def _iter_directory_contents(path: str) -> Iterator[str]:
    def on_walk_error(exc: OSError) -> None:
        raise exc

    for root, subdirs, subfiles in os.walk(path, onerror=on_walk_error, followlinks=False):
        for subdir in subdirs:
            yield os.path.join(root, subdir)
        for fn in subfiles:
            yield os.path.join(root, fn)


def _path_mtime_after(path: str, cutoff_mtime: float, directory: bool) -> bool:
    if _effective_mtime(path) > cutoff_mtime:
        return True
    return directory and any(
        _effective_mtime(child) > cutoff_mtime for child in _iter_directory_contents(path)
    )


def _check_add_paths_coherence(
    logger: logging.Logger, cache_file: str, cache_paths: CallCacheAddPaths
) -> bool:
    """
    Verify additional present/absent local paths recorded in a v2 cache envelope.
    """

    def check_present(path: str) -> None:
        isdir = path.endswith("/")
        path_strip = path.rstrip("/") or "/"

        try:
            if isdir:
                if not os.path.isdir(path_strip):
                    raise StopIteration
            elif not os.path.isfile(path_strip):
                raise StopIteration
            if _path_mtime_after(path_strip, cache_file_mtime, isdir):
                raise StopIteration
        except (FileNotFoundError, NotADirectoryError, StopIteration):
            logger.warning(
                _(
                    "cache entry invalid due to deleted or modified additional path",
                    cache_file=cache_file,
                    changed=path,
                )
            )
            raise StopIteration

    cache_file_mtime = _effective_mtime(cache_file)
    try:
        for path in cache_paths.add_paths:
            check_present(path)
        for path in cache_paths.absent_paths:
            if os.path.exists(path.rstrip("/") or "/"):
                logger.warning(
                    _(
                        "cache entry invalid due to created additional path",
                        cache_file=cache_file,
                        changed=path,
                    )
                )
                raise StopIteration
        return True
    except StopIteration:
        return False


_backends_lock = Lock()
_backends: Dict[str, Any] = {}


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
