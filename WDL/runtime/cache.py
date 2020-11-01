"""
Caching outputs of task/workflow calls (incl. file URI downloader tasks) based on source code and
inputs. When cached outputs are found for reuse, opens advisory locks (flocks) on any local files
referenced therein, and updates their access timestamps (atime).
"""
import abc
import hashlib
import json
import itertools
import os
import logging
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, List, Iterable, Union
from contextlib import AbstractContextManager
from urllib.parse import urlparse, urlunparse
from fnmatch import fnmatchcase
from threading import Lock

from . import config
from .error import CacheOutputFileAgeError

from .. import Env, Value, Type, Document, Tree, Error
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

    def get_digest_for_inputs(self, inputs: Env.Bindings[Value.Base]):
        """
        Return sha256 for json of sorted inputs
        """
        from .. import values_to_json

        json_inputs = json.dumps(values_to_json(inputs), sort_keys=True).encode("utf-8")
        return hashlib.sha256(json_inputs).hexdigest()

    def get_digest_for_task(self, task):
        doc = getattr(task, "parent", None)
        assert isinstance(doc, Document)
        task_string = _describe_task(doc, task)
        return hashlib.sha256(task_string.encode("utf-8")).hexdigest()

    def get(
        self, key: str, output_types: Env.Bindings[Type.Base], inputs: Env.Bindings[Value.Base]
    ) -> Optional[Env.Bindings[Value.Base]]:
        """
        Resolve cache key to call outputs, if available, or None. When matching outputs are found, check to ensure the
        modification time on any output or input files is older than the modification time for the cache file.
        Opens shared flocks on all files referenced therein, which will remain for the life of the CallCache object.
        """
        from .. import values_from_json

        file_path = os.path.join(self.call_cache_dir, f"{key}.json")
        file_coherence_checker = FileCoherence(self._cfg, self._logger)

        if not self._cfg["call_cache"].get_bool("get"):
            return None

        cache = None
        try:
            with open(file_path, "rb") as file_reader:
                cache = values_from_json(json.loads(file_reader.read()), output_types)  # pyre-fixme
        except FileNotFoundError:
            self._logger.info(_("call cache miss", cache_path=file_path))
        except Exception as exn:
            self._logger.warning(
                _("call cache entry present, but unreadable", cache_path=file_path, error=str(exn))
            )
        if cache:
            self._logger.notice(_("call cache hit", cache_path=file_path))  # pyre-fixme
            file_list = set()
            dir_list = set()
            # check output and input file timestamps

            def get_files(v: Union[Value.File, Value.Directory]):
                if isinstance(v, Value.File):
                    file_list.add(v.value)
                else:
                    assert isinstance(v, Value.Directory)
                    dir_list.add(v.value)

            Value.rewrite_env_paths(cache, get_files)
            Value.rewrite_env_paths(inputs, get_files)
            if file_coherence_checker.check_files(file_path, file_list, dir_list):
                return cache
        return None

    def put(self, task_key: str, input_digest: str, outputs: Env.Bindings[Value.Base]) -> None:
        """
        Store call outputs for future reuse
        """
        from .. import values_to_json

        if self._cfg["call_cache"].get_bool("put"):

            filepath = os.path.join(self.call_cache_dir, task_key)
            filename = os.path.join(self.call_cache_dir, f"{task_key}/{input_digest}.json")

            Path(filepath).mkdir(parents=True, exist_ok=True)

            write_atomic(
                json.dumps(values_to_json(outputs, namespace=""), indent=2), filename  # pyre-ignore
            )
            self._logger.info(_("call cache insert", cache_path=filename))

    # specialized caching logic for file downloads (not sensitive to the downloader task details,
    # and looked up in URI-derived folder structure instead of sqlite db)

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
        cacheable, return the given path.
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
                    # the renames() op should be atomic, because the download operation should have
                    # been run under the cache directory (download.py:run_cached)
                    logger.info(_("stored in download cache", uri=uri, cache_path=p))
                    ans = p
        if not p:
            with self._lock:
                (self._workflow_directory_downloads if directory else self._workflow_downloads)[
                    uri
                ] = ans
        self.flock(ans)
        return ans

    def download_cacheable(self, uri: str, directory: bool = False) -> Optional[str]:
        if not self._cfg["download_cache"].get_bool("put"):
            return None
        return self.download_path(uri, directory=directory)

    def flock(self, filename: str, exclusive: bool = False) -> None:
        self._flocker.flock(filename, update_atime=True, exclusive=exclusive)


def _describe_task(doc, task: Tree.Task) -> str:
    """
    Generate a string describing the content of a WDL task. Right now this is just the task
    definition excerpted from the WDL document, with some extra bits to cover any struct types
    used.
    """
    output_lines = []

    # WDL version declaration, if any
    if doc.wdl_version:
        output_lines.append("version " + doc.wdl_version)

    # Insert comments describing struct types used in the task.
    # Originally, we wanted to excerpt/generate the full struct type definitions and produce valid
    # standalone WDL. But, there were complications: because a struct type can be imported from
    # another document and aliased to a different name while doing so, it's possible that the task
    # document refers to the struct by a different name than its original definition. Moreover, the
    # struct might have members that are other structs, which could also be aliased in the current
    # document. So generating valid WDL would involve tricky rewriting of the original struct
    # definitions using one consistent set of names.
    # To avoid dealing with this, instead we just generate comments describing the members of each
    # struct type as named in the task's document. This description (type_id) applies recursively
    # for any members that are themselves structs, making it independent of all struct type names.
    #   https://miniwdl.readthedocs.io/en/latest/WDL.html#WDL.Tree.StructTypeDef.type_id
    structs = _describe_struct_types(task)
    for struct_name in sorted(structs.keys()):
        output_lines.append(f"# {struct_name} :: {structs[struct_name]}")

    # excerpt task{} from document
    # Possible future improvements:
    # excise the meta & parameter_meta sections
    # normalize order of declarations
    # normalize whitespace within lines (not leading/trailing)
    output_lines += _excerpt(doc, task.pos, [task.command.pos])

    return "\n".join(output_lines).strip()


def _describe_struct_types(task: Tree.Task) -> Dict[str, str]:
    """
    Scan all declarations in the task for uses of struct types; produce a mapping from struct name
    to its type_id (a string describing the struct's members, independent of struct names).
    """
    structs = {}
    items: List[Any] = list(task.children)
    while items:
        item = items.pop()
        if isinstance(item, Tree.Decl):
            items.append(item.type)
        elif isinstance(item, Type.StructInstance):
            structs[item.type_name] = item.type_id
        elif isinstance(item, Type.Base):
            # descent into compound types so we'll cover e.g. Array[MyStructType]
            for par_ty in item.parameters:
                items.append(par_ty)
    return structs


def _excerpt(
    doc: Tree.Document, pos: Error.SourcePosition, literals: List[Error.SourcePosition]
) -> List[str]:
    """
    Excerpt the document's source lines indicated by pos : WDL.SourcePosition. Delete comments,
    blank lines, and leading/trailing whitespace from each line -- except those indicated by
    literals.
    """

    def clean(line: int, column: int = 1, end_column: Optional[int] = None) -> List[str]:
        literal = next(
            (True for lit in literals if line >= lit.line and line <= lit.end_line), False
        )
        comment = doc.source_comments[line - 1]
        if comment and not literal:
            assert comment.pos.line == line
            if end_column is None:
                end_column = comment.pos.column - 1
            else:
                end_column = min(end_column, comment.pos.column - 1)
        txt = doc.source_lines[line - 1][(column - 1) : end_column]
        if literal:
            return [txt]
        txt = txt.strip()
        return [txt] if txt else []

    if pos.end_line == pos.line:
        return clean(pos.line, pos.column, pos.end_column)
    return list(
        itertools.chain(
            clean(pos.line, pos.column),
            *(clean(line_nr) for line_nr in range(pos.line + 1, pos.end_line)),
            clean(pos.end_line, 1, pos.end_column),
        )
    )


class FileCoherence(abc.ABC):
    """
    Class to check for file coherence when utilizing an output caching system (based on last modification time
    for cache and referenced files) for files stored locally.
    """

    _cfg: config.Loader
    _logger: logging.Logger

    def __init__(self, cfg: config.Loader, logger: logging.Logger):
        self._cfg = cfg
        self._logger = logger
        self.cache_file_modification_time = 0.0

        # working around circular import
        from .download import able as downloadable

        self._downloadable = downloadable

    def check_files(self, cache_file_path: str, files: Iterable[str], dirs: Iterable[str]) -> bool:
        if self.cache_file_modification_time == 0.0:
            self.cache_file_modification_time = self.get_last_modified_time(cache_file_path)

        def raiser(exc):
            raise exc

        for directory, path in itertools.chain(
            ((False, f) for f in files), ((True, d) for d in dirs)
        ):
            try:
                if not self._downloadable(self._cfg, path):
                    self.check_cache_younger_than_file(path)
                    if directory:
                        # check everything in directory
                        for root, subdirs, subfiles in os.walk(
                            path, onerror=raiser, followlinks=False
                        ):
                            for subdir in subdirs:
                                self.check_cache_younger_than_file(os.path.join(root, subdir))
                            for fn in subfiles:
                                self.check_cache_younger_than_file(os.path.join(root, fn))
            except (FileNotFoundError, NotADirectoryError, CacheOutputFileAgeError):
                self._logger.warning(
                    _(
                        "cache entry invalid due to deleted or modified file/directory",
                        cache_path=path,
                        file_changed=path,
                    )
                )
                try:
                    os.remove(cache_file_path)
                except Exception as exn:
                    self._logger.warning(
                        _(
                            "unable to delete invalidated cache entry",
                            cache_path=path,
                            error=str(exn),
                        )
                    )
                return False
        return True

    def get_last_modified_time(self, file_path: str) -> float:
        # max mtime of hardlink & symlink pointing to it (if applicable)
        return max(
            os.stat(file_path, follow_symlinks=False).st_mtime_ns,
            os.stat(file_path, follow_symlinks=True).st_mtime_ns,
        )

    def check_cache_younger_than_file(self, output_file_path: str) -> bool:
        output_file_modification_time = self.get_last_modified_time(output_file_path)
        # self._logger.debug(_("check_cache_younger_than_file", path=output_file_path,
        # mtime=output_file_modification_time/1e9, cache_mtime=self.cache_file_modification_time/1e9))
        if self.cache_file_modification_time >= output_file_modification_time:
            return True
        else:
            raise CacheOutputFileAgeError
