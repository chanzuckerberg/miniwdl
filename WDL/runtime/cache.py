"""
Caching outputs of task/workflow calls (incl. file URI downloader tasks) based on source code and
inputs. When cached outputs are found for reuse, opens advisory locks (flocks) on any local files
referenced therein, and updates their access timestamps (atime).
"""
import hashlib
import json
import os
import logging
import threading
from pathlib import Path
from typing import Iterator, Dict, Any, Optional, Set, List, IO
from contextlib import AbstractContextManager
from urllib.parse import urlparse, urlunparse
from fnmatch import fnmatchcase

from . import config

from .. import Env, Value, Type, Document
from .._util import (
    StructuredLogMessage as _,
    FlockHolder,
    write_atomic,
    excerpt,
    describe_struct_types,
)


class CallCache(AbstractContextManager):
    _cfg: config.Loader
    _flocker: FlockHolder
    _logger: logging.Logger

    def __init__(
        self, cfg: config.Loader, logger: logging.Logger, wdl_doc: Optional[Document] = None
    ):
        self._cfg = cfg
        self._logger = logger.getChild("CallCache")
        self._flocker = FlockHolder(self._logger)
        self.call_cache_dir = cfg["call_cache"]["dir"]
        self.wdl_doc = wdl_doc

        try:
            os.mkdir(self.call_cache_dir)
        except Exception as e:
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

    def get(
        self, key: str, output_types: Env.Bindings[Type.Base],
    ) -> Optional[Env.Bindings[Value.Base]]:
        """
        Resolve cache key to call outputs, if available, or None. When matching outputs are found,
        opens shared flocks on all files referenced therein, which will remain for the life of the
        CallCache object.
        """
        from .. import values_from_json

        file_path = os.path.join(self.call_cache_dir, f"{key}.json")
        if not self._cfg["call_cache"].get_bool("get"):
            return None

        try:
            with open(file_path, "rb") as file_reader:
                contents = file_reader.read()
        except FileNotFoundError:
            self._logger.info(f"Cache lookup unsuccessful for input_digest: {key}")
            return None
        contents = json.loads(contents)
        self._logger.notice(f"Cache found for input_digest: {key}")
        return values_from_json(contents, output_types)

    def put(self, task_key: str, input_digest: str, outputs: Env.Bindings[Value.Base],) -> None:
        """
        Store call outputs for future reuse
        """
        from .. import values_to_json

        if self._cfg["call_cache"].get_bool("put"):

            filepath = os.path.join(self.call_cache_dir, task_key)
            filename = os.path.join(self.call_cache_dir, f"{task_key}/{input_digest}.json")

            Path(filepath).mkdir(parents=True, exist_ok=True)

            write_atomic(
                json.dumps(values_to_json(outputs, namespace=""), indent=2),  # pyre-ignore
                filename,
            )
            self._logger.info(
                f"Cache created for task_digest: {task_key}, input_digest: {input_digest}"
            )

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

    def get_digest_for_task(self, task):
        task_string = self.describe_task(self.wdl_doc, task)
        return hashlib.sha256(task_string.encode("utf-8")).hexdigest()

    def describe_task(self, doc, task):
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
        structs = describe_struct_types(task)
        for struct_name in sorted(structs.keys()):
            output_lines.append(f"# {struct_name} :: {structs[struct_name]}")

        # excerpt task{} from document
        output_lines += excerpt(doc, task.pos)

        # TODO (?): delete non-semantic whitespace, perhaps excise the meta & parameter_meta sections

        return "\n".join(output_lines).strip()
