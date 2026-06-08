"""
Runtime StdLib implementations for tasks and workflows.
"""

import glob
import hashlib
import logging
import os
from typing import TYPE_CHECKING, Optional

from .. import Env, Error, StdLib, Type, Value
from .._util import WDLVersion, wdl_version_geq
from .._util import StructuredLogMessage as _
from . import config
from .cache import CallCache
from .download import able as downloadable
from .error import OutputError
from ._io_helpers import _resolve_source_relative_path, _resolve_workflow_path

if TYPE_CHECKING:
    from .task_container import TaskContainer
    from ._workflow_state import StateMachine


class TaskStdLib(StdLib.Base):
    logger: logging.Logger
    container: "TaskContainer"
    inputs_only: bool  # if True then only permit access to input files
    source_dir: str

    def __init__(
        self,
        wdl_version: str,
        logger: logging.Logger,
        container: "TaskContainer",
        inputs_only: bool,
        source_dir: str = "",
        eval_context: Optional[StdLib.EvalContext] = None,
    ) -> None:
        super().__init__(
            wdl_version,
            write_dir=os.path.join(container.host_dir, "write_"),
            eval_context=eval_context,
        )
        self.logger = logger
        self.container = container
        self.inputs_only = inputs_only
        self.source_dir = source_dir

    def _source_relative_host_path(self, filename: str, desc: str) -> str:
        directory = filename.endswith("/")
        value = Value.Directory(filename) if directory else Value.File(filename)
        ans = _resolve_source_relative_path(self.container.cfg, self.source_dir, desc, value)
        if ans is None:
            raise Error.InputError(f"File/Directory path not found in {desc}: {filename}")
        return ans

    def _devirtualize_filename(self, filename: str) -> str:
        """
        Return the host path for task StdLib direct file access.

        Directory paths are denoted by a trailing "/".
        Input/private evaluation may read WDL 1.2 source-relative paths directly from the host
        source directory. Output evaluation keeps existing task-output semantics and resolves paths
        only through the execution directory or already-localized inputs.
        """
        # check allowability of reading this file, & map from in-container to host
        directory = filename.endswith("/")
        ans = self.container.host_path(filename, inputs_only=self.inputs_only)
        if (
            ans is None
            and self.inputs_only
            and wdl_version_geq(self.wdl_version, WDLVersion.V1_2)
            and not os.path.isabs(filename)
            and not downloadable(self.container.cfg, filename, directory=directory)
        ):
            ans = self._source_relative_host_path(filename, "read_*() argument")
        if ans is None:
            raise OutputError("function was passed non-existent file " + filename)
        self.logger.debug(_("read_", container=filename, host=ans))
        return ans

    def _resolve_source_relative_path(self, filename: str) -> str:
        """
        Resolve a WDL 1.2 source-relative File/Directory StdLib/operator value for a task.

        Directory paths are denoted by a trailing "/".
        This is used during input/private evaluation, where source-relative paths are mounted into
        the task container and returned as in-container paths. ``container`` is intentionally
        mutated when a new source-relative path must be mounted.
        """
        directory = filename.endswith("/")
        if (
            not self.inputs_only
            or not wdl_version_geq(self.wdl_version, WDLVersion.V1_2)
            or os.path.isabs(filename)
            or downloadable(self.container.cfg, filename, directory=directory)
        ):
            return filename
        source_path = self._source_relative_host_path(filename, "File/Directory StdLib argument")
        source_path_key = source_path + ("/" if directory else "")
        self.container.add_paths([source_path_key])
        return self.container.input_path_map[source_path_key].rstrip("/")

    def _virtualize_filename(self, filename: str) -> str:
        # register new file with container input_path_map
        self.container.add_paths([filename])
        self.logger.debug(
            _("write_", host=filename, container=self.container.input_path_map[filename])
        )
        self.logger.info(_("wrote", file=self.container.input_path_map[filename]))
        return self.container.input_path_map[filename]

    def _join_paths_default_directory(self) -> str:
        return os.path.join(self.container.container_dir, "work")


class TaskInputStdLib(TaskStdLib):
    # StdLib for evaluation of task inputs and command
    def __init__(
        self,
        wdl_version: str,
        logger: logging.Logger,
        container: "TaskContainer",
        source_dir: str = "",
        eval_context: Optional[StdLib.EvalContext] = None,
    ) -> None:
        super().__init__(
            wdl_version,
            logger,
            container,
            True,
            source_dir=source_dir,
            eval_context=eval_context,
        )


class TaskOutputStdLib(TaskStdLib):
    # StdLib for evaluation of task outputs
    def __init__(
        self,
        wdl_version: str,
        logger: logging.Logger,
        container: "TaskContainer",
        eval_context: Optional[StdLib.EvalContext] = None,
    ) -> None:
        super().__init__(wdl_version, logger, container, False, eval_context=eval_context)

        setattr(
            self,
            "stdout",
            StdLib.StaticFunction(
                "stdout",
                [],
                Type.File(),
                lambda: Value.File(os.path.join(self.container.container_dir, "stdout.txt")),
            ),
        )
        setattr(
            self,
            "stderr",
            StdLib.StaticFunction(
                "stderr",
                [],
                Type.File(),
                lambda: Value.File(os.path.join(self.container.container_dir, "stderr.txt")),
            ),
        )

        def _glob(pattern: Value.String, lib: TaskOutputStdLib = self) -> Value.Array:
            pat = pattern.coerce(Type.String()).value
            if not pat:
                raise OutputError("empty glob() pattern")
            assert isinstance(pat, str)
            if pat[0] == "/":
                raise OutputError("glob() pattern must be relative to task working directory")
            if pat.startswith("..") or "/.." in pat:
                raise OutputError("glob() pattern must not use .. uplevels")
            if pat.startswith("./"):
                pat = pat[2:]
            # glob the host directory
            pat = os.path.join(lib.container.host_work_dir(), pat)
            host_files = sorted(fn for fn in glob.glob(pat) if os.path.isfile(fn))
            # convert the host filenames to in-container filenames
            container_files = []
            for hf in host_files:
                dstrip = lib.container.host_work_dir()
                dstrip += "" if dstrip.endswith("/") else "/"
                assert hf.startswith(dstrip)
                container_files.append(
                    os.path.join(lib.container.container_dir, "work", hf[len(dstrip) :])
                )
            return Value.Array(Type.File(), [Value.File(fn) for fn in container_files])

        setattr(
            self,
            "glob",
            StdLib.StaticFunction("glob", [Type.String()], Type.Array(Type.File()), _glob),
        )


class WorkflowStdLib(StdLib.Base):
    "checks against & updates the file/directory allowlist for the read_* and write_* functions"

    cfg: config.Loader
    state: "StateMachine"
    cache: CallCache

    def __init__(
        self,
        cfg: config.Loader,
        wdl_version: str,
        state: "StateMachine",
        cache: CallCache,
        eval_context: Optional[StdLib.EvalContext] = None,
    ) -> None:
        super().__init__(
            wdl_version,
            write_dir=os.path.join(state.run_dir, "write_"),
            eval_context=eval_context,
        )
        self.cfg = cfg
        self.state = state
        self.cache = cache

    def _source_relative_host_path(self, filename: str, desc: str) -> str:
        directory = filename.endswith("/")
        value = Value.Directory(filename) if directory else Value.File(filename)
        ans = _resolve_source_relative_path(self.cfg, self.state.workflow.source_dir, desc, value)
        if ans is None:
            raise Error.InputError(f"File/Directory path not found in {desc}: {filename}")
        return ans

    def _devirtualize_filename(self, filename: str) -> str:
        directory = filename.endswith("/")
        if downloadable(self.cfg, filename, directory=directory):
            cached = self.cache.get_download(filename)
            if cached:
                return cached
        if (
            wdl_version_geq(self.wdl_version, WDLVersion.V1_2)
            and not os.path.isabs(filename)
            and not downloadable(self.cfg, filename, directory=directory)
        ):
            source_path = self._source_relative_host_path(filename, "read_*() argument")
            self.state.fspath_allowlist.add(source_path + ("/" if directory else ""))
            filename = source_path
        ans = _resolve_workflow_path(
            self.cfg,
            self.state.fspath_allowlist,
            "read_*() argument",
            Value.Directory(filename) if directory else Value.File(filename),
        )
        assert ans is not None
        return ans

    def _resolve_source_relative_path(self, filename: str) -> str:
        """
        Resolve a File/Directory StdLib/operator value in a workflow.

        Directory paths are denoted by a trailing "/".
        WDL 1.2 source-relative paths resolve against the workflow source directory, and are
        intentionally added to the workflow allowlist as a side effect. Pre-1.2 relative paths don't
        get source-directory semantics, but they still pass through the workflow path boundary for
        compatibility with legacy ``allow_any_input`` workflows whose File/Directory values have been
        resolved to host paths during declaration binding.
        """
        directory = filename.endswith("/")
        if (
            wdl_version_geq(self.wdl_version, WDLVersion.V1_2)
            and not os.path.isabs(filename)
            and not downloadable(self.cfg, filename, directory=directory)
        ):
            source_path = self._source_relative_host_path(
                filename, "File/Directory StdLib argument"
            )
            self.state.fspath_allowlist.add(source_path + ("/" if directory else ""))
            return source_path
        if not os.path.isabs(filename) and not downloadable(
            self.cfg, filename, directory=directory
        ):
            ans = _resolve_workflow_path(
                self.cfg,
                self.state.fspath_allowlist,
                "File/Directory StdLib argument",
                Value.Directory(filename) if directory else Value.File(filename),
            )
            assert ans is not None
            return ans
        return filename

    def _join_paths_default_directory(self) -> str:
        source = self.state.workflow.pos.abspath
        if not source or source == "(buffer)":
            raise NotImplementedError("join_paths() relative path resolution requires WDL source")
        return os.path.dirname(source)

    def _virtualize_filename(self, filename: str) -> str:
        # After write_* generates a file at the workflow level, query CallCache for an existing
        # identical file; if available, then return that copy. This improves cacheability of
        # downstream tasks that consume the written file, given the unique temp filename for each
        # such file.
        # We fully content-digest the file, but it can't be too large since it was originally
        # serialized from miniwdl memory.
        assert not filename.endswith("/")  # FIXME if/when stdlib functions handle directories
        hasher = hashlib.sha256()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(1048576), b""):
                hasher.update(chunk)
        cache_in: Env.Bindings[Value.Base] = Env.Bindings()
        cache_in = cache_in.bind("file_sha256", Value.String(hasher.hexdigest()))
        cache_key = "write_/" + Value.digest_env(cache_in)
        cache_out_types: Env.Bindings[Type.Base] = Env.Bindings()
        cache_out_types = cache_out_types.bind("file", Type.File())
        cache_out = self.cache.get(cache_key, cache_in, cache_out_types)
        if cache_out:
            filename = cache_out.resolve("file").value
        else:
            # otherwise, put our newly-written file to the cache, and proceed to use it
            self.cache.put(
                cache_key,
                Env.Bindings(Env.Binding("file", Value.File(filename))),
                run_dir=self.state.run_dir,
            )

        # whichever path we took: allow-list the filename
        self.state.fspath_allowlist.add(filename)
        return filename
