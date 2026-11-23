"""
Runtime StdLib implementations for tasks and workflows.
"""

import hashlib
import logging
import os
import stat
from typing import TYPE_CHECKING

import bracex
from wcmatch import glob as wcmatch_glob

from .. import Env, Error, StdLib, Type, Value
from .._util import StructuredLogMessage as _
from .._util import WDLVersion, wdl_version_geq
from . import config
from ._io_helpers import _resolve_workflow_path, resolve_source_relative_path
from .cache import CallCache, CallCacheAddPaths, digest_inputs
from .download import able as downloadable
from .error import OutputError

if TYPE_CHECKING:
    from ._workflow_state import StateMachine
    from .task_container import TaskContainer


# upper bounds on one glob() pattern's brace expansion: the number of
# alternatives (bracex counts them as it generates them, so the bound
# also covers nested cross products) and their total length (each
# alternative retains its own copy of any common suffix)
_MAX_GLOB_BRACE_ALTERNATIVES = 4096
_MAX_GLOB_BRACE_EXPANSION_LENGTH = 16777216


def _expand_bash_braces(pattern: str) -> list[str]:
    """
    Expand the brace expressions in a glob pattern (comma lists, nesting,
    and ``{x..y}`` sequences), in expansion order, using bracex -- wcmatch's
    brace engine. Backslash escapes are kept verbatim for the matcher to
    interpret, as in Bash, and braces the expander can't parse are left
    literal, also as in Bash.
    """
    try:
        alternatives = []
        total_length = 0
        for alt in bracex.iexpand(pattern, keep_escapes=True, limit=_MAX_GLOB_BRACE_ALTERNATIVES):
            total_length += len(alt)
            if total_length > _MAX_GLOB_BRACE_EXPANSION_LENGTH:
                raise OutputError("glob() pattern brace expansion is too large")
            alternatives.append(alt)
        return alternatives
    except (bracex.ExpansionLimitException, RecursionError):
        # bracex enforces the count limit during generation, but deep
        # nesting can hit Python's recursion limit first; both are the
        # same class of excess
        raise OutputError("glob() pattern brace expansion is too large") from None


class TaskStdLib(StdLib.Base):
    logger: logging.Logger
    container: "TaskContainer"
    inputs_only: bool  # if True then only permit access to input files
    source_dir: str
    cache_add_paths: CallCacheAddPaths

    def __init__(
        self,
        wdl_version: str,
        logger: logging.Logger,
        container: "TaskContainer",
        inputs_only: bool,
        *,
        source_dir: str = "",
        cache_add_paths: CallCacheAddPaths | None = None,
        eval_context: StdLib.EvalContext | None = None,
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
        self.cache_add_paths = cache_add_paths or CallCacheAddPaths()

    def _source_relative_host_path(self, filename: str, desc: str) -> str:
        directory = filename.endswith("/")
        value = Value.Directory(filename) if directory else Value.File(filename)
        result = resolve_source_relative_path(self.container.cfg, self.source_dir, desc, value)
        if result.value is None:
            raise Error.InputError(f"File/Directory path not found in {desc}: {filename}")
        if result.source_path:
            self.cache_add_paths.add(result.source_path)
        return result.value

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
        *,
        source_dir: str = "",
        cache_add_paths: CallCacheAddPaths | None = None,
        eval_context: StdLib.EvalContext | None = None,
    ) -> None:
        super().__init__(
            wdl_version,
            logger,
            container,
            True,
            source_dir=source_dir,
            cache_add_paths=cache_add_paths,
            eval_context=eval_context,
        )


class TaskOutputStdLib(TaskStdLib):
    # StdLib for evaluation of task outputs
    def __init__(
        self,
        wdl_version: str,
        logger: logging.Logger,
        container: "TaskContainer",
        *,
        eval_context: StdLib.EvalContext | None = None,
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
            host_dir = lib.container.host_work_dir()

            def _translate_work_path(target: str) -> str | None:
                # map an absolute path under the container work directory to
                # the corresponding host path; None if it isn't one. Reject
                # ".." rather than collapse it across symlinks; preserve
                # trailing "/" and "/." because both require a directory.
                prefix = [
                    c for c in os.path.join(lib.container.container_dir, "work").split("/") if c
                ]
                comps = target.split("/")[1:]
                i = 0
                for want in prefix:
                    while i < len(comps) and comps[i] in (".", ""):
                        i += 1
                    if i >= len(comps) or comps[i] != want:
                        return None
                    i += 1
                suffix = comps[i:]
                while suffix and suffix[0] == "":
                    suffix.pop(0)
                if ".." in suffix:
                    return None
                if not suffix:
                    return lib.container.host_work_dir()
                return os.path.join(lib.container.host_work_dir(), *suffix)

            def _keep(fn: str) -> bool:
                # Keep regular files, links to them, and dangling links.
                # Limit final-component hops; the host resolves intermediate components.
                path = fn
                followed = False
                hops = 0
                while True:
                    try:
                        mode = os.lstat(path).st_mode
                    except (FileNotFoundError, NotADirectoryError):
                        return followed
                    except OSError:
                        return False
                    if not stat.S_ISLNK(mode):
                        return stat.S_ISREG(mode)
                    followed = True
                    hops += 1
                    if hops > 40:
                        return False
                    try:
                        target = os.readlink(path)
                    except OSError:
                        return False
                    if os.path.isabs(target):
                        path = _translate_work_path(target) or target
                    else:
                        path = os.path.join(os.path.dirname(path), target)

            # Brace expansion precedes pathname expansion: sort matches
            # within each alternative, then concatenate in brace order.
            # Apply the relative-path and no-uplevel rules to each
            # alternative, but match it as written: a trailing / or /. still
            # requires a directory. Pass host_dir as the matching root, not
            # as pattern text, so its metacharacters need no escaping.
            # Relative results arrive in scan order and may retain a
            # leading ./ (which brace alternatives can produce).
            host_files: list[str] = []
            for alt in _expand_bash_braces(pat):
                if alt.startswith("/"):
                    raise OutputError("glob() pattern must be relative to task working directory")
                if alt.startswith("..") or "/.." in alt:
                    raise OutputError("glob() pattern must not use .. uplevels")
                kept = []
                for rel in wcmatch_glob.iglob(alt, flags=0, root_dir=host_dir):
                    while rel.startswith("./"):
                        rel = rel[2:]
                    if not rel:
                        continue
                    # Backslash quoting can hide uplevels from the pattern
                    # checks above (e.g. \../x). Recheck the interpreted
                    # names before joining them to host_dir.
                    if os.path.isabs(rel) or ".." in rel.split("/"):
                        raise OutputError(
                            "glob() match must be relative to the task working directory"
                            " and contain no '..' components"
                        )
                    if _keep(os.path.join(host_dir, rel)):
                        kept.append(rel)
                host_files.extend(os.path.join(host_dir, rel) for rel in sorted(kept))
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
        *,
        eval_context: StdLib.EvalContext | None = None,
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
        result = resolve_source_relative_path(self.cfg, self.state.workflow.source_dir, desc, value)
        if result.value is None:
            raise Error.InputError(f"File/Directory path not found in {desc}: {filename}")
        if result.source_path:
            # Remember resolved source-relative paths for cache coherence (since they won't appear
            # in the run inputs, but can affect workflow outputs).
            self.state.cache_add_paths.add(result.source_path)
        return result.value

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
        cache_key = "write_/" + digest_inputs(cache_in)
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
                inputs=cache_in,
                add_paths=CallCacheAddPaths(),
            )

        # whichever path we took: allow-list the filename
        self.state.fspath_allowlist.add(filename)
        return filename
