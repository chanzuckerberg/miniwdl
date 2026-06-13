"""
Internal runtime File/Directory, input/download, and output-linking helpers.
"""

import math
import os
import shutil
import logging
from typing import Dict, NamedTuple, Optional, Set, Tuple, Union

import regex

from .. import Env, Error, Expr, Type, Value, Tree
from .._util import link_force, path_really_within, symlink_force
from .._util import StructuredLogMessage as _
from . import config
from .cache import CallCache, CallCacheAddPaths
from .download import able as downloadable


def _resolve_source_relative_path_raw(
    cfg: config.Loader,
    source_directory: str,
    desc: str,
    v: Union[Value.File, Value.Directory],
) -> Optional[str]:
    """
    Resolve one File/Directory path against a WDL source directory when needed.

    ``source_directory`` is either "" or a local WDL source directory with trailing "/". Absolute
    paths and downloadable URIs are returned unchanged. Relative paths require ``source_directory``,
    are resolved with realpath, and must remain inside the source directory tree. Missing paths
    return None so callers can rewrite optional File?/Directory? values to Null before final type
    coercion.

    This scalar helper has no side effects.
    """
    isdir = isinstance(v, Value.Directory)
    if os.path.isabs(v.value) or downloadable(cfg, v.value, directory=isdir):
        return v.value

    if not source_directory:
        raise Error.InputError(
            "relative File/Directory path in "
            + desc
            + " requires a local WDL source file: "
            + v.value
        )

    root = (
        "/"
        if cfg["file_io"].get_bool("copy_input_files")
        else os.path.realpath(cfg["file_io"]["root"])
    )
    if not path_really_within(source_directory, root):
        raise Error.InputError(
            "WDL source directories with source-relative File & Directory inputs must be "
            f"located within the configured `file_io.root' directory `{root}' unlike "
            f"`{source_directory}'"
        )

    ans = os.path.realpath(
        os.path.join(source_directory, v.value.rstrip("/") if isdir else v.value)
    )
    within = path_really_within(ans, source_directory)
    if within and not path_really_within(ans, root):
        raise Error.InputError(
            "Source-relative File & Directory inputs must be located within the configured "
            f"`file_io.root' directory `{root}' unlike `{ans}'"
        )
    if within and not os.path.exists(ans):
        return None
    if within and not (os.path.isdir(ans) if isdir else os.path.isfile(ans)):
        kind = "Directory" if isdir else "File"
        expected = "directory" if isdir else "file"
        raise Error.InputError(f"{kind} path is not a {expected}: {v.value}")
    if not within:
        raise Error.InputError(
            "File/Directory path in "
            + desc
            + f" must reside within WDL source directory {source_directory}: "
            + v.value
        )

    return ans


class SourceRelativePathResolved(NamedTuple):
    """
    Result of resolving one File/Directory value against a WDL source directory.

    ``value`` is the rewritten File/Directory value, or None for an optional missing path.
    ``source_path`` is the absolute present path with the Directory trailing-slash convention,
    suitable for workflow allowlists, task container mounts, and CallCacheAddPaths.add().
    ``absent_path`` is the absolute missing path to record with ``absent=True``.
    """

    value: Optional[str]
    source_path: Optional[str] = None
    absent_path: Optional[str] = None


class SourceRelativePathsResolved(NamedTuple):
    """
    Result of resolving File/Directory leaves within a compound value.
    """

    value: Value.Base
    source_paths: Set[str]
    cache_add_paths: CallCacheAddPaths


def _resolve_source_relative_path(
    cfg: config.Loader,
    source_directory: str,
    desc: str,
    v: Union[Value.File, Value.Directory],
) -> SourceRelativePathResolved:
    """
    Resolve one File/Directory path and report any cache_add_path it implies.
    """
    ans = _resolve_source_relative_path_raw(cfg, source_directory, desc, v)
    if ans is None:
        return SourceRelativePathResolved(
            None, absent_path=_source_relative_cache_add_path(source_directory, v)
        )
    if ans == v.value:
        return SourceRelativePathResolved(ans)
    source_path = ans + ("/" if isinstance(v, Value.Directory) else "")
    return SourceRelativePathResolved(ans, source_path=source_path)


def _resolve_source_relative_paths(
    cfg: config.Loader,
    source_directory: str,
    value: Value.Base,
    desired_type: Type.Base,
    desc: str,
) -> SourceRelativePathsResolved:
    """
    Coerce a value to a path-containing type and resolve any source-relative paths within.

    This recursively applies ``_resolve_source_relative_path_raw`` to File/Directory leaves after
    coercing ``value`` to ``desired_type``. It returns both newly resolved present paths, so callers
    can perform allowlist or container-mount side effects after validation succeeds, and a
    CallCacheAddPaths with both present and optional-absent cache_add_paths for cache coherence.
    """
    source_paths: Set[str] = set()
    cache_add_paths = CallCacheAddPaths()
    value = value.coerce(desired_type)

    def rewrite_path(v: Union[Value.File, Value.Directory]) -> Optional[str]:
        result = _resolve_source_relative_path(cfg, source_directory, desc, v)
        if result.absent_path:
            cache_add_paths.add(result.absent_path, absent=True)
            return None
        if result.source_path:
            source_paths.add(result.source_path)
            cache_add_paths.add(result.source_path)
        return result.value

    value = Value.rewrite_paths(
        value,
        rewrite_path,
    )
    try:
        return SourceRelativePathsResolved(
            value.coerce(desired_type), source_paths, cache_add_paths
        )
    except FileNotFoundError:
        raise Error.InputError(f"File/Directory path not found in {desc}") from None


def _source_relative_cache_add_path(
    source_directory: str, v: Union[Value.File, Value.Directory]
) -> str:
    """
    Format the absolute path corresponding to a source-relative value as a cache_add_path.

    Used for optional missing paths after ``_resolve_source_relative_path_raw`` has established that
    the path is a valid source-relative path except for being absent.
    """
    isdir = isinstance(v, Value.Directory)
    ans = os.path.realpath(
        os.path.join(source_directory, v.value.rstrip("/") if isdir else v.value)
    )
    return ans + ("/" if isdir else "")


def _fspaths(env: Env.Bindings[Value.Base]) -> Set[str]:
    """
    Get the unique paths of all File & Directory values in the environment. Directory paths will
    have a trailing '/'.
    """
    ans = set()

    def collector(v: Value.Base) -> None:
        if isinstance(v, Value.File):
            assert not v.value.endswith("/")
            ans.add(v.value)
        elif isinstance(v, Value.Directory):
            ans.add(v.value.rstrip("/") + "/")
        for ch in v.children:
            collector(ch)

    for b in env:
        collector(b.value)
    return ans


def _resolve_workflow_path(
    cfg: config.Loader,
    allowlist: Set[str],
    desc: str,
    v: Union[Value.File, Value.Directory],
    null_if_missing: bool = False,
) -> Optional[str]:
    """
    Check a workflow-level File/Directory path and return the host path needed for I/O.

    The path is permitted when it is already in the workflow allowlist, is a downloadable URI, is
    nested under an allowlisted input Directory, or is accepted by ``allow_any_input``. Local paths
    accepted through the latter two cases are checked for existence, expected File/Directory kind,
    and containment under the input Directory or configured root.

    WDL 1.2 source-relative declaration/call-input paths are resolved and allowlisted before this
    helper runs. If an otherwise-allowed child path is missing, return None only for optional-path
    processing via ``null_if_missing``.
    """
    isdir = isinstance(v, Value.Directory)
    fspath = v.value.rstrip("/") + ("/" if isdir else "")
    if fspath in allowlist or downloadable(cfg, fspath, directory=isdir):
        return v.value
    allowlisted_child, allowlisted_child_path = _resolve_allowlisted_directory_child(
        cfg, allowlist, fspath, isdir
    )
    if allowlisted_child:
        if allowlisted_child_path is None:
            if null_if_missing:
                return None
            raise Error.InputError(f"{desc} uses nonexistent file/directory: {fspath}")
        if not allowlisted_child_path:
            raise Error.InputError(f"{desc} uses nonexistent file/directory: {fspath}")
        return allowlisted_child_path
    if not cfg.get_bool("file_io", "allow_any_input"):
        raise Error.InputError(
            desc + " uses file/directory not expressly supplied with workflow inputs"
            " (to allow, set [file_io] allow_any_input = true): " + fspath
        )
    # allow_any_input: checks that normally happen in CLI.validate_input_path
    if not (os.path.isdir(fspath) if isdir else os.path.isfile(fspath)):
        raise Error.InputError(f"{desc} uses nonexistent file/directory: {fspath}")
    fspath = os.path.abspath(fspath).rstrip("/")
    if not path_really_within(fspath, cfg["file_io"]["root"]):
        raise Error.InputError(
            f"{desc} {v.value} must reside within [file_io] root " + cfg["file_io"]["root"]
        )
    return fspath


def _resolve_allowlisted_directory_child(
    cfg: config.Loader,
    allowlist: Set[str],
    fspath: str,
    isdir: bool,
) -> Tuple[bool, Optional[str]]:
    """
    Resolve a File/Directory path nested beneath an allowlisted input Directory.

    Local children must exist with the expected File/Directory kind and resolve inside the parent.
    Remote URI children are checked lexically.
    """
    for parent in allowlist:
        if not parent.endswith("/"):
            continue
        if downloadable(cfg, parent, directory=True):
            if fspath.startswith(parent):
                return True, fspath.rstrip("/") if isdir else fspath
            continue
        if not path_really_within(fspath, parent):
            continue
        if not os.path.exists(fspath):
            return True, None
        if os.path.isdir(fspath) if isdir else os.path.isfile(fspath):
            return True, os.path.abspath(fspath).rstrip("/")
        return True, ""
    return False, None


def _add_downloadable_defaults(
    cfg: config.Loader, available_inputs: Env.Bindings[Tree.Decl], inputs: Env.Bindings[Value.Base]
) -> Env.Bindings[Value.Base]:
    """
    Look for available File/Directory inputs that default to a string constant appearing to be a
    downloadable URI. For each one, add a binding for that default to the user-supplied inputs (if
    not already overridden in them).

    This is to trigger download of the default URIs even though we otherwise don't evaluate input
    declarations until after processing downloads.
    """
    ans = inputs
    for b in available_inputs:
        if (
            isinstance(b.value.type, (Type.File, Type.Directory))
            and b.name not in ans
            and isinstance(b.value.expr, Expr.String)
        ):
            directory = isinstance(b.value.type, Type.Directory)
            maybe_uri = b.value.expr.literal
            if maybe_uri and downloadable(cfg, maybe_uri.value, directory=directory):
                v = (
                    Value.Directory(maybe_uri.value, b.value.expr)
                    if directory
                    else Value.File(maybe_uri.value, b.value.expr)
                )
                ans = ans.bind(b.name, v)
    return ans


def link_outputs(
    cache: CallCache,
    outputs: Env.Bindings[Value.Base],
    run_dir: str,
    hardlinks: bool = False,
    use_relative_output_paths: bool = False,
) -> Env.Bindings[Value.Base]:
    """
    Following a successful run, the output files may be scattered throughout a complex directory
    tree used for execution. To help navigating this, generate a subdirectory of the run directory
    containing nicely organized symlinks to the output files, and rewrite File values in the
    outputs env to use these symlinks.
    """

    def link1(target: str, link: str, directory: bool) -> None:
        if hardlinks:
            # TODO: what if target is an input from a different filesystem?
            if directory:
                shutil.copytree(target, link, symlinks=True, copy_function=link_force)
            else:
                link_force(target, link)
        else:
            symlink_force(target, link)

    def map_paths(v: Value.Base, dn: str) -> Value.Base:
        if isinstance(v, (Value.File, Value.Directory)):
            target = (
                v.value
                if os.path.exists(v.value)
                else cache.get_download(v.value, isinstance(v, Value.Directory))
            )
            if target:
                target = os.path.realpath(target)
                assert os.path.exists(target)
                if not hardlinks and path_really_within(target, os.path.dirname(run_dir)):
                    # make symlink relative
                    target = os.path.relpath(target, start=os.path.realpath(dn))
                link = os.path.join(dn, os.path.basename(v.value.rstrip("/")))
                os.makedirs(dn, exist_ok=False)
                link1(target, link, isinstance(v, Value.Directory))
                # Drop a dotfile alongside Directory outputs, to inform a program crawling the out/
                # directory without reference to the output types or JSON for whatever reason. It
                # might otherwise have trouble distinguishing Directory outputs among the
                # structured subdirectories we create for compound types.
                if isinstance(v, Value.Directory):
                    with open(os.path.join(dn, ".WDL_Directory"), "w") as _dotfile:
                        pass
                v.value = link
        # recurse into compound values
        elif isinstance(v, Value.Array) and v.value:
            d = int(math.ceil(math.log10(len(v.value))))  # how many digits needed
            for i in range(len(v.value)):
                v.value[i] = map_paths(v.value[i], os.path.join(dn, str(i).rjust(d, "0")))
        elif isinstance(v, Value.Map) and v.value:
            # create a subdirectory for each key, as long as the key names seem to make reasonable
            # path components; otherwise, treat the dict as a list of its values
            keys_ok = (
                sum(
                    1
                    for b in v.value
                    if regex.fullmatch("[-_a-zA-Z0-9][-_a-zA-Z0-9.]*", str(b[0]).strip("'\""))
                    is None
                )
                == 0
            )
            d = int(math.ceil(math.log10(len(v.value))))
            for i, b in enumerate(v.value):
                v.value[i] = (
                    b[0],
                    map_paths(
                        b[1],
                        os.path.join(
                            dn, str(b[0]).strip("'\"") if keys_ok else str(i).rjust(d, "0")
                        ),
                    ),
                )
        elif isinstance(v, Value.Pair):
            v.value = (
                map_paths(v.value[0], os.path.join(dn, "left")),
                map_paths(v.value[1], os.path.join(dn, "right")),
            )
        elif isinstance(v, Value.Struct):
            for key in v.value:
                v.value[key] = map_paths(v.value[key], os.path.join(dn, key))
        return v

    os.makedirs(os.path.join(run_dir, "out"), exist_ok=False)

    if use_relative_output_paths:
        return link_outputs_relative(link1, cache, outputs, run_dir, hardlinks=hardlinks)

    return outputs.map(
        lambda binding: Env.Binding(
            binding.name,
            map_paths(
                Value.rewrite_paths(binding.value, lambda v: v.value),  # nop to deep copy
                os.path.join(run_dir, "out", binding.name),
            ),
        )
    )


def link_outputs_relative(
    link1,
    cache: CallCache,
    outputs: Env.Bindings[Value.Base],
    run_dir: str,
    hardlinks: bool = False,
) -> Env.Bindings[Value.Base]:
    """
    link_outputs with [file_io] use_relative_output_paths = true. We organize the links to reflect
    the generated files' paths relative to their task working directory.
    """
    link_destinations: Dict[str, str] = dict()

    def map_path_relative(v: Union[Value.File, Value.Directory]) -> str:
        target = (
            v.value
            if os.path.exists(v.value)
            else cache.get_download(v.value, isinstance(v, Value.Directory))
        )
        if target:
            real_target = os.path.realpath(target)
            rel_link = None
            if path_really_within(target, os.path.join(run_dir, "work")):
                # target was generated by current task; use its path relative to the task work dir
                if not os.path.basename(run_dir).startswith("download-"):  # except download tasks
                    rel_link = os.path.relpath(
                        real_target, os.path.realpath(os.path.join(run_dir, "work"))
                    )
            else:
                # target is an out/ link generated by a call in the current workflow OR a cached
                # run; use the link's path relative to that out/ dir, which by induction should
                # equal its path relative to the original work/ dir.
                # we need heuristic to find the out/ dir in a task/workflow run directory, since the
                # user's cwd or the task-generated relative path might coincidentally have
                # something named 'out'.
                p = None
                for p in reversed([m.span()[0] for m in regex.finditer("/out(?=/)", target)]):
                    if p and (
                        os.path.isfile(os.path.join(target[:p], "task.log"))
                        or os.path.isfile(os.path.join(target[:p], "workflow.log"))
                    ):
                        break
                    p = None
                if p and p + 5 < len(target):
                    rel_link = os.path.relpath(target, target[: p + 5])
            # if neither of the above cases applies, then fall back to just the target basename
            rel_link = rel_link or os.path.basename(target)
            abs_link = os.path.join(os.path.join(run_dir, "out"), rel_link)
            if link_destinations.get(abs_link, real_target) != real_target:
                raise FileExistsError(
                    "Output filename collision; to allow this, set"
                    " [file_io] use_relative_output_paths = false. Affected path: " + abs_link
                )
            os.makedirs(os.path.dirname(abs_link), exist_ok=True)
            link1(real_target, abs_link, isinstance(v, Value.Directory))
            link_destinations[abs_link] = real_target
            return abs_link
        return v.value

    return Value.rewrite_env_paths(outputs, map_path_relative)


def _warn_output_basename_collisions(
    logger: logging.Logger, outputs: Env.Bindings[Value.Base]
) -> None:
    targets_by_basename: Dict[str, Set[str]] = {}

    def walker(v: Union[Value.File, Value.Directory]) -> str:
        target = v.value
        if os.path.exists(target):
            target = os.path.realpath(target)
        basename = os.path.basename(target)
        targets_by_basename.setdefault(basename, set()).add(target)
        return v.value

    Value.rewrite_env_paths(outputs, walker)

    collisions = [bn for bn, targets in targets_by_basename.items() if len(targets) > 1]
    if collisions:
        logger.warning(
            _(
                "multiple output files share the same basename; while miniwdl supports this,"
                " consider modifying WDL to ensure distinct output basenames",
                basenames=collisions,
            )
        )


def _warn_struct_extra(
    logger: logging.Logger, decl_name: str, v: Value.Base, warned_keys: Optional[Set[str]] = None
) -> None:
    """
    Log notices about extraneous keys found in struct initialization from JSON/Map/Object.
    """
    if warned_keys is None:
        warned_keys = set()
    if isinstance(v, Value.Struct) and v.extra:
        extra_keys = set(k for k in v.extra if not k.startswith("#"))
        if extra_keys - warned_keys:
            logger.notice(
                _(
                    "extraneous keys in struct initializer",
                    decl=decl_name,
                    struct=str(v.type),
                    extra_keys=list(extra_keys),
                )
            )
            warned_keys.update(extra_keys)
    for ch in v.children:
        _warn_struct_extra(logger, decl_name, ch, warned_keys)
