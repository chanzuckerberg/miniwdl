"""
Routines for packaging a WDL source file, with all imported source files, into a ZIP (or tar)
archive.

*New in v1.5.0*
"""

import contextlib
import glob
import io
import json
import logging
import os
import pathlib
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from collections.abc import Iterator
from typing import Any, NamedTuple

from . import Error, Expr, Lint, Tree, Type, Value
from ._util import StructuredLogMessage as _
from ._util import WDLVersion, path_really_within, wdl_version_geq


def build(
    top_doc: Tree.Document,
    archive: str,
    logger: logging.Logger,
    inputs: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
    archive_format: str = "zip",
    additional_files: list[str] | None = None,
    auto_add: bool = True,
    add_all: bool = False,
):
    """
    Generate zip archive of the WDL document, all its imports, optional default inputs, and a
    generated manifest JSON.

    If imports are drawn from outside the main WDL's directory (or by URI), they'll be stored in a
    special subdirectory and import statements will be rewritten to match.

    :param top_doc: the loaded main WDL document
    :param archive: destination filename for the archive
    :param inputs: default inputs to include in the archive (JSON-like dict), if any
    :param meta: extra metadata to include in the manifest, if any
    :param archive_format: ``"zip"`` or ``"tar"``
    :param additional_files: other files, directories, or glob patterns to include, stored at the
                             same location relative to the WDL source files
    :param auto_add: automatically include the files and directories named by literal
                     source-relative paths in WDL 1.2+ declarations and call inputs. Either
                     way, source-relative references that the archive doesn't cover draw
                     warnings.
    :param add_all: also include every other file under each WDL source directory: in a git
                    work tree, the files git tracks plus untracked files it doesn't ignore (as
                    listed by ``git ls-files``); otherwise, all files. Raises
                    :class:`WDL.Error.InputError` if git can't be run but a source directory
                    appears to be in a git repository, rather than include files that git would
                    ignore.
    """

    with contextlib.ExitStack() as cleanup:
        # write WDL source code to temp directory
        dir_to_zip, zip_paths, wdls = _build_source_dir(cleanup, top_doc, logger)

        # add MANIFEST.json; schema roughly following Amazon Genomics CLI's:
        #  https://aws.github.io/amazon-genomics-cli/docs/concepts/workflows/#multi-file-workflows
        manifest: dict[str, Any] = {"mainWorkflowURL": os.path.basename(top_doc.pos.abspath)}
        if meta:
            manifest["meta"] = meta
        if inputs:
            manifest["inputFileURLs"] = ["default_input.json"]
            with open(os.path.join(dir_to_zip, "default_input.json"), "w") as inputs_file:
                json.dump(inputs, inputs_file, indent=2)
        with open(os.path.join(dir_to_zip, "MANIFEST.json"), "w") as manifest_file:
            json.dump(manifest, manifest_file, indent=2)
        logger.debug("manifest = " + json.dumps(manifest))
        added: dict[str, str] = {}
        if additional_files:
            added = add_additional_files(dir_to_zip, additional_files, zip_paths, wdls, logger)

        # add files referenced by WDL 1.2+ source-relative paths (and --add-all)
        _add_source_relative_files(
            dir_to_zip, zip_paths, wdls, added, archive, auto_add, add_all, logger
        )

        # zip the temp directory (into another temp directory)
        spool_dir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_"))
        spool_zip = os.path.join(spool_dir, os.path.basename(archive))
        logger.info(f"Prepare archive {spool_zip} from directory {dir_to_zip}")
        create_reproducible_archive(dir_to_zip, spool_zip, archive_format)

        # move into final location (hopefully atomic)
        dirname = os.path.dirname(archive)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        logger.info(f"Move archive to destination {archive}")
        shutil.move(spool_zip, archive)


def build_source_dir(
    cleanup: contextlib.ExitStack, top_doc: Tree.Document, logger: logging.Logger
) -> str:
    zip_dir, _zip_paths, _wdls = _build_source_dir(cleanup, top_doc, logger)
    return zip_dir


def _build_source_dir(
    cleanup: contextlib.ExitStack, top_doc: Tree.Document, logger: logging.Logger
) -> tuple[str, dict[str, str], dict[str, Tree.Document]]:
    """
    Stage rewritten WDL source files and return the path mapping used to do it.

    ``build_source_dir()`` historically returned only the staging directory. The additional-file
    path logic needs the same original-source to archive-path mapping, so this internal variant
    returns all three pieces while preserving the public helper above.
    """
    # directory of main WDL file (possibly URI)
    main_dir = os.path.dirname(top_doc.pos.abspath).rstrip("/") + "/"

    # collect all WDL docs keyed by abspath
    wdls = {}
    queue = [top_doc]
    while queue:
        a_doc = queue.pop()
        for imported_doc in a_doc.imports:
            assert imported_doc.doc
            queue.append(imported_doc.doc)
        wdls[a_doc.pos.abspath] = a_doc

    # derive archive paths
    zip_paths = build_zip_paths(main_dir, wdls, logger)
    assert sorted(zip_paths.keys()) == sorted(wdls.keys())
    assert zip_paths[top_doc.pos.abspath] == os.path.basename(top_doc.pos.abspath)

    # write source files into temp directory (rewriting imports as needed)
    zip_dir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_"))
    for abspath, a_doc in wdls.items():
        source_lines = rewrite_imports(a_doc, zip_paths, logger)
        fn = os.path.join(zip_dir, zip_paths[abspath])
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        with open(fn, "w") as outfile:
            for line in source_lines:
                print(line, file=outfile)

    return zip_dir, zip_paths, wdls


def build_zip_paths(
    main_dir: str, wdls: dict[str, Tree.Document], logger: logging.Logger
) -> dict[str, str]:
    # compute the path inside the archive at which to store each document

    ans = {}
    outside_warn = False
    for abspath in wdls:
        if abspath.startswith(main_dir):
            ans[abspath] = os.path.relpath(abspath, main_dir)
        else:
            # place outside import under __outside_wdl, vaguely reproducing directory structure
            abspath2 = abspath.replace("://", "_")
            prefix = os.path.commonprefix([abspath2, main_dir.replace("://", "_")])
            if prefix and not prefix.endswith("/"):
                prefix = os.path.dirname(prefix) + "/"
            ans[abspath] = "__outside_wdl/" + abspath2[len(prefix) :]
            outside_warn = True
        logger.info(f"{ans[abspath]} <= {abspath}")

    if outside_warn:
        logger.warning(
            "One or more source files are imported from outside the top-level WDL's directory."
            " The source archive will store them under __outside_wdl/"
            " and WDL import statements will be rewritten to match."
        )

    return ans


def rewrite_imports(
    doc: Tree.Document, zip_paths: dict[str, str], logger: logging.Logger
) -> list[str]:
    # rewrite doc source_lines, changing import statements to refer to relative path in zip
    source_lines = doc.source_lines.copy()

    for imp in doc.imports:
        assert imp.doc
        lo = imp.pos.line - 1
        hi = imp.pos.end_line
        found = False
        for lineno in range(lo, hi):
            line = source_lines[lineno]
            old_uri = imp.uri
            new_uri = os.path.relpath(
                zip_paths[imp.doc.pos.abspath], os.path.dirname(zip_paths[doc.pos.abspath])
            )
            for quot in ('"', "'"):
                old_uri_pattern = f"{quot}{old_uri}{quot}"
                if old_uri_pattern in line:
                    assert quot not in new_uri
                    found = True
                    line2 = line.replace(old_uri_pattern, f"{quot}{new_uri}{quot}")
                    if line != line2:
                        logger.debug(doc.pos.abspath)
                        logger.debug("  " + line)
                        logger.debug("  => " + line2)
                        source_lines[lineno] = line2
        assert found

    return source_lines


def add_additional_files(
    zip_dir: str,
    additional_files: list[str],
    zip_paths: dict[str, str],
    wdls: dict[str, Tree.Document],
    logger: logging.Logger,
) -> dict[str, str]:
    """
    Add files/directories to an archive staging directory, preserving paths relative to WDL source
    directories represented in ``zip_paths``. Returns a map of each staged archive path to the
    realpath of the source file it was staged from.

    Examples:

    * ``/proj/data/ref.fa`` -> ``data/ref.fa`` (when ``/proj/main.wdl`` is archived as
      ``main.wdl``)
    * ``/shared/lib/data/ref.fa`` -> ``__outside_wdl/lib/data/ref.fa`` (when
      ``/shared/lib/tasks.wdl`` is rewritten under ``__outside_wdl/lib/tasks.wdl``)
    """
    logger.debug(f"Additional files: {additional_files}")
    source_dirs = _additional_source_dirs(zip_paths, wdls)
    if not source_dirs:
        raise Error.InputError("Additional files require local WDL source files")

    copied: dict[str, str] = {}
    for pattern in additional_files:
        # Leave non-glob paths literal so we can report "not found" rather than "matched nothing".
        matches = glob.glob(pattern, recursive=True) if glob.has_magic(pattern) else [pattern]
        matches = sorted(matches)
        if not matches:
            raise Error.InputError("Additional file pattern matched nothing: " + pattern)
        for match in matches:
            if not os.path.lexists(match):
                raise Error.InputError("Additional file not found: " + match)
            for src, dest in _additional_files_from_path(match, source_dirs):
                if dest in copied:
                    # Overlapping globs may name the same source file twice; that's harmless.
                    # A different realpath landing at the same archive path is a real collision.
                    if copied[dest] == os.path.realpath(src):
                        continue
                    raise Error.InputError("Additional file overwrites existing path: " + dest)
                dest_path = os.path.join(zip_dir, dest)
                if os.path.exists(dest_path) or os.path.lexists(dest_path):
                    raise Error.InputError("Additional file overwrites existing path: " + dest)
                if not path_really_within(dest_path, zip_dir):
                    raise Error.InputError("Invalid additional file destination: " + dest)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                shutil.copyfile(src, dest_path)
                copied[dest] = os.path.realpath(src)
                logger.info(f"{dest} <= {src}")
    return copied


def _additional_source_dirs(
    zip_paths: dict[str, str], wdls: dict[str, Tree.Document]
) -> list[tuple[str, str]]:
    """
    Pair each local WDL source directory with its corresponding archive directory.

    Additional files are accepted only when they resolve under one of these directories. Sorting by
    longest source directory first makes nested imports win over their parents, which is needed to
    preserve the source-relative meaning of WDL 1.2 paths after import rewrites.

    Examples:

    * ``/proj/tasks/t.wdl`` -> ``tasks`` (nested local import directory wins over ``/proj``)
    * ``/proj/main.wdl`` -> ``""`` (top-level source directory maps to the archive root)
    * ``/shared/lib/t.wdl`` -> ``__outside_wdl/lib`` (outside import keeps its rewritten prefix)
    """
    ans: dict[str, str] = {}
    for abspath, zip_path in zip_paths.items():
        if abspath in wdls:
            source_dir = wdls[abspath].source_dir.rstrip("/")
            if not source_dir or not os.path.isfile(abspath):
                continue
            archive_dir = os.path.dirname(zip_path)
            # Different archive paths for one source directory would make it impossible to know
            # where non-WDL neighbors from that directory should go.
            if source_dir in ans and ans[source_dir] != archive_dir:
                raise Error.InputError(
                    "Cannot place additional files relative to ambiguous WDL source directory: "
                    + source_dir
                )
            ans[source_dir] = archive_dir
    return sorted(ans.items(), key=lambda item: len(item[0]), reverse=True)


def _additional_files_from_path(
    path: str, source_dirs: list[tuple[str, str]]
) -> Iterator[tuple[str, str]]:
    """
    Expand one matched additional path into concrete file/archive-path pairs.

    Directories are walked recursively, following symlinks only after each directory entry passes
    ``_additional_dest()``. ``visited_dirs`` prevents symlink loops from cycling indefinitely while
    still allowing safe symlinks to contribute the file contents they point to.

    Examples:

    * ``/proj/data/a.txt`` -> ``data/a.txt`` (file found while walking ``/proj/data``)
    * ``/proj/data/sub/b.txt`` -> ``data/sub/b.txt`` (recursive directory contents are kept)
    * ``/proj/data/latest.txt`` -> ``data/latest.txt`` (safe symlink content is copied)
    """
    path = os.path.abspath(path)
    _additional_dest(path, source_dirs)  # validate the requested path, including symlink target
    if os.path.isdir(path):
        visited_dirs: set[str] = set()
        for dirpath, dirnames, filenames in os.walk(path, followlinks=True):
            # Re-check every walked directory because os.walk follows symlinks when asked.
            _additional_dest(dirpath, source_dirs)
            real_dirpath = os.path.realpath(dirpath)
            if real_dirpath in visited_dirs:
                dirnames[:] = []
                continue
            visited_dirs.add(real_dirpath)
            for dirname in list(dirnames):
                dirname_path = os.path.join(dirpath, dirname)
                try:
                    # Reject a directory symlink before os.walk descends into it.
                    _additional_dest(dirname_path, source_dirs)
                except Error.InputError:
                    raise Error.InputError(
                        "Additional directory contains unsafe symlink or path: " + dirname_path
                    ) from None
                if os.path.realpath(dirname_path) in visited_dirs:
                    # Avoid loops like data/back -> .., even when the target is otherwise safe.
                    dirnames.remove(dirname)
            for filename in sorted(filenames):
                filename_path = os.path.join(dirpath, filename)
                yield filename_path, _additional_dest(filename_path, source_dirs)
    else:
        yield path, _additional_dest(path, source_dirs)


def _additional_dest(path: str, source_dirs: list[tuple[str, str]]) -> str:
    """
    Compute the archive path for one additional file or directory after safety checks.

    The path and its realpath target must both remain inside the selected source directory. This
    permits ordinary in-tree symlinks while rejecting symlinks that would package files from outside
    the WDL source tree.

    Examples:

    * ``/proj/data/a.txt`` -> ``data/a.txt`` (source dir ``/proj`` maps to archive root)
    * ``/shared/lib/data/a.txt`` -> ``__outside_wdl/lib/data/a.txt`` (source dir
      ``/shared/lib`` maps to ``__outside_wdl/lib``)
    * ``/proj/data/secret -> /etc/passwd`` -> rejected (symlink target escapes ``/proj``)
    """
    if not (os.path.isfile(path) or os.path.isdir(path)):
        raise Error.InputError("Additional path is neither a file nor a directory: " + path)
    # Resolve symlinks in the parent directory (e.g. macOS /tmp -> /private/tmp) so the containment
    # check lines up with the realpath-canonicalized source directories, while keeping the leaf name
    # so safe in-tree symlinks retain their own archive path.
    real_path = os.path.join(os.path.realpath(os.path.dirname(path)), os.path.basename(path))
    for source_dir, archive_dir in source_dirs:
        relpath = os.path.relpath(real_path, os.path.realpath(source_dir))
        if relpath.startswith(".." + os.sep) or relpath == "..":
            continue
        # path_really_within additionally rejects symlinks whose target escapes the source tree.
        if path_really_within(path, source_dir):
            return os.path.normpath(os.path.join(archive_dir, relpath))
    raise Error.InputError("Additional path must reside within a WDL source directory: " + path)


class _SourceRelativeRef(NamedTuple):
    """
    A WDL 1.2 source-relative File/Directory reference in a declaration or call input
    """

    description: str  # location of the reference, for warnings
    source_dir: str  # directory of the WDL document containing the reference
    literal: str | None  # the literal path, or None if computed by an expression
    kind: Lint._SourceRelativePathKind | None  # classification of the literal
    directory: bool
    optional: bool

    @property
    def target(self) -> str:
        # the path as written, normalized but with symlinks preserved, as the runner resolves it
        assert self.literal is not None
        return os.path.abspath(os.path.join(self.source_dir, self.literal))


def _scan_source_relative_refs(wdls: dict[str, Tree.Document]) -> list[_SourceRelativeRef]:
    # Find File/Directory values in WDL 1.2+ declarations and call inputs, classifying literal
    # paths relative to the directory of the document containing them. (Call inputs resolve
    # against the caller's directory.) Paths passed to stdlib functions aren't scanned (#941).
    refs: list[_SourceRelativeRef] = []
    for doc in wdls.values():
        if not wdl_version_geq(doc.effective_wdl_version, WDLVersion.V1_2):
            continue
        doc_name = os.path.basename(doc.pos.abspath)
        for task in doc.tasks:
            for decl in (task.inputs or []) + task.postinputs:
                desc = f"task '{task.name}' declaration '{decl.name}' in {doc_name}"
                _scan_expr(decl.type, decl.expr, doc.source_dir, desc, refs)
        if not doc.workflow:
            continue
        for decl in doc.workflow.inputs or []:
            desc = f"workflow '{doc.workflow.name}' input '{decl.name}' in {doc_name}"
            _scan_expr(decl.type, decl.expr, doc.source_dir, desc, refs)
        for node in _workflow_nodes(doc.workflow.body):
            if isinstance(node, Tree.Decl):
                desc = f"declaration '{node.name}' in {doc_name}"
                _scan_expr(node.type, node.expr, doc.source_dir, desc, refs)
            elif isinstance(node, Tree.Call) and node.callee:
                for name, expr in node.inputs.items():
                    # available_inputs includes pre-1.0 tasks' implicit inputs
                    callee_input = node.callee.available_inputs.get(name)
                    if callee_input:
                        desc = f"call input '{node.name}.{name}' in {doc_name}"
                        _scan_expr(callee_input.type, expr, doc.source_dir, desc, refs)
    return refs


def _workflow_nodes(body: list[Tree.WorkflowNode]) -> Iterator[Tree.WorkflowNode]:
    for node in body:
        yield node
        if isinstance(node, Tree.WorkflowSection):
            yield from _workflow_nodes(node.body)


def _scan_expr(
    to_type: Type.Base,
    expr: Expr.Base | None,
    source_dir: str,
    description: str,
    refs: list[_SourceRelativeRef],
) -> None:
    if expr is None:
        return
    leaves = list(Lint._file_coercion_leaves(to_type, expr))
    if not leaves:
        # A non-literal compound value: if its type has no File/Directory positions where
        # to_type does, its paths are computed from strings (or an Any-typed value).
        if (
            not isinstance(expr, (Expr.Array, Expr.Map, Expr.Pair, Expr.Struct))
            and _has_path_type(to_type)
            and not _has_path_type(expr.type)
            and not _reads_path(expr)
        ):
            refs.append(
                _SourceRelativeRef(description, source_dir, None, None, False, to_type.optional)
            )
        return
    for leaf_type, leaf in leaves:
        directory = isinstance(leaf_type, Type.Directory)
        literal = leaf.literal
        # a computed path may still select from literal alternatives, as in the "default
        # unless overridden" idiom select_first([override, "data/default.fa"])
        values = [literal.value] if isinstance(literal, Value.String) else _alternatives(leaf)
        recorded = False
        for value in values:
            if "://" in value:
                continue  # downloadable URI
            kind = Lint._source_relative_path_kind(source_dir, value, directory)
            if kind != Lint._SourceRelativePathKind.ABSOLUTE:
                refs.append(
                    _SourceRelativeRef(
                        description, source_dir, value, kind, directory, leaf_type.optional
                    )
                )
                recorded = True
        if (
            not recorded
            and not isinstance(literal, Value.String)
            and isinstance(leaf.type, (Type.String, Type.Any))
            and not _reads_path(leaf)
        ):
            # path computed by an expression. One derived from another File/Directory value
            # (e.g. bam + ".bai") is absolute at run time, so it isn't reported.
            refs.append(
                _SourceRelativeRef(
                    description, source_dir, None, None, directory, leaf_type.optional
                )
            )


def _alternatives(expr: Expr.Base) -> list[str]:
    # string literals in positions a path may be selected from: array elements, if/else
    # branches, and select_first/select_all arguments (but not concatenation operands)
    literal = expr.literal
    if isinstance(literal, Value.String):
        return [literal.value]
    if isinstance(expr, Expr.Array):
        children = expr.items
    elif isinstance(expr, Expr.IfThenElse):
        children = [expr.consequent, expr.alternative]
    elif isinstance(expr, Expr.Apply) and expr.function_name in ("select_first", "select_all"):
        children = expr.arguments
    else:
        return []
    return [value for child in children for value in _alternatives(child)]


def _has_path_type(ty: Type.Base) -> bool:
    # whether File or Directory appears anywhere in the type
    if isinstance(ty, (Type.File, Type.Directory)):
        return True
    if isinstance(ty, (Type.StructInstance, Type.Object)) and ty.members is None:
        return False
    return any(_has_path_type(param) for param in ty.parameters)


def _reads_path(expr: Expr.Base) -> bool:
    # whether the expression reads a value whose type contains File/Directory
    return (isinstance(expr, (Expr.Ident, Expr.Get)) and _has_path_type(expr.type)) or any(
        _reads_path(child) for child in expr.children if isinstance(child, Expr.Base)
    )


def _source_relative_dest(ref: _SourceRelativeRef, archive_dirs: dict[str, str]) -> str | None:
    # where the runner will look for the reference after unpacking: the literal joined onto
    # the archive directory of the document containing it (preserving symlinks, like the runner)
    archive_dir = archive_dirs.get(ref.source_dir.rstrip("/"))
    if ref.literal is None or archive_dir is None:
        return None
    return os.path.normpath(os.path.join(archive_dir, ref.literal))


def _expand_ref(ref: _SourceRelativeRef, dest: str) -> list[tuple[str, str]]:
    # (source, archive path) pairs for a reference's target, including a directory's whole
    # tree; raises InputError if anything in it resolves outside the source directory
    if not path_really_within(ref.target, ref.source_dir):
        raise Error.InputError(
            "Source-relative path resolves outside the WDL source directory: " + ref.target
        )
    if not os.path.isdir(ref.target):
        return [(ref.target, dest)]
    return list(_walk_tree(ref.target, dest, ref.source_dir))


def _walk_tree(
    root: str,
    dest: str,
    within: str,
    logger: logging.Logger | None = None,
    skip_git: bool = False,
) -> Iterator[tuple[str, str]]:
    """
    Yield (path, archive path) for directory ``root`` and everything beneath it, following
    symlinks. A directory symlink back to one of its own ancestors is yielded but not
    descended into. An entry resolving outside ``within``, or a directory that can't be
    listed, raises :class:`WDL.Error.InputError`; or if ``logger`` is given, it's skipped
    with a warning.
    """

    def problem(message: str, path: str) -> None:
        if logger is None:
            raise Error.InputError(f"{message}: {path}")
        logger.warning(_("Skipping path for the source archive: " + message, path=path))

    def on_error(exn: OSError) -> None:
        problem(f"cannot list directory ({exn.strerror})", str(exn.filename))

    for dirpath, dirnames, filenames in os.walk(root, followlinks=True, onerror=on_error):
        archive_dirpath = os.path.normpath(os.path.join(dest, os.path.relpath(dirpath, root)))
        yield dirpath, archive_dirpath
        if _is_symlink_cycle(dirpath, root):
            dirnames[:] = []
            continue
        keep = []
        for name in sorted(dirnames) + sorted(filenames):
            path = os.path.join(dirpath, name)
            if skip_git and name == ".git":
                continue
            if not path_really_within(path, within):
                problem("resolves outside the source directory", path)
            elif name in dirnames:
                keep.append(name)
            else:
                yield path, os.path.join(archive_dirpath, name)
        dirnames[:] = keep


def _is_symlink_cycle(dirpath: str, root: str) -> bool:
    real = os.path.realpath(dirpath)
    while dirpath != root and dirpath != os.path.dirname(dirpath):
        dirpath = os.path.dirname(dirpath)
        if os.path.realpath(dirpath) == real:
            return True
    return False


def _add_source_relative_files(
    zip_dir: str,
    zip_paths: dict[str, str],
    wdls: dict[str, Tree.Document],
    added: dict[str, str],
    archive: str,
    auto_add: bool,
    add_all: bool,
    logger: logging.Logger,
) -> None:
    """
    Stage the targets of literal source-relative File/Directory paths in WDL 1.2+ declarations
    and call inputs (if ``auto_add``), and every file under the WDL source directories (if
    ``add_all``); then warn about references the archive doesn't cover (issue #878). Problems
    are warnings: this never fails the archive (except :func:`_git_ls_files`' refusal).
    ``added`` maps paths already staged by ``--additional`` to their source realpaths.
    """
    refs = _scan_source_relative_refs(wdls)
    if not (refs or add_all):
        return
    try:
        source_dirs = _additional_source_dirs(zip_paths, wdls)
    except Error.InputError as exn:
        logger.warning(_("Skipping automatic file inclusion", reason=str(exn)))
        source_dirs = []
    archive_dirs = dict(source_dirs)

    pairs: list[tuple[str, str, str | None]] = []
    for ref in refs if auto_add else []:
        dest = _source_relative_dest(ref, archive_dirs)
        if ref.kind != Lint._SourceRelativePathKind.OK or dest is None:
            continue
        try:
            pairs.extend((src, dst, ref.description) for src, dst in _expand_ref(ref, dest))
        except (Error.InputError, OSError) as exn:
            logger.warning(
                _(
                    "Skipping source-relative path for the source archive",
                    path=ref.literal,
                    reference=ref.description,
                    reason=str(exn),
                )
            )
    swept_dirs: set[str] = set()
    if add_all and source_dirs:
        swept_dirs = {os.path.realpath(source_dir) for source_dir, _ in source_dirs}
        exclude = {os.path.realpath(os.path.abspath(archive))}
        pairs.extend(
            (src, dst, None) for src, dst in _add_all_source_files(source_dirs, exclude, logger)
        )

    staged = {zip_path: os.path.realpath(abspath) for abspath, zip_path in zip_paths.items()}
    staged.update(added)
    n_files, n_bytes = _stage_files(zip_dir, pairs, staged, logger)
    if n_files:
        logger.info(
            _("Automatically included files in the source archive", files=n_files, bytes=n_bytes)
        )
    _warn_uncovered_refs(refs, zip_dir, archive_dirs, auto_add, swept_dirs, logger)


def _stage_files(
    zip_dir: str,
    pairs: list[tuple[str, str, str | None]],
    staged: dict[str, str],
    logger: logging.Logger,
) -> tuple[int, int]:
    """
    Copy (source, archive path, reference description) into the staging directory, recording
    each in ``staged`` (archive path -> source realpath). Something else already at the archive
    path -- a different file, or a generated one like MANIFEST.json -- is a collision, warned
    about and skipped. Failures are warnings: automatic inclusion never fails the archive.
    Returns the number of files and bytes staged.
    """
    n_files = n_bytes = 0
    for src, dest, reference in pairs:
        dest_path = os.path.join(zip_dir, dest)
        if not path_really_within(dest_path, zip_dir):
            logger.warning(
                _("Skipping file for the source archive: invalid destination", path=src, dest=dest)
            )
            continue
        if os.path.lexists(dest_path):
            same = staged.get(dest) == os.path.realpath(src)
            if not (same or (os.path.isdir(src) and os.path.isdir(dest_path))):
                logger.warning(
                    _(
                        "Skipping file for the source archive: a different file is already"
                        " staged at this archive path",
                        path=src,
                        dest=dest,
                        reference=reference,
                    )
                )
            continue
        try:
            if os.path.isdir(src):
                os.makedirs(dest_path, exist_ok=True)
            else:
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                shutil.copyfile(src, dest_path)
                n_files += 1
                n_bytes += os.path.getsize(dest_path)
        except OSError as exn:
            with contextlib.suppress(OSError):
                if os.path.isfile(dest_path):
                    os.unlink(dest_path)  # don't leave a partial copy
            logger.warning(
                _(
                    "Skipping file for the source archive: could not copy it",
                    path=src,
                    dest=dest,
                    reason=str(exn),
                )
            )
            continue
        staged[dest] = os.path.realpath(src)
        logger.info(f"{dest} <= {src}")
    return n_files, n_bytes


def _git_ls_files(source_dir: str) -> set[str] | None:
    """
    List the files git attributes to ``source_dir`` (tracked, plus untracked and not ignored),
    relative to it; or None if it isn't in a git work tree. If git can't be run but the
    directory shows evidence of a repository, raise :class:`WDL.Error.InputError` rather
    than let ``--add-all`` include files git would ignore.
    """
    try:
        proc = subprocess.run(
            [
                "git",
                "-C",
                source_dir,
                "ls-files",
                "-z",
                "--cached",
                "--others",
                "--exclude-standard",
            ],
            capture_output=True,
            check=False,
            # untranslated messages, for the "not a git repository" check below
            env={**os.environ, "LC_ALL": "C"},
        )
    except OSError as exn:
        if _repository_evidence(source_dir):
            raise Error.InputError(
                f"--add-all needs git to list the files in {source_dir}, which appears to be"
                " in a git repository, but git could not be run; install git, or use --add"
                " instead of --add-all"
            ) from exn
        return None
    if proc.returncode != 0:
        if b"not a git repository" in proc.stderr:
            return None
        if _repository_evidence(source_dir):
            stderr_lines = proc.stderr.decode("utf-8", errors="replace").strip().splitlines()
            detail = stderr_lines[0] if stderr_lines else f"exit status {proc.returncode}"
            raise Error.InputError(
                f"--add-all needs git to list the files in {source_dir}, which appears to be"
                f" in a git repository, but git ls-files failed ({detail}); fix that, or use"
                " --add instead of --add-all"
            )
        return None
    return {os.fsdecode(entry) for entry in proc.stdout.split(b"\0") if entry}


def _repository_evidence(source_dir: str) -> bool:
    # Best effort, consulted only when git can't be run: a .git (directory or file) or
    # .gitignore in source_dir or below, where the sweep looks anyway. Ancestors aren't examined.
    for _dirpath, dirnames, filenames in os.walk(source_dir):
        if ".git" in dirnames or ".git" in filenames or ".gitignore" in filenames:
            return True
    return False


def _add_all_source_files(
    source_dirs: list[tuple[str, str]], exclude: set[str], logger: logging.Logger
) -> Iterator[tuple[str, str]]:
    # (file, archive path) for every file under each source directory, for --add-all: git's
    # listing in a work tree, otherwise everything. Symlinks resolving outside the directory
    # are skipped with a warning.
    for source_dir, archive_dir in source_dirs:
        root = os.path.realpath(source_dir)
        git_files = _git_ls_files(root)
        if git_files is None:
            logger.info(
                _(
                    "Source directory is not in a git work tree (or git is unavailable);"
                    " including all files under it, with no ignore rules applied",
                    path=source_dir,
                )
            )
            candidates: Iterator[tuple[str, str]] = (
                (path, dest)
                for path, dest in _walk_tree(root, archive_dir, root, logger, skip_git=True)
                if not os.path.isdir(path)
            )
        else:
            candidates = (
                (os.path.join(root, relpath), os.path.normpath(os.path.join(archive_dir, relpath)))
                for relpath in sorted(git_files)
            )
        for path, dest in candidates:
            if os.path.realpath(path) in exclude:
                continue
            if os.path.isdir(path) and not os.path.islink(path):
                # a directory in git's listing is a submodule or nested repository
                logger.warning(
                    _(
                        "Skipping git submodule or nested repository for the source archive:"
                        " --add-all does not include its files; add specific files with --add",
                        path=path,
                    )
                )
            elif not os.path.isfile(path):
                pass  # tracked but since deleted, or a dangling symlink
            elif not path_really_within(path, root):
                logger.warning(
                    _(
                        "Skipping file for the source archive:"
                        " symlink resolves outside the source directory",
                        path=path,
                    )
                )
            else:
                yield path, dest


def _warn_uncovered_refs(
    refs: list[_SourceRelativeRef],
    zip_dir: str,
    archive_dirs: dict[str, str],
    auto_add: bool,
    swept_dirs: set[str],
    logger: logging.Logger,
) -> None:
    # warn about each reference the archive doesn't cover (issue #878: warn, never fail)
    kind = Lint._SourceRelativePathKind
    for ref in refs:
        fields: dict[str, Any] = {"reference": ref.description}
        if ref.kind is None:
            message = "Cannot determine statically which file this path expression names; " + (
                "--add-all includes it only if the path is in a WDL source directory and not"
                " ignored by git"
                if os.path.realpath(ref.source_dir) in swept_dirs
                else "if it's a source-relative path, include the file with --additional"
            )
        elif ref.kind == kind.OK:
            dest = _source_relative_dest(ref, archive_dirs)
            if dest is not None and _staged_covers(ref, dest, zip_dir):
                continue
            message = (
                "Source-relative path is not included in the archive; add it with --additional"
            )
            if not auto_add:
                message += " (automatic inclusion disabled)"
            fields.update(path=ref.literal, dest=dest)
        elif ref.kind == kind.MISSING and ref.optional:
            continue
        else:
            message = {
                kind.MISSING: "Source-relative path does not exist",
                kind.ESCAPES: "Source-relative path resolves outside the WDL source directory",
                kind.WRONG_KIND: "Source-relative path is not a "
                + ("directory" if ref.directory else "file"),
                kind.UNAVAILABLE: "Source-relative path is in a WDL document loaded from a URI,"
                " so it cannot be packaged",
            }[ref.kind]
            fields.update(path=ref.literal)
            if ref.kind != kind.UNAVAILABLE:
                fields.update(target=ref.target)
        logger.warning(_(message, **fields))


def _staged_covers(ref: _SourceRelativeRef, dest: str, zip_dir: str) -> bool:
    # whether the staging directory has the reference's target at dest -- for a directory,
    # everything in its tree (directory coverage isn't implied by one staged file)
    try:
        expected = _expand_ref(ref, dest)
    except (Error.InputError, OSError):
        return False
    return all(os.path.lexists(os.path.join(zip_dir, archive_path)) for _, archive_path in expected)


def create_reproducible_archive(zip_dir: str, output_path: str, format: str):
    # write zip/tar archive with internal filenames lexicographically-ordered and all timestamps
    # set to an arbitrary constant
    src_dest_list = [
        (path, path.relative_to(zip_dir))
        for path in pathlib.Path(zip_dir).glob("**/*")  # Finds all files recursively
        if path.is_file()
        or path.is_symlink()  # Symlinks will be included in the zip as normal files
        or path.is_dir()  # directory entries, so empty directories survive archiving
    ]
    # Sort paths by destination
    src_dest_list.sort(key=lambda x: x[1])
    if format == "zip":
        _write_no_mtime_zip(output_path, src_dest_list)
    elif format == "tar":
        _write_no_mtime_tar(output_path, src_dest_list)
    else:
        raise ValueError(f"Unknown format: {format}")
    return output_path


def _write_no_mtime_zip(zip_archive: str, src_dest_list: list[tuple[pathlib.Path, pathlib.Path]]):
    with zipfile.ZipFile(zip_archive, "w") as archive:
        for src, dest in src_dest_list:
            # This always sets the mod time at 1980-1-1
            if src.is_dir() and not src.is_symlink():
                # directory entry: trailing slash + directory attributes, no content
                dest_info = zipfile.ZipInfo(str(dest) + "/")
                dest_info.external_attr = (0o40755 << 16) | 0x10
                archive.writestr(dest_info, b"")
                continue
            dest_info = zipfile.ZipInfo(str(dest))
            with (
                archive.open(dest_info, "w") as archive_file,
                open(src, "rb") as in_file,
            ):
                while True:
                    block = in_file.read(io.DEFAULT_BUFFER_SIZE)
                    if not block:
                        break
                    archive_file.write(block)


def _write_no_mtime_tar(tar_archive: str, src_dest_list: list[tuple[pathlib.Path, pathlib.Path]]):
    with tarfile.TarFile(tar_archive, "w") as archive:
        for src, dest in src_dest_list:
            dest_info = tarfile.TarInfo(str(dest))  # Mtime by default at 0
            if src.is_dir() and not src.is_symlink():
                dest_info.type = tarfile.DIRTYPE
                dest_info.mode = 0o755  # TarInfo's default 0644 lacks directory search permission
                dest_info.size = 0
                archive.addfile(dest_info)
                continue
            dest_info.size = os.stat(src).st_size
            with open(src, "rb") as in_file:
                archive.addfile(dest_info, in_file)


class UnpackedZip(NamedTuple):
    """
    Contextual value of :func:`WDL.Zip.unpack`: absolute paths of source directory, main WDL, and
    default input JSON file (if any). The source directory prefixes the latter paths.
    """

    dir: str
    main_wdl: str
    input_file: str | None


@contextlib.contextmanager
def unpack(archive_fn: str, tempdir_parent: str | None = None) -> Iterator[UnpackedZip]:
    """
    Open a context with the WDL source archive unpacked into a temp directory, yielding
    :class:`UnpackedZip`. The temp directory will be deleted on context exit.

    A path to the MANIFEST.json of an already-unpacked source archive may also be used, or a
    directory containing one. In this case, it is NOT deleted on context exit. ::

        with WDL.Zip.unpack("/path/to/source.zip") as unpacked:
            doc = WDL.load(unpacked.main_wdl)
            ...
    """
    with contextlib.ExitStack() as cleanup:
        # extract zip if needed (also allowing use of already-extracted manifest/dir)
        if os.path.isdir(archive_fn):
            archive_fn = os.path.join(archive_fn, "MANIFEST.json")
        if os.path.basename(archive_fn) == "MANIFEST.json":
            manifest_fn = archive_fn
        else:
            try:
                dn = cleanup.enter_context(
                    tempfile.TemporaryDirectory(prefix="miniwdl_run_zip_", dir=tempdir_parent)
                )
            except OSError as exn:
                msg = "Unable to create temporary directory for unpacking source archive"
                if tempdir_parent:
                    msg += (
                        " under "
                        + tempdir_parent
                        + "; set TMPDIR to a writable directory under file_io.root"
                    )
                raise Error.InputError(msg) from exn
            try:
                shutil.unpack_archive(archive_fn, dn)
            except Exception:
                raise Error.InputError("Unreadable source archive " + archive_fn)
            manifest_fn = os.path.join(dn, "MANIFEST.json")

        try:
            with open(manifest_fn) as infile:
                manifest = json.load(infile)
            assert isinstance(manifest, dict) and isinstance(
                manifest.get("mainWorkflowURL", None), str
            )
        except Exception:
            raise Error.InputError("Missing or invalid MANIFEST.json in " + archive_fn)

        dn = os.path.abspath(os.path.dirname(manifest_fn))
        main_wdl = manifest["mainWorkflowURL"]

        input_file = None
        if (
            isinstance(manifest.get("inputFileURLs", None), list)
            and manifest["inputFileURLs"]
            and isinstance(manifest["inputFileURLs"][0], str)
        ):
            input_file = manifest["inputFileURLs"][0]

        # sanity check
        main_wdl_abs = os.path.join(dn, main_wdl)
        input_file_abs = os.path.join(dn, input_file) if input_file else None
        if not (os.path.isfile(main_wdl_abs) and path_really_within(main_wdl_abs, dn)) or (
            input_file_abs
            and not (os.path.isfile(input_file_abs) and path_really_within(input_file_abs, dn))
        ):
            raise Error.InputError(
                "MANIFEST.json refers to missing or invalid files in " + archive_fn
            )

        yield UnpackedZip(dn, main_wdl_abs, input_file_abs)
