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
    auto_include: bool = True,
    hoover: bool = False,
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
    :param auto_include: automatically include files/directories referenced by literal WDL 1.2
                         source-relative File/Directory defaults and call inputs found in the
                         source code; warn about any source-relative references that can't be
                         covered statically (disable to only warn about the literal references)
    :param hoover: also include all other files under each WDL source directory, except those
                   ignored by ``.gitignore`` files within the source tree
    """

    with contextlib.ExitStack() as cleanup:
        # write WDL source code to temp directory
        dir_to_zip, zip_paths, wdls = _build_source_dir(cleanup, top_doc, logger)

        # Track what actually lands at each archive path, and from which source file, so
        # coverage of source-relative references can be verified against the archive's real
        # contents instead of what was merely requested. Generated files (the manifest and
        # the default inputs JSON) are recorded with source None: they're reserved names
        # that no user file may silently take over, nor be taken over by.
        staged: dict[str, str | None] = {
            zip_path: os.path.realpath(abspath) for abspath, zip_path in zip_paths.items()
        }

        # add MANIFEST.json; schema roughly following Amazon Genomics CLI's:
        #  https://aws.github.io/amazon-genomics-cli/docs/concepts/workflows/#multi-file-workflows
        manifest: dict[str, Any] = {"mainWorkflowURL": os.path.basename(top_doc.pos.abspath)}
        if meta:
            manifest["meta"] = meta
        if inputs:
            manifest["inputFileURLs"] = ["default_input.json"]
            with open(os.path.join(dir_to_zip, "default_input.json"), "w") as inputs_file:
                json.dump(inputs, inputs_file, indent=2)
            staged["default_input.json"] = None
        with open(os.path.join(dir_to_zip, "MANIFEST.json"), "w") as manifest_file:
            json.dump(manifest, manifest_file, indent=2)
        staged["MANIFEST.json"] = None
        logger.debug("manifest = " + json.dumps(manifest))
        if additional_files:
            staged.update(
                add_additional_files(dir_to_zip, additional_files, zip_paths, wdls, logger)
            )

        # Scan declarations and call inputs for WDL 1.2 source-relative File/Directory
        # references (issue #878): auto-include the ones with provable literal paths, and
        # warn about any the archive won't cover. Automatic inclusion never fails the
        # archive; --additional remains the strict, explicit mechanism.
        refs = _scan_source_relative_refs(wdls)
        if refs or hoover:
            try:
                source_dirs = _additional_source_dirs(zip_paths, wdls)
            except Error.InputError as exn:
                logger.warning(_("Skipping automatic file inclusion", reason=str(exn)))
                source_dirs = []
            archive_dirs = dict(source_dirs)
            include_pairs: list[tuple[str, str, _SourceRelativeRef | None]] = []
            if auto_include:
                for ref in refs:
                    if ref.kind != Lint._SourceRelativePathKind.OK or not ref.target:
                        continue
                    dest = _source_relative_dest(ref, archive_dirs)
                    if dest is None:
                        continue
                    try:
                        # materialize the expansion before extending: if it fails partway,
                        # the whole reference is skipped, not partially included
                        expanded = list(_expand_scanned_target(ref.target, dest, ref))
                    except (Error.InputError, OSError) as exn:
                        logger.warning(
                            _(
                                "Skipping source-relative path for the source archive",
                                path=ref.literal,
                                reference=ref.description,
                                reason=str(exn),
                            )
                        )
                    else:
                        include_pairs.extend((src, dst, ref) for src, dst in expanded)
            swept_dirs: set[str] = set()
            if hoover and source_dirs:
                swept_dirs = {os.path.realpath(source_dir) for source_dir, _ in source_dirs}
                include_pairs.extend(
                    (src, dst, None)
                    for src, dst in _hoover_source_files(
                        source_dirs,
                        {os.path.realpath(os.path.abspath(archive))},
                        logger,
                    )
                )
            collided: set[str] = set()
            if include_pairs and source_dirs:
                _include_scanned_paths(dir_to_zip, include_pairs, staged, collided, logger)
            _warn_uncovered_source_relative_refs(
                refs,
                dir_to_zip,
                archive_dirs,
                staged,
                collided,
                auto_include,
                swept_dirs,
                logger,
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
    One WDL 1.2 source-relative File/Directory reference found by scanning declarations and
    call inputs (see :func:`_scan_source_relative_refs`).
    """

    description: str  # human-readable location of the reference, for warnings
    source_dir: str  # directory of the WDL document containing the reference
    literal: str | None  # the literal path, if the reference is a string literal
    target: str | None  # absolute path the literal resolves to, if classifiable
    kind: Lint._SourceRelativePathKind | None  # literal classification; None if not a literal
    directory: bool  # the reference should name a directory (rather than a file)
    optional: bool  # a missing target is acceptable (optional File/Directory)


def _scan_source_relative_refs(wdls: dict[str, Tree.Document]) -> list[_SourceRelativeRef]:
    """
    Scan all documents in the import closure for WDL 1.2 source-relative File/Directory
    references in declarations and call inputs, classifying string literals against the
    containing document's source directory with the linter's classifier
    (:func:`WDL.Lint._source_relative_path_kind`). StdLib path arguments (``read_lines`` etc.)
    and output sections are out of scope.

    Only literals can be classified exactly; a non-literal String expression in a
    File/Directory position yields a reference with kind None, since the path it computes
    can't be known statically.
    """
    refs: list[_SourceRelativeRef] = []
    for doc in wdls.values():
        if wdl_version_geq(doc.effective_wdl_version, WDLVersion.V1_2):
            _scan_document_refs(doc, refs)
    return refs


def _scan_document_refs(doc: Tree.Document, refs: list[_SourceRelativeRef]) -> None:
    source_dir = doc.source_dir
    doc_name = os.path.basename(doc.pos.abspath)
    for task in doc.tasks:
        for decl in list(task.inputs or []) + list(task.postinputs):
            _scan_typed_expr_refs(
                decl.type,
                decl.expr,
                source_dir,
                f"task '{task.name}' declaration '{decl.name}' in {doc_name}",
                refs,
            )
    if doc.workflow:
        for decl in doc.workflow.inputs or []:
            _scan_typed_expr_refs(
                decl.type,
                decl.expr,
                source_dir,
                f"workflow '{doc.workflow.name}' input '{decl.name}' in {doc_name}",
                refs,
            )
        _scan_workflow_body_refs(doc.workflow.body, source_dir, doc_name, refs)


def _scan_workflow_body_refs(
    elements: list[Tree.WorkflowNode],
    source_dir: str,
    doc_name: str,
    refs: list[_SourceRelativeRef],
) -> None:
    for elt in elements:
        if isinstance(elt, Tree.Decl):
            _scan_typed_expr_refs(
                elt.type, elt.expr, source_dir, f"declaration '{elt.name}' in {doc_name}", refs
            )
        elif isinstance(elt, Tree.Call):
            _scan_call_refs(elt, source_dir, doc_name, refs)
        elif isinstance(elt, (Tree.Scatter, Tree.Conditional)):
            _scan_workflow_body_refs(elt.body, source_dir, doc_name, refs)


def _scan_call_refs(
    call: Tree.Call, source_dir: str, doc_name: str, refs: list[_SourceRelativeRef]
) -> None:
    if call.callee is None:
        return
    for name, expr in call.inputs.items():
        try:
            # the callee's canonical callable-input interface (as in Call.typecheck_input):
            # for a task without an input{} section, this includes its bare declarations
            # (pre-1.0 "implicit" inputs), which callee.inputs alone would miss
            decl = call.callee.available_inputs[name]
        except KeyError:
            continue
        # call input bindings resolve against the *caller's* source directory
        _scan_typed_expr_refs(
            decl.type, expr, source_dir, f"call input '{call.name}.{name}' in {doc_name}", refs
        )


def _scan_typed_expr_refs(
    to_type: Type.Base,
    expr: Expr.Base | None,
    source_dir: str,
    description: str,
    refs: list[_SourceRelativeRef],
) -> None:
    # Mirror the recursive literal walk of WDL.Lint._file_coercion_decl_suspicious, but
    # collect the classification of each File/Directory leaf instead of a boolean verdict.
    if expr is None:
        return
    if isinstance(to_type, (Type.File, Type.Directory)):
        literal = expr.literal
        if isinstance(literal, Value.String):
            if "://" in literal.value:
                return  # downloadable URI, not source-relative
            kind = Lint._source_relative_path_kind(
                source_dir, literal.value, isinstance(to_type, Type.Directory)
            )
            if kind == Lint._SourceRelativePathKind.ABSOLUTE:
                return
            # the path as written, normalized but with symlinks preserved: both the
            # runner's resolution (WDL.runtime._io_helpers.resolve_source_relative_path)
            # and the archive placement key off the literal's own path, never its
            # realpath, so a symlinked file keeps its own name
            target = (
                os.path.abspath(os.path.join(source_dir, literal.value)) if source_dir else None
            )
            refs.append(
                _SourceRelativeRef(
                    description,
                    source_dir,
                    literal.value,
                    target,
                    kind,
                    isinstance(to_type, Type.Directory),
                    to_type.optional,
                )
            )
        elif isinstance(expr.type, (Type.String, Type.Any)):
            # path computed by an expression; not statically knowable. An Any-typed
            # expression (e.g. a StdLib function result coerced to File) is just as
            # unprovable as a String-typed one, and the declaration still deserves the
            # uncovered-reference warning (without scanning the expression's own
            # arguments, which remain out of scope).
            refs.append(
                _SourceRelativeRef(
                    description,
                    source_dir,
                    None,
                    None,
                    None,
                    isinstance(to_type, Type.Directory),
                    to_type.optional,
                )
            )
        return
    if isinstance(to_type, Type.Array) and isinstance(expr, Expr.Array):
        for item in expr.items:
            _scan_typed_expr_refs(to_type.item_type, item, source_dir, description, refs)
        return
    if isinstance(to_type, Type.Map) and isinstance(expr, Expr.Map):
        key_type, value_type = to_type.item_type
        for key, value in expr.items:
            _scan_typed_expr_refs(key_type, key, source_dir, description, refs)
            _scan_typed_expr_refs(value_type, value, source_dir, description, refs)
        return
    if isinstance(to_type, Type.Pair) and isinstance(expr, Expr.Pair):
        _scan_typed_expr_refs(to_type.left_type, expr.left, source_dir, description, refs)
        _scan_typed_expr_refs(to_type.right_type, expr.right, source_dir, description, refs)
        return
    if (
        isinstance(to_type, Type.StructInstance)
        and to_type.members
        and isinstance(expr, Expr.Struct)
    ):
        for name, member in expr.members.items():
            if name in to_type.members:
                _scan_typed_expr_refs(to_type.members[name], member, source_dir, description, refs)
        return
    if isinstance(expr.type, Type.Any) and _type_has_file_positions(to_type):
        # whole value computed with unknown static type (e.g. a StdLib function result);
        # any File/Directory leaves within are unprovable
        refs.append(
            _SourceRelativeRef(description, source_dir, None, None, None, False, to_type.optional)
        )
        return
    if _computed_feeds_file_positions(to_type, expr.type):
        # non-literal compound value whose File/Directory leaves are computed at run time
        refs.append(
            _SourceRelativeRef(description, source_dir, None, None, None, False, to_type.optional)
        )


def _type_has_file_positions(to_type: Type.Base) -> bool:
    """Whether ``to_type`` has File/Directory in any (nested) position."""
    if isinstance(to_type, (Type.File, Type.Directory)):
        return True
    if isinstance(to_type, Type.Array):
        return _type_has_file_positions(to_type.item_type)
    if isinstance(to_type, Type.Map):
        (key_type, value_type) = to_type.item_type
        return _type_has_file_positions(key_type) or _type_has_file_positions(value_type)
    if isinstance(to_type, Type.Pair):
        return _type_has_file_positions(to_type.left_type) or _type_has_file_positions(
            to_type.right_type
        )
    if isinstance(to_type, Type.StructInstance) and to_type.members:
        return any(_type_has_file_positions(member) for member in to_type.members.values())
    return False


def _computed_feeds_file_positions(to_type: Type.Base, expr_type: Type.Base) -> bool:
    """
    Whether ``expr_type`` has String or Any in any position where ``to_type`` has
    File/Directory -- i.e. the expression computes one or more paths at run time, from
    strings or from opaquely-typed values, so they can't be known statically.
    """
    if isinstance(expr_type, Type.Any):
        # an opaquely-typed value in a compound position: unprovable wherever the
        # target type has File/Directory positions within it
        return _type_has_file_positions(to_type)
    if isinstance(to_type, (Type.File, Type.Directory)):
        return isinstance(expr_type, Type.String)
    if isinstance(to_type, Type.Array) and isinstance(expr_type, Type.Array):
        return _computed_feeds_file_positions(to_type.item_type, expr_type.item_type)
    if isinstance(to_type, Type.Map) and isinstance(expr_type, Type.Map):
        (key_type, value_type) = to_type.item_type
        (expr_key_type, expr_value_type) = expr_type.item_type
        return _computed_feeds_file_positions(
            key_type, expr_key_type
        ) or _computed_feeds_file_positions(value_type, expr_value_type)
    if isinstance(to_type, Type.Pair) and isinstance(expr_type, Type.Pair):
        return _computed_feeds_file_positions(
            to_type.left_type, expr_type.left_type
        ) or _computed_feeds_file_positions(to_type.right_type, expr_type.right_type)
    if (
        isinstance(to_type, Type.StructInstance)
        and isinstance(expr_type, Type.StructInstance)
        and to_type.members
        and expr_type.members
    ):
        return any(
            name in expr_type.members
            and _computed_feeds_file_positions(member_type, expr_type.members[name])
            for name, member_type in to_type.members.items()
        )
    if (
        isinstance(to_type, Type.StructInstance)
        and isinstance(expr_type, Type.Object)
        and to_type.members
        and expr_type.members
    ):
        # an Object value coerced to a struct: compare member-wise
        return any(
            name in expr_type.members
            and _computed_feeds_file_positions(member_type, expr_type.members[name])
            for name, member_type in to_type.members.items()
        )
    return False


def _source_relative_dest(ref: _SourceRelativeRef, archive_dirs: dict[str, str]) -> str | None:
    """
    The archive path at which the runner will look for this reference after unpacking: the
    archive directory of the document that *mentions* the reference, joined with the literal
    as written. This mirrors the runner's own resolution
    (``WDL.runtime._io_helpers.resolve_source_relative_path``: a symlink-preserving normpath
    join onto the mentioning document's directory) -- placement must be derived per-reference
    from the mentioning document, not by matching the target file against all source
    directories, and the literal's own path (including any symlinks in it) is preserved.
    """
    if ref.literal is None:
        return None
    archive_dir = archive_dirs.get(ref.source_dir.rstrip("/"))
    if archive_dir is None:
        return None
    return os.path.normpath(os.path.join(archive_dir, ref.literal))


def _expand_scanned_target(
    target: str, dest: str, ref: _SourceRelativeRef
) -> Iterator[tuple[str, str]]:
    """
    Expand one automatically-discovered file/directory into (source, archive path) pairs:
    the target itself, plus every entry of a directory tree, each keeping its path relative
    to the target (so symlinks retain their own names in the archive). Directories are
    yielded as pairs too, so empty directories are staged (and archived) instead of
    vanishing. Entries whose realpath escapes the mentioning document's source directory
    raise :class:`WDL.Error.InputError`, mirroring ``--additional``'s safety posture; the
    caller turns that into a warning.
    """
    if not path_really_within(target, ref.source_dir):
        raise Error.InputError(
            "Source-relative path resolves outside the WDL source directory: " + target
        )
    if not os.path.isdir(target):
        yield target, dest
        return
    yield from _expand_dir(target, dest, ref, frozenset())


def _expand_dir(
    dirpath: str, dest: str, ref: _SourceRelativeRef, ancestors: frozenset[str]
) -> Iterator[tuple[str, str]]:
    """
    Yield (source, archive path) for a directory and, recursively, its contents. Cycle
    protection tracks only the realpaths of the directories *above* this one in the
    recursion: a directory whose realpath is its own ancestor is yielded (so its name is
    staged) but not descended into. Sibling aliases to the same directory are NOT cycles
    -- each pathname is expanded in full, since the runner may resolve either spelling.
    """
    yield dirpath, dest
    real_dirpath = os.path.realpath(dirpath)
    if real_dirpath in ancestors:
        return
    try:
        entries = sorted(os.listdir(dirpath))
    except OSError as exn:
        raise Error.InputError(
            f"Cannot list directory for the source archive: {dirpath}: {exn}"
        ) from exn
    ancestors = ancestors | {real_dirpath}
    for entry in entries:
        entry_path = os.path.join(dirpath, entry)
        _expand_check_within(entry_path, ref)
        if os.path.isdir(entry_path):
            yield from _expand_dir(entry_path, os.path.join(dest, entry), ref, ancestors)
        else:
            yield entry_path, os.path.join(dest, entry)


def _expand_check_within(path: str, ref: _SourceRelativeRef) -> None:
    if not path_really_within(path, ref.source_dir):
        raise Error.InputError("Source-relative path contains unsafe symlink or path: " + path)


def _include_scanned_paths(
    zip_dir: str,
    pairs: list[tuple[str, str, _SourceRelativeRef | None]],
    staged: dict[str, str | None],
    collided: set[str],
    logger: logging.Logger,
) -> None:
    """
    Copy automatically-discovered files (scanner hits, hoover sweep) into the archive staging
    directory at their precomputed archive paths. ``staged`` (archive path -> realpath of the
    source staged there, or None for generated files) is updated with everything actually
    staged, so coverage checks see verified contents rather than requests. A destination
    already staged from the *same* source file is skipped silently; one staged from anything
    else (including the generated MANIFEST.json/default_input.json) is a collision: warn, and
    record it in ``collided`` rather than silently dropping the payload. Per-source failures
    are warnings with any partial file removed: automatic inclusion never fails the archive
    (issue #878). Only failures writing the final archive itself are fatal, and those happen
    later, in :func:`create_reproducible_archive`.
    """
    for src, dest, ref in pairs:
        dest_path = os.path.join(zip_dir, dest)
        if not path_really_within(dest_path, zip_dir):
            logger.warning(
                _(
                    "Skipping file for the source archive: invalid destination",
                    path=src,
                    dest=dest,
                )
            )
            continue
        if dest in staged:
            previous = staged[dest]
            if previous is not None and previous == os.path.realpath(src):
                continue  # this very file is already staged at the destination
            collided.add(dest)
            logger.warning(
                _(
                    "Skipping file for the source archive: archive path already staged"
                    " from a different source",
                    path=src,
                    dest=dest,
                    reference=(ref.description if ref else None),
                )
            )
            continue
        try:
            if os.path.isdir(src):
                os.makedirs(dest_path, exist_ok=True)
            else:
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                shutil.copyfile(src, dest_path)
        except OSError as exn:
            # remove any partially-copied file so the archive doesn't carry a stub
            with contextlib.suppress(OSError):
                if os.path.isfile(dest_path):
                    os.unlink(dest_path)
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


def _load_gitignore(dirpath: str, logger: logging.Logger) -> Any | None:
    """
    Load one directory's ``.gitignore`` as a pathspec GitIgnoreSpec, if present. A
    ``.gitignore`` that can't be read or parsed is treated as absent, with a warning:
    like the rest of automatic inclusion, ignore-file discovery never fails the archive.
    """
    from pathspec import GitIgnoreSpec

    gitignore_fn = os.path.join(dirpath, ".gitignore")
    if os.path.isfile(gitignore_fn):
        try:
            with open(gitignore_fn) as gitignore_file:
                return GitIgnoreSpec.from_lines(gitignore_file)
        except (OSError, ValueError) as exn:
            logger.warning(
                _(
                    "Skipping unreadable .gitignore for the source archive sweep",
                    path=gitignore_fn,
                    reason=str(exn),
                )
            )
    return None


def _hoover_source_files(
    source_dirs: list[tuple[str, str]], exclude: set[str], logger: logging.Logger
) -> Iterator[tuple[str, str]]:
    """
    Yield (file, archive path) for every file under each WDL source directory, except those
    ignored by ``.gitignore`` files within the tree (pathspec) and ``.git`` directories.
    Only in-tree ``.gitignore`` files are consulted (not global excludes or
    ``.git/info/exclude``), so behavior is the same in git checkouts and plain directory
    trees. Symlinks resolving outside the swept directory are skipped with a warning,
    mirroring ``--additional``'s safety posture.

    Ignore rules are inherited the way git applies them: a swept directory also obeys the
    ``.gitignore`` files of its ancestors within the swept tree (down from the shallowest
    swept ancestor directory), so e.g. a ``*.secret`` rule in the top-level source
    directory's ``.gitignore`` also excludes matching files under an imported document's
    subdirectory swept in the same run.
    """
    roots = [source_dir for source_dir, _archive_dir in source_dirs]
    for source_dir, archive_dir in source_dirs:
        root = source_dir
        # seed the spec stack with .gitignore files of ancestors within the swept tree:
        # every directory from the shallowest swept ancestor of root down to root's parent
        bound = root
        for other in roots:
            if (root == other or root.startswith(other.rstrip("/") + os.sep)) and len(other) < len(
                bound
            ):
                bound = other
        spec_stack: list[tuple[str, Any]] = []
        ancestor = os.path.dirname(root)
        chain: list[str] = []
        while ancestor == bound or ancestor.startswith(bound.rstrip("/") + os.sep):
            chain.append(ancestor)
            if ancestor == bound:
                break
            ancestor = os.path.dirname(ancestor)
        pruned = False
        for base in reversed(chain):
            if spec_stack and _gitignored(spec_stack, base, is_dir=True):
                # git prunes here: an ancestor directory excluded by a shallower spec
                # is never descended into, so its own .gitignore (and anything deeper)
                # is never read -- and the exclusion is final for the whole subtree:
                # no negation can reopen a directory whose parent stays excluded
                pruned = True
                break
            spec = _load_gitignore(base, logger)
            if spec is not None:
                spec_stack.append((base, spec))
        if pruned or (spec_stack and _gitignored(spec_stack, root, is_dir=True)):
            # git never descends into an excluded directory, so this directory's own
            # .gitignore (and any negations in it) is never consulted either
            continue

        def _walk_error(exn: OSError) -> None:
            logger.warning(
                _(
                    "Skipping unreadable directory for the source archive sweep",
                    reason=str(exn),
                )
            )

        visited_dirs: set[str] = set()
        for dirpath, dirnames, filenames in os.walk(root, followlinks=True, onerror=_walk_error):
            real_dirpath = os.path.realpath(dirpath)
            if real_dirpath in visited_dirs:
                dirnames[:] = []
                continue
            visited_dirs.add(real_dirpath)
            # drop .gitignore specs from subtrees the walk has moved out of (the seeded
            # ancestor specs stay: every walked directory is beneath them)
            spec_stack = [
                (base, spec)
                for base, spec in spec_stack
                if dirpath == base or dirpath.startswith(base + os.sep)
            ]
            spec = _load_gitignore(dirpath, logger)
            if spec is not None:
                spec_stack.append((dirpath, spec))
            keep_dirs = []
            for dirname in dirnames:
                dirname_path = os.path.join(dirpath, dirname)
                if dirname == ".git":
                    continue
                if not path_really_within(dirname_path, root):
                    logger.warning(
                        _(
                            "Skipping directory for the source archive:"
                            " symlink resolves outside the swept directory",
                            path=dirname_path,
                            root=root,
                        )
                    )
                    continue
                if _gitignored(spec_stack, dirname_path, is_dir=True):
                    continue
                keep_dirs.append(dirname)
            dirnames[:] = keep_dirs
            for filename in sorted(filenames):
                file_path = os.path.join(dirpath, filename)
                if os.path.realpath(file_path) in exclude:
                    continue
                if not path_really_within(file_path, root):
                    logger.warning(
                        _(
                            "Skipping file for the source archive:"
                            " symlink resolves outside the swept directory",
                            path=file_path,
                            root=root,
                        )
                    )
                    continue
                if _gitignored(spec_stack, file_path, is_dir=False):
                    continue
                dest = os.path.normpath(os.path.join(archive_dir, os.path.relpath(file_path, root)))
                yield file_path, dest


def _gitignored(spec_stack: list[tuple[str, Any]], abspath: str, is_dir: bool) -> bool:
    """
    Decide whether ``abspath`` is ignored by the stacked ``.gitignore`` specs (ordered
    shallowest first). The deepest spec with an opinion wins, per git's precedence rules;
    negations within each file are handled by pathspec.
    """
    ignored = False
    for base, spec in spec_stack:
        if not (abspath == base or abspath.startswith(base + os.sep)):
            continue
        relpath = os.path.relpath(abspath, base).replace(os.sep, "/")
        if is_dir:
            relpath += "/"
        result = spec.check_file(relpath)
        if result.include is not None:
            ignored = result.include
    return ignored


def _warn_uncovered_source_relative_refs(
    refs: list[_SourceRelativeRef],
    zip_dir: str,
    archive_dirs: dict[str, str],
    staged: dict[str, str | None],
    collided: set[str],
    auto_include: bool,
    swept_dirs: set[str],
    logger: logging.Logger,
) -> None:
    """
    Warn about each scanned source-relative reference the archive won't cover (issue #878:
    warn, never fail). A reference counts as covered only by verified contents: the file
    must actually be staged, from the very source it refers to, at the archive path the
    runner will resolve it to (see :func:`_source_relative_ref_covered`) -- not merely
    have been requested for inclusion.
    """
    kind = Lint._SourceRelativePathKind
    for ref in refs:
        if ref.kind is None:
            # path computed by an expression; statically unknowable either way
            if os.path.realpath(ref.source_dir) not in swept_dirs:
                logger.warning(
                    _(
                        "Cannot determine statically the file for the source-relative"
                        " path expression; include it with --additional if it refers"
                        " to a file in the WDL source tree",
                        reference=ref.description,
                    )
                )
            else:
                # the hoover sweep isn't proof of coverage for a computed path: it
                # covers the path only if it happens to lie in the swept tree and
                # isn't excluded by .gitignore, neither of which can be checked
                # statically -- so the warning stands, qualified
                logger.warning(
                    _(
                        "Cannot determine statically the file for the source-relative"
                        " path expression; the --hoover sweep covers it only if the"
                        " computed path lies in a swept source directory and is not"
                        " excluded by .gitignore",
                        reference=ref.description,
                    )
                )
        elif ref.kind == kind.OK:
            dest = _source_relative_dest(ref, archive_dirs)
            if dest is not None and dest in collided:
                continue  # the collision warning already covered this reference
            if not _source_relative_ref_covered(ref, zip_dir, archive_dirs, staged):
                message = (
                    "Source-relative path is not included in the archive; add it with --additional"
                )
                if not auto_include:
                    message += " (automatic inclusion disabled)"
                logger.warning(_(message, path=ref.literal, reference=ref.description, dest=dest))
        elif ref.kind == kind.MISSING:
            if not ref.optional:
                logger.warning(
                    _(
                        "Source-relative path does not exist",
                        path=ref.literal,
                        reference=ref.description,
                        target=ref.target,
                    )
                )
        elif ref.kind == kind.ESCAPES:
            logger.warning(
                _(
                    "Source-relative path resolves outside the WDL source directory",
                    path=ref.literal,
                    reference=ref.description,
                    target=ref.target,
                )
            )
        elif ref.kind == kind.WRONG_KIND:
            logger.warning(
                _(
                    "Source-relative path is not a " + ("directory" if ref.directory else "file"),
                    path=ref.literal,
                    reference=ref.description,
                    target=ref.target,
                )
            )
        elif ref.kind == kind.UNAVAILABLE:
            logger.warning(
                _(
                    "Source-relative path is in a WDL document loaded from a URI,"
                    " so it cannot be packaged",
                    path=ref.literal,
                    reference=ref.description,
                )
            )


def _source_relative_ref_covered(
    ref: _SourceRelativeRef,
    zip_dir: str,
    archive_dirs: dict[str, str],
    staged: dict[str, str | None],
) -> bool:
    """
    Whether the archive verifiably covers this reference. For a file: the entry at the
    archive path the runner will resolve the reference to was staged from the very file
    the reference names (an entry staged from a different source -- or a generated file
    such as MANIFEST.json -- doesn't count, even if the path matches). For a directory:
    the directory itself must exist in the staging tree AND every file under the target
    directory must be staged, from its corresponding source, at its corresponding
    archive path -- one staged descendant is not proof of whole-directory coverage.
    """
    if ref.target is None:
        return False
    dest = _source_relative_dest(ref, archive_dirs)
    if dest is None:
        return False
    target_real = os.path.realpath(ref.target)
    if not ref.directory:
        return staged.get(dest) is not None and staged.get(dest) == target_real
    try:
        expected = list(_expand_scanned_target(ref.target, dest, ref))
    except Error.InputError:
        return False
    # every expected entry must be verifiably present: files via the staged map's
    # provenance, directories (including empty ones, which leave no other trace) via
    # the staging tree itself
    return all(
        os.path.isdir(os.path.join(zip_dir, dst))
        if os.path.isdir(src)
        else staged.get(dst) == os.path.realpath(src)
        for src, dst in expected
    )


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
                dest_info.external_attr = (0o40775 << 16) | 0x10
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
