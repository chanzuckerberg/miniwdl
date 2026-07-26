"""
Routines for packaging a WDL source file, with all imported source files, into a ZIP file.

*New in v1.5.0*
"""

import io
import os
import json
import glob
import pathlib
import shutil
import logging
import tarfile
import tempfile
import contextlib
import zipfile
from typing import List, Dict, Optional, Any, Iterator, NamedTuple, Tuple, Set

from . import Tree, Error
from ._util import path_really_within


def build(
    top_doc: Tree.Document,
    archive: str,
    logger: logging.Logger,
    inputs: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    archive_format: str = "zip",
    additional_files: Optional[List[str]] = None,
):
    """
    Generate zip archive of the WDL document, all its imports, optional default inputs, and a
    generated manifest JSON.

    If imports are drawn from outside the main WDL's directory (or by URI), they'll be stored in a
    special subdirectory and import statements will be rewritten to match.
    """

    with contextlib.ExitStack() as cleanup:
        # write WDL source code to temp directory
        dir_to_zip, zip_paths, wdls = _build_source_dir(cleanup, top_doc, logger)

        # add MANIFEST.json; schema roughly following Amazon Genomics CLI's:
        #  https://aws.github.io/amazon-genomics-cli/docs/concepts/workflows/#multi-file-workflows
        manifest: Dict[str, Any] = {"mainWorkflowURL": os.path.basename(top_doc.pos.abspath)}
        if meta:
            manifest["meta"] = meta
        if inputs:
            manifest["inputFileURLs"] = ["default_input.json"]
            with open(os.path.join(dir_to_zip, "default_input.json"), "w") as inputs_file:
                json.dump(inputs, inputs_file, indent=2)
        with open(os.path.join(dir_to_zip, "MANIFEST.json"), "w") as manifest_file:
            json.dump(manifest, manifest_file, indent=2)
        logger.debug("manifest = " + json.dumps(manifest))
        if additional_files:
            add_additional_files(dir_to_zip, additional_files, zip_paths, wdls, logger)

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
) -> Tuple[str, Dict[str, str], Dict[str, Tree.Document]]:
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
    assert sorted(list(zip_paths.keys())) == sorted(list(wdls.keys()))
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
    main_dir: str, wdls: Dict[str, Tree.Document], logger: logging.Logger
) -> Dict[str, str]:
    # compute the path inside the archive at which to store each document

    ans = {}
    outside_warn = False
    for abspath in wdls.keys():
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
    doc: Tree.Document, zip_paths: Dict[str, str], logger: logging.Logger
) -> List[str]:
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
    additional_files: List[str],
    zip_paths: Dict[str, str],
    wdls: Dict[str, Tree.Document],
    logger: logging.Logger,
) -> None:
    """
    Add files/directories to an archive staging directory, preserving paths relative to WDL source
    directories represented in ``zip_paths``.

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

    copied: Dict[str, str] = {}
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


def _additional_source_dirs(
    zip_paths: Dict[str, str], wdls: Dict[str, Tree.Document]
) -> List[Tuple[str, str]]:
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
    ans: Dict[str, str] = {}
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
    path: str, source_dirs: List[Tuple[str, str]]
) -> Iterator[Tuple[str, str]]:
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
        visited_dirs: Set[str] = set()
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


def _additional_dest(path: str, source_dirs: List[Tuple[str, str]]) -> str:
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


def create_reproducible_archive(zip_dir: str, output_path: str, format: str):
    # write zip/tar archive with internal filenames lexicographically-ordered and all timestamps
    # set to an arbitrary constant
    src_dest_list = [
        (path, path.relative_to(zip_dir))
        for path in pathlib.Path(zip_dir).glob("**/*")  # Finds all files recursively
        if path.is_file()
        or path.is_symlink()  # Symlinks will be included in the zip as normal files
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


def _write_no_mtime_zip(zip_archive: str, src_dest_list: List[Tuple[pathlib.Path, pathlib.Path]]):
    with zipfile.ZipFile(zip_archive, "w") as archive:
        for src, dest in src_dest_list:
            # This always sets the mod time at 1980-1-1
            dest_info = zipfile.ZipInfo(str(dest))
            with archive.open(dest_info, "w") as archive_file:
                with open(src, "rb") as in_file:
                    while True:
                        block = in_file.read(io.DEFAULT_BUFFER_SIZE)
                        if not block:
                            break
                        archive_file.write(block)


def _write_no_mtime_tar(tar_archive: str, src_dest_list: List[Tuple[pathlib.Path, pathlib.Path]]):
    with tarfile.TarFile(tar_archive, "w") as archive:
        for src, dest in src_dest_list:
            dest_info = tarfile.TarInfo(str(dest))  # Mtime by default at 0
            dest_info.size = os.stat(src).st_size
            with open(src, "rb") as in_file:
                archive.addfile(dest_info, in_file)


UnpackedZip = NamedTuple(
    "UnpackedZip", [("dir", str), ("main_wdl", str), ("input_file", Optional[str])]
)
"""
Contextual value of `WDL.Zip.unpack()`: absolute paths of source directory, main WDL, and default
input JSON file (if any). The source directory prefixes the latter paths.
"""


@contextlib.contextmanager
def unpack(archive_fn: str, tempdir_parent: Optional[str] = None) -> Iterator[UnpackedZip]:
    """
    Open a context with the WDL source archive unpacked into a temp directory, yielding
    `UnpackedZip`. The temp directory will be deleted on context exit.

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
