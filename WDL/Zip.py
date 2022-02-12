"""
Routines for packaging a WDL source file, with all imported source files, into a ZIP file.
"""

import os
import json
import shutil
import logging
import tempfile
import contextlib
from typing import List, Dict, Optional, Any, Tuple

from . import Tree, Error
from ._util import path_really_within


def build(
    top_doc: Tree.Document,
    archive: str,
    logger: logging.Logger,
    inputs: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    archive_format: str = "zip",
):
    "Generate zip archive of the WDL document and all its imports"

    with contextlib.ExitStack() as cleanup:
        # write WDL source code to temp directory
        dir_to_zip = build_source_dir(cleanup, top_doc, logger)

        # add MANIFEST.json; schema roughly following Amazon Genomics CLI's:
        #  https://aws.github.io/amazon-genomics-cli/docs/concepts/workflows/#multi-file-workflows
        manifest = {"mainWorkflowURL": os.path.basename(top_doc.pos.abspath)}
        if meta:
            manifest["meta"] = meta
        if inputs:
            manifest["inputFileURLs"] = ["default_inputs.json"]
            with open(os.path.join(dir_to_zip, "default_inputs.json"), "w") as inputs_file:
                json.dump(inputs, inputs_file, indent=2)
        with open(os.path.join(dir_to_zip, "MANIFEST.json"), "w") as manifest_file:
            json.dump(manifest, manifest_file, indent=2)
        logger.debug("manifest = " + json.dumps(manifest))

        # zip the temp directory (into another temp directory)
        tmp_zip = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_"))
        logger.info(f"archiving {dir_to_zip}")
        tmp_zip = shutil.make_archive(
            os.path.join(tmp_zip, os.path.basename(top_doc.pos.abspath)),
            archive_format,
            root_dir=dir_to_zip,
            logger=logger,
        )

        # move zip to final location
        logger.info(f"{archive} <= {tmp_zip}")
        if "/" in archive:
            os.makedirs(os.path.dirname(archive), exist_ok=True)
        os.rename(tmp_zip, archive)


def build_source_dir(
    cleanup: contextlib.ExitStack, top_doc: Tree.Document, logger: logging.Logger
) -> str:
    # directory of main WDL file (possibly URI)
    main_dir = os.path.dirname(top_doc.pos.abspath).rstrip("/") + "/"

    # collect all WDL docs keyed by abspath
    wdls = {}
    queue = [top_doc]
    while queue:
        a_doc = queue.pop()
        for imported_doc in a_doc.imports:
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

    return zip_dir


def build_zip_paths(
    main_dir: str, wdls: Dict[str, Tree.Document], logger: logging.Logger
) -> Dict[str, str]:
    # compute the path inside the archive at which to store each document
    import hashlib
    import base64

    ans = {}
    outside_warn = False
    for abspath in wdls.keys():
        if abspath.startswith(main_dir):
            ans[abspath] = os.path.relpath(abspath, main_dir)
        else:
            # place outside import under __outside_wdl, vaguely reproducing directory structure
            abspath2 = abspath.replace("://", "_")
            prefix = os.path.commonprefix([abspath2, main_dir.replace("://", "_")])
            assert abspath2.startswith(prefix) and prefix.endswith("/")
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
        lo = imp.pos.line - 1
        hi = imp.pos.end_line
        for lineno in range(lo, hi):
            line = source_lines[lineno]
            old_uri = imp.uri
            new_uri = os.path.relpath(
                zip_paths[imp.doc.pos.abspath], os.path.dirname(zip_paths[doc.pos.abspath])
            )
            line2 = line.replace(f'"{old_uri}"', f'"{new_uri}"')
            if line != line2:
                logger.debug(doc.pos.abspath)
                logger.debug("  " + line)
                logger.debug("  => " + line2)
                source_lines[lineno] = line2

    return source_lines


def unpack(
    archive_fn: str, cleanup: Optional[contextlib.ExitStack] = None
) -> Tuple[str, Optional[str]]:
    """
    Extract WDL source archive to a temp directory, returning the full paths to the main WDL and
    the default inputs JSON file, if any. An action to delete the temp directory is added to the
    cleanup ExitStack, if provided.
    """

    # allow use of already-extracted zip
    if os.path.isdir(archive_fn) and os.path.isfile(os.path.join(archive_fn, "MANIFEST.json")):
        archive_fn = os.path.join(archive_fn, "MANIFEST.json")

    # extract zip if needed
    if os.path.basename(archive_fn) == "MANIFEST.json":
        manifest_fn = archive_fn
    else:
        if cleanup:
            dn = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_run_zip_"))
        else:
            dn = tempfile.mkdtemp(prefix="miniwdl_run_zip_")
        try:
            shutil.unpack_archive(archive_fn, dn)
        except:
            raise Error.InputError("Unreadable source archive " + archive_fn)
        manifest_fn = os.path.join(dn, "MANIFEST.json")

    # read manifest
    dn = os.path.dirname(manifest_fn)
    if not os.path.exists(manifest_fn):
        # no manifest; try main.wdl
        main_wdl = os.path.join(dn, "main.wdl")
        if os.path.isfile(main_wdl) and path_really_within(main_wdl, dn):
            return (main_wdl, None)
    try:
        with open(manifest_fn) as infile:
            manifest = json.load(infile)
        assert isinstance(manifest, dict) and isinstance(manifest.get("mainWorkflowURL", None), str)
    except:
        raise Error.InputError("Missing or invalid MANIFEST.json in " + archive_fn)

    dn = os.path.dirname(manifest_fn)
    main_wdl = os.path.join(dn, manifest["mainWorkflowURL"])

    input_file = None
    if (
        isinstance(manifest.get("inputFileURLs", None), list)
        and manifest["inputFileURLs"]
        and isinstance(manifest["inputFileURLs"][0], str)
    ):
        input_file = os.path.join(dn, manifest["inputFileURLs"][0])

    # sanity check
    if not (os.path.isfile(main_wdl) and path_really_within(main_wdl, dn)) or (
        input_file and not (os.path.isfile(input_file) and path_really_within(input_file, dn))
    ):
        raise Error.InputError("MANIFEST.json refers to missing or invalid files in " + archive_fn)

    return main_wdl, input_file
