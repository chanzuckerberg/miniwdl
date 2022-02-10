"""
Routines for packaging a WDL source file, with all imported source files, into a ZIP file.

(miniwdl-specific) The ZIP file may also contain a manifest JSON file with metadata and default
inputs.
"""

import os
import logging
import tempfile
import shutil
from typing import List, Dict, Optional, Any

from . import Tree, Error


def build(
    top_doc: Tree.Document,
    archive: str,
    input: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    extra_files: Optional[List[str]] = None,
    archive_format: str = "zip",
    logger: Optional[logging.Logger] = None,
) -> List[str]:
    import hashlib
    import base64

    main_dir = os.path.dirname(top_doc.pos.abspath).rstrip("/") + "/"  # possibly URI

    wdls = {}
    log = []

    queue = [top_doc]
    while queue:
        a_doc = queue.pop()
        for imported_doc in a_doc.imports:
            queue.append(imported_doc)
        wdls[a_doc.pos.abspath] = a_doc.source_text

    outsiders = set(p for p in wdls.keys() if not p.startswith(main_dir))
    if outsiders:
        assert False  # FIXME
        log.append(
            "One or more source files are imported from locations external to the top-level WDL's directory."
        )
        log.append(
            "Their directory structure will be flattened inside the archive and WDL import statements will be rewritten to match."
        )
        log.append(
            "To keep the ZIP layout more intuitive, ensure all imported source files reside in/under the top-level WDL's directory."
        )

    relpaths = {}
    external_warn = False
    for abspath in wdls.keys():
        if abspath.startswith(main_dir):
            relpaths[abspath] = os.path.relpath(abspath, main_dir)
        else:
            relpaths[abspath] = os.path.join(
                "_external_wdl_",
                base64.b32encode(
                    hashlib.shake_128(wdls[abspath].source_text.encode("utf-8")).digest(10)
                ).decode(),
                os.path.basename(abspath),
            )
            external_warn = True

    if external_warn:
        logger.warning(
            "One or more source files are imported from locations external to the top-level WDL's directory."
        )
        logger.warning(
            "Their directory structure will be flattened inside the archive and WDL import statements will be rewritten to match."
        )
        logger.warning(
            "To keep the archive layout more intuitive, ensure all imported source files reside in/under the top-level WDL's directory."
        )

    # store all outsiders in a flat _outside_wdl/name.CRC32.wdl namespace
    # all imports of outsiders, and all imports within outsiders, must be rewritten
    # CRC32s will refer to original document so that we can perform the rewrites in any order
    # corner cases to think about:
    # - what if an outsider imports an insider?

    # more-radical idea: rename & flatten dir structure of ALL imported WDLs
    # detect if all imports have a common directory prefix
