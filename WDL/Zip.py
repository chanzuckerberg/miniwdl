"""
Routines for packaging a WDL source file, with all imported source files, into a ZIP file.

(miniwdl-specific) The ZIP file may also contain a manifest JSON file with metadata and default
inputs.
"""

import os
from typing import List, Dict, Optional, Any

from . import Tree, Error

def build(
    top_doc: Tree.Document,
    filename: str,
    input: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    extra_files: Optional[List[str]] = None,
) -> List[str]:
    import zipfile
    
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
        log.append("One or more source files are imported from outside the top-level WDL's directory (or by URI).")
        log.append("To accommodate this, the directory structure will be flattened inside the ZIP file and WDL import statements will be rewritten to match.")
        log.append("To keep the ZIP layout more intuitive, ensure all imported source files reside in/under the top-level WDL's directory.")

    # store all outsiders in a flat _outside_wdl/name.CRC32.wdl namespace
    # all imports of outsiders, and all imports within outsiders, must be rewritten
    # CRC32s will refer to original document so that we can perform the rewrites in any order
    # corner cases to think about:
    # - what if an outsider imports an insider?

    # more-radical idea: rename & flatten dir structure of ALL imported WDLs
    # detect if all imports have a common directory prefix