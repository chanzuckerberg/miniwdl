"""
Bundles are miniwdl's file format packaging a WDL source file, imported source files, and optional
JSON default inputs, all into one file for easy transport. The bundle can then be used with
`miniwdl run` and other subcommands. 

The format is merely a UTF-8 YAML structure inlining the original source files, along with their
import layout. Optionally, the YAML text can be compressed with xz and this compressed data encoded
with Base85. This compressed form is useful for passing in environment variables with container
schedulers that limit their size.
"""

import os
from typing import Optional, Any, Dict, List

from WDL.Expr import Boolean
from . import Tree


def build(doc: Tree.Document, input: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    layout = []
    sources = []

    def add_source(doc) -> int:
        existing = next(
            (i for i, src in enumerate(sources) if src["abspath"] == doc.pos.abspath), -1
        )
        if existing >= 0:
            return existing
        sources.append({"abspath": doc.pos.abspath, "source_text": doc.source_text})
        return len(sources) - 1

    def build_layout(doc, ref=None):
        layout = {"ref": ref or os.path.basename(doc.pos.uri), "source": add_source(doc)}
        imports = [build_layout(imp.doc, imp.uri) for imp in doc.imports]
        if imports:
            layout["imports"] = imports
        return layout

    ans = {} if not input else {"input": input}
    ans["layout"] = build_layout(doc)
    ans["sources"] = sources

    return ans


YAML_MAGIC = "#wdl_bundle\n"
COMPACT_MAGIC = "{Wp48"  # Base85 encoding of xz magic bytes


def encode(bundle: Dict[str, Any], compress: Boolean = False) -> str:
    import yaml  # delay heavy import

    # https://stackoverflow.com/a/50519774
    def selective_representer(dumper, data):
        return dumper.represent_scalar(
            "tag:yaml.org,2002:str", data, style="|" if "\n" in data else None
        )

    yaml.add_representer(str, selective_representer)

    ans = YAML_MAGIC + yaml.dump(bundle)

    if not compress:
        return ans

    import lzma
    import base64

    ans = base64.b85encode(lzma.compress(ans.encode("utf-8"))).decode("utf-8")
    assert ans.startswith(COMPACT_MAGIC)
    return ans


def detect(source_text: str) -> Boolean:
    return source_text.startswith(YAML_MAGIC) or source_text.startswith(COMPACT_MAGIC)


def decode(bundle: str) -> Dict[str, Any]:
    bundle = bundle.lstrip()

    if bundle.startswith(COMPACT_MAGIC):
        import lzma
        import base64

        try:
            bundle = lzma.decompress(base64.b85decode(bundle)).decode("utf-8")
        except:
            raise IOError("compressed WDL bundle is corrupt")

    if not bundle.startswith(YAML_MAGIC):
        raise IOError("WDL bundle is corrupt")

    import yaml

    return yaml.safe_load(bundle)
