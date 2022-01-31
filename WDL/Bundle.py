"""
Bundles are miniwdl's file format packaging a WDL source file, imported source files, and optional
JSON default inputs, all into one file for convenient transfer. The bundle can then be used with
miniwdl commands instead of the original WDL source tree.

The format is merely a UTF-8 YAML structure inlining the original source files, along with metadata
recording how they import each other. Optionally, the YAML text can be compressed with xz and this
compressed data encoded with Base85. This compressed form is useful for passing in environment
variables with container schedulers that limit their size.
"""

import os
from typing import Optional, Any, Dict, List

from . import Tree, Error


def build(
    doc: Tree.Document,
    input: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Bundle the WDL document and all its imports, along with optional input JSON
    """
    layout = []
    sources = []

    def add_source(doc) -> int:
        existing = next(
            (i for i, src in enumerate(sources) if src["abspath"] == doc.pos.abspath), -1
        )
        if existing >= 0:
            return existing
        sources.append({"abspath": doc.pos.abspath, "source_text": doc.source_text.rstrip()})
        return len(sources) - 1

    def build_layout(doc, ref=None):
        if not ref:
            ref = os.path.basename(doc.pos.abspath)
        layout = {"ref": ref, "source": add_source(doc)}
        imports = [build_layout(imp.doc, imp.uri) for imp in doc.imports]
        if imports:
            layout["imports"] = imports
        return layout

    ans: Dict[str, Any] = {}
    if meta:
        ans["meta"] = meta
    if input:
        ans["input"] = input
    ans["layout"] = build_layout(doc)

    # strip common prefix from all local abspaths, making bundle reproducible across machines
    abspaths = [source["abspath"] for source in sources]
    abspaths = [p for p in abspaths if p.startswith("/")]
    if len(abspaths) > 1:
        prefix = os.path.commonprefix(abspaths).rstrip("/")
        for source in sources:
            if source["abspath"].startswith(prefix):
                source["abspath"] = source["abspath"][len(prefix) :]
    else:
        for source in sources:
            if source["abspath"].startswith("/"):
                source["abspath"] = "/" + os.path.basename(source["abspath"])

    ans["sources"] = sources

    return ans


YAML_MAGIC = "#wdl_bundle\n"
COMPRESSED_MAGIC = "{Wp48"  # Base85 encoding of xz magic bytes


def encode(bundle: Dict[str, Any], compress: bool = False) -> str:
    """
    Encode the bundle to text, optionally with compression
    """
    import yaml  # delay heavy import

    # render multi-line strings (e.g. WDL source files) as YAML literals
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

    ans = base64.b85encode(
        lzma.compress(ans.encode("utf-8"), preset=(9 | lzma.PRESET_EXTREME))
    ).decode("utf-8")
    assert ans.startswith(COMPRESSED_MAGIC)
    return ans


def detect(source_text: str) -> bool:
    """
    Detect whether the text is probably a bundle
    """
    source_text = source_text.strip()
    return source_text.startswith(YAML_MAGIC) or source_text.startswith(COMPRESSED_MAGIC)


def decode(bundle: str) -> Dict[str, Any]:
    """
    Decode bundle text (auto-detects YAML or compressed)
    """

    if not detect(bundle):
        raise Error.InputError("input is not a WDL bundle")
    bundle = bundle.strip()

    if bundle.startswith(COMPRESSED_MAGIC):
        import lzma
        import base64

        try:
            bundle = lzma.decompress(base64.b85decode(bundle)).decode("utf-8")
        except:
            raise Error.InputError("compressed WDL bundle is corrupt")

    import yaml

    try:
        return yaml.safe_load(bundle)
    except:
        raise Error.InputError("corrupt WDL bundle")


# arbitrary constant unlikely to collide with filenames
READ_BUNDLE_INPUT = "83Cee747E4BCFF80938eA1056F925d1c24412f0b"


def make_read_source(bundle: Dict[str, Any]):
    """
    Generate a ``read_source`` routine to read from the bundle. To get the bundled input JSON if
    any, call it with the special value ``WDL.Bundle.READ_BUNDLE_INPUT``.
    """
    sources = bundle["sources"]
    imports_idx = {}  # layout imports, indexed by source abspath

    async def read_source_from_bundle(
        uri: str, path: List[str], importer: Optional[Tree.Document]
    ) -> Tree.ReadSourceResult:
        if uri == READ_BUNDLE_INPUT:
            return bundle.get("input", None)

        try:
            if not imports_idx:
                # first use, read "main" WDL
                assert bundle["layout"]["source"] == 0
                assert not importer
                main_abspath = sources[0]["abspath"]
                imports_idx[main_abspath] = bundle["layout"].get("imports", [])
                return Tree.ReadSourceResult(
                    source_text=sources[0]["source_text"], abspath=main_abspath
                )

            # resolve uri in importer layout
            assert importer
            imports = imports_idx[importer.pos.abspath]
            layout = next((imp for imp in imports if imp["ref"] == uri))
            abspath = sources[layout["source"]]["abspath"]
            imports2 = layout.get("imports", [])
            if imports_idx.get(abspath, imports2) != imports2:
                raise StopIteration  # internal inconsistency
            imports_idx[abspath] = imports2

            return Tree.ReadSourceResult(
                source_text=sources[layout["source"]]["source_text"], abspath=abspath
            )
        except (KeyError, IndexError, StopIteration):
            raise Error.InputError("WDL bundle is corrupt or incomplete")

    return read_source_from_bundle
