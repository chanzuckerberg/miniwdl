"""Toolkit for static analysis of Workflow Description Language (WDL)"""
import os
import errno
import inspect
from typing import List, Optional, Callable, Dict, Any, Awaitable
from . import _util, _parser, Error, Type, Value, Env, Expr, Tree, Walker, Lint, StdLib
from .Tree import (
    Decl,
    StructTypeDef,
    Task,
    Call,
    Scatter,
    Conditional,
    Workflow,
    Document,
    WorkflowNode,
    WorkflowSection,
)
from . import runtime

SourcePosition = Error.SourcePosition
SourceNode = Error.SourceNode


def load(
    uri: str,
    path: Optional[List[str]] = None,
    check_quant: bool = True,
    read_source: Optional[Callable[[str, List[str], Optional[str]], Awaitable[str]]] = None,
    import_max_depth: int = 10,
) -> Document:
    """
    Parse a WDL document given filename/URI, recursively descend into imported documents, then typecheck the tasks and workflow.

    :param path: local filesystem directories to search for imports, in addition to the current working directory

    :param check_quant: set to ``False`` to relax static typechecking of the optional (?) and nonempty (+) type quantifiers. This is discouraged, but may be useful for older WDL workflows which assume less-rigorous static validation of these annotations.

    :param read_source: async routine to read the WDL source code from filename/URI; see :func:`read_source_default` below for details

    :param import_max_depth: to prevent recursive import infinite loops, fail when there are too many import nesting levels (default 10)

    :raises WDL.Error.SyntaxError: when the document is syntactically invalid under the WDL grammar
    :raises WDL.Error.ValidationError: when the document is syntactically OK, but fails typechecking or other static validity checks
    :raises WDL.Error.MultipleValidationErrors: when multiple validation errors are detected in one pass, listed in the ``exceptions`` attribute
    :raises WDL.Error.ImportError: when an imported sub-document can't be loaded; the ``__cause__`` attribute has the specific error
    """
    return Tree.load(
        uri,
        path=path,
        check_quant=check_quant,
        read_source=read_source,
        import_max_depth=import_max_depth,
    )


async def load_async(
    uri: str,
    path: Optional[List[str]] = None,
    check_quant: bool = True,
    read_source: Optional[Callable[[str, List[str], Optional[str]], Awaitable[str]]] = None,
    import_max_depth: int = 10,
) -> Document:
    """
    Async version of :func:`load`, with all the same arguments
    """
    return await Tree.load_async(
        uri,
        path=path,
        check_quant=check_quant,
        read_source=read_source,
        import_max_depth=import_max_depth,
    )


async def read_source_default(uri: str, path: List[str], importer_uri: Optional[str]) -> str:
    """
    Default async routine for the ``read_source`` parameter to :func:`load` and :func:`load_async`,
    which they use to read the desired WDL document and its imports. This default routine handles
    local files only, supplying the search path logic to resolve relative filenames; it fails with
    network URIs.

    :param uri: Filename/URI to read
    :param path: Local directiores to search for relative filename imports. The routine may mutate
                 this list to control the search path for documents imported from the current one.
    :param importer_uri: Filename/URI of the importing document, if any
    :returns: WDL source code string

    Callers may wish to override ``read_source`` with logic to download source code from network
    URIs, and for local filenames fall back to ``return await WDL.read_source_default(...)``.

    Note: the synchronous :func:`load` merely calls :func:`load_async` on the current
    ``asyncio.get_event_loop()`` and awaits the result.
    """
    return await Tree.read_source_default(uri, path, importer_uri)


def parse_document(txt: str, version: Optional[str] = None, uri: str = "") -> Document:
    """
    Parse WDL document text into an abstract syntax tree. Doesn't descend into
    imported documents nor typecheck the AST.

    :param version: Override the WDL language version, such as "1.0" or "draft-2". (By default, detects from the "version" string at the beginning of the document, per the WDL spec.)

    :param uri: filename/URI for error reporting (not otherwise used)
    """
    return _parser.parse_document(txt, version, uri)


def parse_expr(txt: str, version: Optional[str] = None) -> Expr.Base:
    """
    Parse an isolated WDL expression text into an abstract syntax tree
    """
    return _parser.parse_expr(txt, version)


def parse_tasks(txt: str, version: Optional[str] = None) -> List[Task]:
    return _parser.parse_tasks(txt, version)


def values_from_json(
    values_json: Dict[str, Any],
    available: Env.Decls,
    required: Optional[Env.Decls] = None,
    namespace: Optional[List[str]] = None,
) -> Env.Values:
    """
    Given a dict parsed from Cromwell-style JSON and the available input (or
    output) declarations of a task or workflow, create a ``WDL.Env.Values``.

    :param required: raise an error if any of these required inputs aren't
                     present
    :param namespace: expect each key to start with this namespace prefixed to
                      the input/output names (e.g. the workflow name)
    """
    ans = []
    for key in values_json:
        fqn = key.split(".")
        if not fqn or [name for name in fqn if not name]:
            raise Error.InputError("invalid key in JSON: " + key)
        if namespace and len(fqn) > len(namespace):
            fqn = fqn[len(namespace) :]
        try:
            ty = Env.resolve(available, fqn[:-1], fqn[-1]).type
        except KeyError:
            raise Error.InputError("unknown input/output: " + key) from None
        v = Value.from_json(ty, values_json[key])
        ans = Env.bind(ans, fqn[:-1], fqn[-1], v)
    if required:
        missing = Env.subtract(required, ans)
        if missing:
            raise Error.InputError(
                "missing required inputs/outputs: " + ", ".join(values_to_json(missing))
            )
    return ans


def values_to_json(values_env: Env.Values, namespace: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Convert a ``WDL.Env.Values`` to a dict which ``json.dumps`` to
    Cromwell-style JSON.

    :param namespace: prefix this namespace to each key (e.g. workflow name)
    """
    # also can be used on Env.Decls or Env.Types, then the right-hand side of
    # each entry will be the type string.
    namespace = namespace or []
    ans = {}
    for item in reversed(values_env):
        if isinstance(item, Env.Binding):
            v = item.rhs
            if isinstance(v, Value.Base):
                j = v.json
            elif isinstance(item.rhs, Tree.Decl):
                j = str(item.rhs.type)
            else:
                assert isinstance(item.rhs, Type.Base)
                j = str(item.rhs)
            ans[".".join(namespace + [item.name])] = j
        elif isinstance(item, Env.Namespace):
            for k, v in values_to_json(
                item.bindings, namespace=(namespace + [item.namespace])
            ).items():
                ans[k] = v
        else:
            assert False
    return ans
