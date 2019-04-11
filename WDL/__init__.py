"""Toolkit for static analysis of Workflow Description Language (WDL)"""
import os
import errno
import inspect
from typing import List, Optional, Callable
from WDL import _parser, Error, Type, Value, Env, Expr, Tree, Walker, StdLib
from WDL.Tree import Decl, StructTypeDef, Task, Call, Scatter, Conditional, Workflow, Document

SourcePosition = Error.SourcePosition
SourceNode = Error.SourceNode


def load(
    uri: str,
    path: List[str] = [],
    check_quant: bool = True,
    import_uri: Optional[Callable[[str], str]] = None,
    import_max_depth=10,
) -> Document:
    """
    Parse a WDL document given filename/URI, recursively descend into imported documents, then typecheck the tasks and workflow.

    :param path: local filesystem directories to search for imports, in addition to the current working directory

    :param check_quant: set to ``False`` to relax static typechecking of the optional (?) and nonempty (+) type quantifiers. This is discouraged, but may be useful for older WDL workflows which assume less-rigorous static validation of these annotations.

    :param import_uri: to support non-file URI import, supply a function that takes the URI and returns a local file path

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
        import_uri=import_uri,
        import_max_depth=import_max_depth,
    )


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
