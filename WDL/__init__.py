"""Toolkit for static analysis of Workflow Description Language (WDL)"""
import os, errno
import lark
import inspect
import WDL._parser
import WDL.Expr, WDL.Document, WDL.Walker
import WDL.StdLib # NEEDED to initialize standard library
from typing import List

def parse_expr(txt : str) -> WDL.Expr.Base:
    """
    Parse an isolated WDL expression text into an abstract syntax tree
    """
    return WDL._parser.parse_expr(txt)

def parse_tasks(txt : str) -> List[WDL.Document.Task]:
    return WDL._parser.parse_tasks(txt)

def parse_document(txt : str, uri : str = '') -> WDL.Document.Document:
    """
    Parse WDL document text into an abstract syntax tree. Doesn't descend into
    imported documents nor typecheck the AST.

    :param uri: filename/URI for error reporting (not otherwise used)
    """
    return WDL._parser.parse_document(txt, uri)

def load(uri : str, path : List[str] = []) -> WDL.Document.Document:
    """
    Parse a WDL document given filename/URI, recursively descend into imported documents, then typecheck the tasks and workflow.

    :param path: local filesystem directories to search for imports, in addition to the current working directory
    """
    return WDL.Document.load(uri, path)
