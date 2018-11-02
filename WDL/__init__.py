"""Toolkit for static analysis of Workflow Description Language (WDL)"""
import os, errno
import lark
import inspect
from WDL import _parser, Error, Type, Value, Env, Expr, Document, Walker, StdLib
from typing import List

SourcePosition = Error.SourcePosition
SourceNode = Error.SourceNode

def parse_expr(txt : str) -> Expr.Base:
    """
    Parse an isolated WDL expression text into an abstract syntax tree
    """
    return _parser.parse_expr(txt)

def parse_tasks(txt : str) -> List[Document.Task]:
    return _parser.parse_tasks(txt)

def parse_document(txt : str, uri : str = '') -> Document.Document:
    """
    Parse WDL document text into an abstract syntax tree. Doesn't descend into
    imported documents nor typecheck the AST.

    :param uri: filename/URI for error reporting (not otherwise used)
    """
    return _parser.parse_document(txt, uri)

def load(uri : str, path : List[str] = []) -> Document.Document:
    """
    Parse a WDL document given filename/URI, recursively descend into imported documents, then typecheck the tasks and workflow.

    :param path: local filesystem directories to search for imports, in addition to the current working directory
    """
    return Document.load(uri, path)
