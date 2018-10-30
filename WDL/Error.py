# pyre-strict
from abc import ABC
from typing import Any, List, Optional, Dict, Callable, NamedTuple, TypeVar
import WDL.Type as T
from WDL.Expr import TVApply, TVIdent

class ParserError(Exception):
    def __init__(self, filename : str) -> None:
        super().__init__(filename)

SourcePosition = NamedTuple("SourcePosition",
                            [('filename',str), ('line',int), ('column',int),
                             ('end_line',int), ('end_column',int)])
"""Source file, line, and column, attached to each AST node"""

class SourceNode:
    """Base class for an AST node, recording the source position"""

    pos : SourcePosition
    """Source position for this AST node"""

    def __init__(self, pos : SourcePosition) -> None:
        self.pos = pos

class Base(Exception):
    node : SourceNode
    def __init__(self, node : SourceNode, message : str) -> None:
        self.node = node
        message = "({} Ln {}, Col {}) {}".format(node.pos.filename, node.pos.line, node.pos.column, message)
        super().__init__(message)

class NoSuchFunction(Base):
    def __init__(self, node : SourceNode, name : str) -> None:
        super().__init__(node, "No such function: " + name)

class WrongArity(Base):
    def __init__(self, node : TVApply, expected : int) -> None:
        super().__init__(node, "{} expects {} argument(s)".format(node.function_name, expected))

class NotAnArray(Base):
    def __init__(self, node : SourceNode) -> None:
        super().__init__(node, "Not an array")

class NotAPair(Base):
    def __init__(self, node : SourceNode) -> None:
        super().__init__(node, "Not a pair (taking left or right)")

class StaticTypeMismatch(Base):
    def __init__(self, node : SourceNode, expected : T.Base, actual : T.Base, message : Optional[str] = None) -> None:
        msg = "Expected {} instead of {}".format(str(expected), str(actual))
        if message is not None:
            msg = msg + "; " + message
        super().__init__(node, msg)

class IncompatibleOperand(Base):
    def __init__(self, node : SourceNode, message : str) -> None:
        super().__init__(node, message)

class OutOfBounds(Base):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "Array index out of bounds")

class EmptyArray(Base):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "Empty array for Array+ input/declaration")

class UnknownIdentifier(Base):
    def __init__(self, node : TVIdent) -> None:
        id = node.namespace
        id.append(node.name)
        super().__init__(node, "Unknown identifier " + '.'.join(id))

class NoSuchInput(Base):
    def __init__(self, node : SourceNode, name : str) -> None:
        super().__init__(node, "No such input " + name)

class NullValue(Base):
    def __init__(self, node : SourceNode) -> None:
        super().__init__(node, "Null value")

class MultipleDefinitions(Base):
    def __init__(self, node : SourceNode, message : str) -> None:
        super().__init__(node, message)
