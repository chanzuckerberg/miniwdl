# pyre-strict
from typing import List, Optional, NamedTuple, Union, Iterable
from functools import total_ordering
import WDL.Type as T


class ParserError(Exception):
    def __init__(self, filename: str) -> None:
        super().__init__(filename)


class ImportError(Exception):
    def __init__(self, document: str, import_uri: str, message: Optional[str] = None) -> None:
        msg = "Failed to import {} to {}".format(import_uri, document)
        if message:
            msg = msg + ": " + message
        super().__init__(msg)


SourcePosition = NamedTuple(
    "SourcePosition",
    [("filename", str), ("line", int), ("column", int), ("end_line", int), ("end_column", int)],
)
"""Source file, line, and column, attached to each AST node"""


@total_ordering
class SourceNode:
    """Base class for an AST node, recording the source position"""

    pos: SourcePosition
    """Source position for this AST node"""

    def __init__(self, pos: SourcePosition) -> None:
        self.pos = pos

    def __lt__(self, rhs) -> bool:
        if isinstance(rhs, SourceNode):
            return (
                self.pos.filename,
                self.pos.line,
                self.pos.column,
                self.pos.end_line,
                self.pos.end_column,
            ) < (
                rhs.pos.filename,
                rhs.pos.line,
                rhs.pos.column,
                rhs.pos.end_line,
                rhs.pos.end_column,
            )
        return False

    def __eq__(self, rhs) -> bool:
        return self.pos == rhs.pos


class Base(Exception):
    node: Optional[SourceNode]

    def __init__(self, node: Union[SourceNode, SourcePosition], message: str) -> None:
        if isinstance(node, SourceNode):
            self.node = node
            self.pos = node.pos
        else:
            self.pos = node
        message = "({} Ln {}, Col {}) {}".format(
            self.pos.filename, self.pos.line, self.pos.column, message
        )
        super().__init__(message)


class NoSuchFunction(Base):
    def __init__(self, node: SourceNode, name: str) -> None:
        super().__init__(node, "No such function: " + name)


class WrongArity(Base):
    def __init__(self, node: SourceNode, expected: int) -> None:
        # avoiding circular dep:
        # assert isinstance(node, WDL.Expr.Apply)
        msg = "{} expects {} argument(s)".format(getattr(node, "function_name"), expected)
        super().__init__(node, msg)


class NotAnArray(Base):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "Not an array")


class NotAPair(Base):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "Not a pair (taking left or right)")


class StaticTypeMismatch(Base):
    def __init__(
        self, node: SourceNode, expected: T.Base, actual: T.Base, message: Optional[str] = None
    ) -> None:
        msg = "Expected {} instead of {}".format(str(expected), str(actual))
        if message is not None:
            msg = msg + " " + message
        super().__init__(node, msg)


class IncompatibleOperand(Base):
    def __init__(self, node: SourceNode, message: str) -> None:
        super().__init__(node, message)


class OutOfBounds(Base):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "Array index out of bounds")


class EmptyArray(Base):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "Empty array for Array+ input/declaration")


class UnknownIdentifier(Base):
    def __init__(self, node: SourceNode) -> None:
        # avoiding circular dep:
        # assert isinstance(node, WDL.Expr.Ident)
        namespace: List[str] = getattr(node, "namespace")
        name: str = getattr(node, "name")
        super().__init__(node, "Unknown identifier " + ".".join(namespace + [name]))


class NoSuchInput(Base):
    def __init__(self, node: SourceNode, name: str) -> None:
        super().__init__(node, "No such input " + name)


class MissingInput(Base):
    def __init__(self, node: SourceNode, name: str, inputs: Iterable[str]) -> None:
        super().__init__(
            node, "Call {} missing required input(s) {}".format(name, ", ".join(inputs))
        )


class NullValue(Base):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "Null value")


class MultipleDefinitions(Base):
    def __init__(self, node: Union[SourceNode, SourcePosition], message: str) -> None:
        super().__init__(node, message)
