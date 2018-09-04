# pyre-strict
from typing import Any, List, Optional, Dict, Callable, NamedTuple, TypeVar
import WDL.Type as T
from WDL.Expr import SourcePosition, TVBase, TVApply

class Base(Exception):
    expr : TVBase
    def __init__(self, expr : TVBase, message : str) -> None:
        self.expr = expr
        message = "(Ln {}, Col {}) {}".format(expr.pos.line, expr.pos.column, message) # pyre-ignore
        super().__init__(message)

class NoSuchFunction(Base):
    def __init__(self, expr : TVBase, name : str) -> None:
        super().__init__(expr, "No such function: " + name)

class WrongArity(Base):
    def __init__(self, expr : TVApply, expected : int) -> None:
        super().__init__(expr, "{} expects {} argument(s)".format(expr.name, expected)) # pyre-ignore

class NotAnArray(Base):
    def __init__(self, expr : TVBase) -> None:
        super().__init__(expr, "Not an array")

class StaticTypeMismatch(Base):
    def __init__(self, expr : TVBase, expected : T.Base, actual : T.Base, message : Optional[str] = None) -> None:
        msg = "Expected {} instead of {}".format(str(expected), str(actual))
        if message is not None:
            msg = msg + "; " + message
        super().__init__(expr, msg)

class IncompatibleOperand(Base):
    def __init__(self, expr : TVBase, message : str) -> None:
        super().__init__(expr, message)

class OutOfBounds(Base):
    def __init__(self, expr: TVBase) -> None:
        super().__init__(expr, "Array index out of bounds")

class UnknownIdentifier(Base):
    def __init__(self, expr : TVBase) -> None:
        super().__init__(expr, "Unknown identifier")

class MissingValue(Base):
    def __init__(self, expr : TVBase) -> None:
        super().__init__(expr, "Optional value is missing")
