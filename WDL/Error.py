# pyre-strict
import os
from typing import List, Optional, NamedTuple, Union, Iterable, TypeVar, Generator, Callable, Any
from functools import total_ordering
from contextlib import contextmanager
import WDL.Type as T


class SyntaxError(Exception):
    """Failure to lex/parse a WDL document"""

    def __init__(self, filename: str, msg: str) -> None:
        super().__init__("({}) {}".format(filename, msg))


class ImportError(Exception):
    """Failure to open/retrieve an imported WDL document

    The ``__cause__`` attribute may hold the inner error object."""

    def __init__(self, document: str, import_uri: str, message: Optional[str] = None) -> None:
        msg = "({}) Failed to import {}".format(document, import_uri)
        if message:
            msg = msg + ", " + message
        super().__init__(msg)


SourcePosition = NamedTuple(
    "SourcePosition",
    [("filename", str), ("line", int), ("column", int), ("end_line", int), ("end_column", int)],
)
"""Source file, line, and column, attached to each AST node"""

TVSourceNode = TypeVar("TVSourceNode", bound="SourceNode")


@total_ordering
class SourceNode:
    """Base class for an AST node, recording the source position"""

    pos: SourcePosition
    """Source position for this AST node"""

    def __init__(self, pos: SourcePosition) -> None:
        self.pos = pos

    def __lt__(self, rhs: TVSourceNode) -> bool:
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

    def __eq__(self, rhs: TVSourceNode) -> bool:
        assert isinstance(rhs, SourceNode)
        return self.pos == rhs.pos

    @property
    def children(self: TVSourceNode) -> Iterable[TVSourceNode]:
        """
        :type: Iterable[SourceNode]

        Yield all child nodes
        """
        return []


class ValidationError(Exception):
    """Base class for a WDL validation error (when the document loads and parses, but fails typechecking or other static validity tests)"""

    pos: SourcePosition
    """:type: SourcePosition"""

    node: Optional[SourceNode] = None
    """:type: Optional[SourceNode]"""

    source_text: Optional[str] = None
    """:type: Optional[str]

    The complete source text of the WDL document (if available)"""

    def __init__(self, node: Union[SourceNode, SourcePosition], message: str) -> None:
        if isinstance(node, SourceNode):
            self.node = node
            self.pos = node.pos
        else:
            self.pos = node
        message = "({} Ln {}, Col {}) {}".format(
            os.path.basename(self.pos.filename), self.pos.line, self.pos.column, message
        )
        super().__init__(message)


class InvalidType(ValidationError):
    pass


class NoSuchTask(ValidationError):
    def __init__(self, node: Union[SourceNode, SourcePosition], name: str) -> None:
        super().__init__(node, "No such task/workflow: " + name)


class NoSuchFunction(ValidationError):
    def __init__(self, node: SourceNode, name: str) -> None:
        super().__init__(node, "No such function: " + name)


class WrongArity(ValidationError):
    def __init__(self, node: SourceNode, expected: int) -> None:
        # avoiding circular dep:
        # assert isinstance(node, WDL.Expr.Apply)
        msg = "{} expects {} argument(s)".format(getattr(node, "function_name"), expected)
        super().__init__(node, msg)


class NotAnArray(ValidationError):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "Not an array")


class NoSuchMember(ValidationError):
    def __init__(self, node: SourceNode, member: str) -> None:
        super().__init__(node, "No such member '{}'".format(member))


class StaticTypeMismatch(ValidationError):
    def __init__(
        self, node: SourceNode, expected: T.Base, actual: T.Base, message: Optional[str] = None
    ) -> None:
        self.expected = expected
        self.actual = actual
        msg = "Expected {} instead of {}".format(str(expected), str(actual))
        if message is not None:
            msg = msg + " " + message
        super().__init__(node, msg)


class IncompatibleOperand(ValidationError):
    def __init__(self, node: SourceNode, message: str) -> None:
        super().__init__(node, message)


class OutOfBounds(ValidationError):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "Array index out of bounds")


class EmptyArray(ValidationError):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "Empty array for Array+ input/declaration")


class UnknownIdentifier(ValidationError):
    def __init__(self, node: SourceNode) -> None:
        # avoiding circular dep:
        # assert isinstance(node, WDL.Expr.Ident)
        super().__init__(node, "Unknown identifier " + ".".join(getattr(node, "_ident")))


class NoSuchInput(ValidationError):
    def __init__(self, node: SourceNode, name: str) -> None:
        super().__init__(node, "No such input " + name)


class UncallableWorkflow(ValidationError):
    def __init__(self, node: SourceNode, name: str) -> None:
        super().__init__(
            node,
            "Cannot call workflow {} because its calls don't supply all required inputs, or it lacks an output section".format(
                name
            ),
        )


class MultipleDefinitions(ValidationError):
    pass


class StrayInputDeclaration(ValidationError):
    pass


class CircularDependencies(ValidationError):
    def __init__(self, node: SourceNode) -> None:
        super().__init__(node, "circular dependencies involving {}".format(getattr(node, "name")))


class MultipleValidationErrors(Exception):
    """Propagates several validation/typechecking errors"""

    exceptions: List[ValidationError]
    """:type: List[ValidationError]"""

    def __init__(
        self, *exceptions: List[Union[ValidationError, "MultipleValidationErrors"]]
    ) -> None:
        super().__init__()
        self.exceptions = []
        for exn in exceptions:
            if isinstance(exn, ValidationError):
                self.exceptions.append(exn)
            elif isinstance(exn, MultipleValidationErrors):
                self.exceptions.extend(exn.exceptions)
            else:
                assert False
        assert self.exceptions
        self.exceptions = sorted(self.exceptions, key=lambda exn: getattr(exn, "pos"))


class _MultiContext:
    ""
    _exceptions: List[Union[ValidationError, MultipleValidationErrors]]

    def __init__(self) -> None:
        self._exceptions = []

    def try1(self, fn: Callable[[], Any]) -> Optional[Any]:  # pyre-ignore
        try:
            return fn()
        except (ValidationError, MultipleValidationErrors) as exn:
            self._exceptions.append(exn)
            return None

    def append(self, exn: Union[ValidationError, MultipleValidationErrors]) -> None:
        self._exceptions.append(exn)

    def maybe_raise(self) -> None:
        if len(self._exceptions) == 1:
            raise self._exceptions[0]
        if self._exceptions:
            raise MultipleValidationErrors(*self._exceptions)  # pyre-ignore


@contextmanager
def multi_context() -> Generator[_MultiContext, None, None]:
    ""
    # Context manager to assist with catching and propagating multiple
    # validation/typechecking errors
    #
    # with WDL.Error.multi_context() as errors:
    #
    #    result = errors.try1(lambda: perform_validation())
    #    # Returns the result of invoking the lambda. If the lambda invocation
    #    # raises WDL.Error.ValidationError or
    #    # WDL.Error.MultipleValidationErrors, records the error and returns
    #    # None. (Other exceptions would halt execution and propagate
    #    # normally.)
    #
    #    errors.append(WDL.Error.NullValue())
    #    # errors.append() manually records one error.
    #
    # When the context closes, any exceptions recorded with errors.try1() or
    # errors.append() are raised at that point. Note that any exception raised
    # outside of errors.try1() will exit the context immediately and discard
    # any previously-recorded errors.
    #
    # Lastly, you can call errors.maybe_raise() to immediately propagate any
    # exceptions recorded so far, or if none, proceed with the remainder of
    # the context body.
    ctx = _MultiContext()
    yield ctx
    ctx.maybe_raise()


class RuntimeError(Exception):
    pass


class EvalError(RuntimeError):
    """Error evaluating a WDL expression or declaration"""

    pos: SourcePosition
    """:type: SourcePosition"""

    node: Optional[SourceNode] = None
    """:type: Optional[SourceNode]"""

    def __init__(self, node: Union[SourceNode, SourcePosition], message: str) -> None:
        if isinstance(node, SourceNode):
            self.node = node
            self.pos = node.pos
        else:
            self.pos = node
        message = "({} Ln {}, Col {}) {}".format(
            os.path.basename(self.pos.filename), self.pos.line, self.pos.column, message
        )
        super().__init__(message)


class NullValue(EvalError):
    def __init__(self, node: Union[SourceNode, SourcePosition]) -> None:
        super().__init__(node, "Null value")


class InputError(RuntimeError):
    """Error reading an input value/file"""

    pass
