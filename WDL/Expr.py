# pyre-strict
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Callable, NamedTuple, TypeVar
import WDL.Type as Ty
import WDL.Value as Val

SourcePosition = NamedTuple("SourcePosition",
                            [('line',int), ('column',int),
                             ('end_line',int), ('end_column',int)])

BaseT = TypeVar('BaseT', bound='Base')
class Error(Exception):
    expr : BaseT
    def __init__(self, expr : BaseT, message : str) -> None:
        self.expr = expr
        message = "(Ln {}, Col {}) {}".format(expr.pos.line, expr.pos.column, message) # pyre-ignore
        super().__init__(message)

class NoSuchFunction(Error):
    def __init__(self, expr : BaseT, name : str) -> None:
        super().__init__(expr, "No such function: " + name)

ApplyT = TypeVar('ApplyT', bound='Apply')
class WrongArity(Error):
    def __init__(self, expr : ApplyT, expected : int) -> None:
        super().__init__(expr, "{} expects {} argument(s)".format(expr.name, expected)) # pyre-ignore

class NotAnArray(Error):
    def __init__(self, expr : BaseT) -> None:
        super().__init__(expr, "Not an array")

class StaticTypeMismatch(Error):
    def __init__(self, expr : BaseT, expected : Ty.Base, actual : Ty.Base, message : Optional[str] = None) -> None:
        msg = "Expected {} instead of {}".format(str(expected), str(actual))
        if message is not None:
            msg = msg + "; " + message
        super().__init__(expr, msg)

class IncompatibleOperand(Error):
    def __init__(self, expr : BaseT, message : str) -> None:
        super().__init__(expr, message)

class OutOfBounds(Error):
    def __init__(self, expr: BaseT) -> None:
        super().__init__(expr, "Array index out of bounds")

class Env:
    pass


class Base(ABC):
    pos : SourcePosition
    type : Ty.Base

    def __init__(self, pos : SourcePosition, type : Ty.Base) -> None:
        self.type = type
        self.pos = pos

    def typecheck(self, expected : Ty.Base) -> BaseT:
        if expected is not None and self.type != expected:
            raise StaticTypeMismatch(self, expected, self.type)
        return self

    @abstractmethod
    def eval(self, env : Env) -> Val.Base:
        pass

# Boolean literal
class Boolean(Base):
    _literal : bool
    def __init__(self, pos : SourcePosition, literal : bool) -> None:
        super().__init__(pos, Ty.Boolean())
        self._literal = literal
    def eval(self, env : Env) -> Val.Boolean:
        return Val.Boolean(self._literal)

# Integer literal
class Int(Base):
    _literal : int
    def __init__(self, pos : SourcePosition, literal : int) -> None:
        super().__init__(pos, Ty.Int())
        self._literal = literal
    def typecheck(self, expected : Ty.Base) -> Base:
        if expected is not None and isinstance(expected, Ty.Float):
            return self
        return super().typecheck(expected) # pyre-ignore
    def eval(self, env : Env) -> Val.Int:
        return Val.Int(self._literal)

# Float literal
class Float(Base):
    _literal : float
    def __init__(self, pos : SourcePosition, literal : float) -> None:
        super().__init__(pos, Ty.Float())
        self._literal = literal
    def eval(self, env : Env) -> Val.Float:
        return Val.Float(self._literal)

# Array
class Array(Base):
    items : List[Base]

    def __init__(self, pos : SourcePosition, items : List[Base], item_type : Optional[Ty.Base] = None) -> None:
        if item_type is None:
            # TODO handle empty array
            item_type = items[0].type

        # TODO: if elements are a mix of Int and Float, coerce all to Float.
        for item in items:
            if item.type != item_type:
                raise StaticTypeMismatch(self, item_type, item.type, "array item type mismatch")

        self.items = items
        super(Array, self).__init__(pos, Ty.Array(item_type))

    def eval(self, env : Env) -> Val.Array:
        return Val.Array(self.type, [item.eval(env) for item in self.items])

# If
class IfThenElse(Base):
    condition : Base
    consequent : Base
    alternative : Base

    def __init__(self, pos : SourcePosition, items : List[Base]) -> None:
        assert len(items) == 3
        self.condition = items[0]
        self.consequent = items[1]
        self.alternative = items[2]
        super().__init__(pos, self.consequent.type)
        if self.condition.type != Ty.Boolean():
            raise StaticTypeMismatch(self, Ty.Boolean(), self.condition.type, "in if condition")
        # TODO: allow one Int & one Float
        if self.consequent.type != self.alternative.type:
            raise StaticTypeMismatch(self, self.consequent.type, self.alternative.type, "if consequent & alternative must have the same type")
    
    def eval(self, env : Env) -> Val.Base:
        if self.condition.eval(env).expect(Ty.Boolean()).value == False:
            return self.alternative.eval(env)
        return self.consequent.eval(env)

# function applications

# Abstract interface to an internal function implementation
class _Function(ABC):

    # Typecheck the given argument expressions against the function signature.
    # Raise an error or return the type of the value that the function will
    # return when applied to these arguments.
    @abstractmethod
    def typecheck(self, expr : ApplyT) -> Ty.Base:
        pass

    @abstractmethod
    def __call__(self, expr : ApplyT, env : Env) -> Val.Base:
        pass
# Table of standard library functions, filled in below and in StdLib.py
_stdlib : Dict[str,_Function] = {}

class Apply(Base):
    function : _Function
    arguments : List[Base]

    def __init__(self, pos : SourcePosition, function : str, arguments : List[Base]) -> None:
        self.pos = pos
        try:
            self.function = _stdlib[function]
        except KeyError:
            raise NoSuchFunction(self, function) from None
        self.arguments = arguments
        return_type = self.function.typecheck(self)
        super().__init__(pos, return_type)

    def eval(self, env : Env) -> Val.Base:
        return self.function(self, env)

# _Function helper for simple functions with fixed argument and return types
# (used in StdLib.py)
class _StaticFunction(_Function):
    name : str
    argument_types : List[Ty.Base]
    return_type : Ty.Base
    F : Callable

    def __init__(self, name : str, argument_types : List[Ty.Base], return_type : Ty.Base, F : Callable) -> None:
        self.name = name
        self.argument_types = argument_types
        self.return_type = return_type
        self.F = F

    def typecheck(self, expr : Apply) -> Ty.Base:
        if len(expr.arguments) != len(self.argument_types):
            raise WrongArity(expr, len(self.argument_types))
        for i in range(len(self.argument_types)):
            try:
                expr.arguments[i].typecheck(self.argument_types[i])
            except StaticTypeMismatch:
                raise StaticTypeMismatch(expr.arguments[i], self.argument_types[i], expr.arguments[i].type, "{} argument #{}".format(name, i+1)) from None
        return self.return_type

    def __call__(self, expr : Apply, env : Env) -> Val.Base:
        assert len(expr.arguments) == len(self.argument_types)
        argument_values = [arg.eval(env).coerce(ty) for arg, ty in zip(expr.arguments, self.argument_types)]
        ans : Val.Base = self.F(*argument_values)
        return ans.coerce(self.return_type)

# Special function for array access arr[index], returning the element type
class _ArrayGet(_Function):
    def typecheck(self, expr : Apply) -> Ty.Base:
        assert len(expr.arguments) == 2
        if not isinstance(expr.arguments[0].type, Ty.Array):
            raise NotAnArray(expr.arguments[0])
        try:
            expr.arguments[1].typecheck(Ty.Int())
        except StaticTypeMismatch:
            raise StaticTypeMismatch(expr.arguments[1], Ty.Int(), expr.arguments[1].type, "Array index") from None
        return expr.arguments[0].type.item_type

    def __call__(self, expr : Apply, env : Env) -> Val.Base:
        assert len(expr.arguments) == 2
        arr = expr.arguments[0].eval(env)
        assert isinstance(arr.type, Ty.Array)
        assert isinstance(arr.value, list)
        idx = expr.arguments[1].eval(env).expect(Ty.Int()).value
        if idx < 0 or idx >= len(arr.value):
            raise OutOfBounds(expr.arguments[1])
        return arr.value[idx] # pyre-ignore
_stdlib["_get"] = _ArrayGet()
