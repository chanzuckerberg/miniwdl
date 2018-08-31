# pyre-strict
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Callable
import WDL.Type as Ty
import WDL.Value as Val

class Env:
    pass

class Base(ABC):
    type : Ty.Base

    def __init__(self, type : Ty.Base) -> None:
        assert isinstance(type, Ty.Base)
        self.type = type

    @abstractmethod
    def eval(self, env : Env) -> Val.Base:
        pass

# Boolean literal
class Boolean(Base):
    _literal : bool
    def __init__(self, literal : bool) -> None:
        super().__init__(Ty.Boolean())
        self._literal = literal
    def eval(self, env : Env) -> Val.Boolean:
        return Val.Boolean(self._literal)

# Integer literal
class Int(Base):
    _literal : int
    def __init__(self, literal : int) -> None:
        super().__init__(Ty.Int())
        self._literal = literal
    def eval(self, env : Env) -> Val.Int:
        return Val.Int(self._literal)

# Float literal
class Float(Base):
    _literal : float
    def __init__(self, literal : float) -> None:
        super().__init__(Ty.Float())
        self._literal = literal
    def eval(self, env : Env) -> Val.Float:
        return Val.Float(self._literal)

# Array
class Array(Base):
    items : List[Base]

    def __init__(self, items : List[Base], item_type : Optional[Ty.Base] = None) -> None:
        if item_type is None:
            if len(items) == 0:
                raise Ty.StaticTypeError("empty array has ambiguous type")
            item_type = items[0].type

        # TODO: if elements are a mix of Int and Float, coerce all to Float.
        for item in items:
            if item.type != item_type:
                raise Ty.StaticTypeMismatchError(item_type, item.type, "array item type mismatch")

        self.items = items
        super(Array, self).__init__(Ty.Array(item_type))

    def eval(self, env : Env) -> Val.Array:
        return Val.Array(self.type, [item.eval(env) for item in self.items])

# If
class IfThenElse(Base):
    condition : Base
    consequent : Base
    alternative : Base

    def __init__(self, items : List[Base]) -> None:
        assert len(items) == 3
        self.condition = items[0]
        if self.condition.type != Ty.Boolean():
            raise Ty.StaticTypeMismatchError(Ty.Boolean(), self.condition.type, "if condition")
        self.consequent = items[1]
        self.alternative = items[2]
        if self.consequent.type != self.alternative.type:
            raise Ty.StaticTypeMismatchError(self.consequent.type, self.alternative.type, "if consequent & alternative must have the same type")
        super().__init__(self.consequent.type)
    
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
    def typecheck(self, arguments : List[Base]) -> Ty.Base:
        pass

    @abstractmethod
    def __call__(self, arguments : List[Base], env : Env) -> Val.Base:
        pass
# Table of standard library functions, filled in below and in StdLib.py
_stdlib : Dict[str,_Function] = {}

class NoSuchFunctionError(Exception):
    def __init__(self, name : str) -> None:
        super().__init__("No such function: " + name)

class Apply(Base):
    function : _Function
    arguments : List[Base]

    def __init__(self, function : str, arguments : List[Base]) -> None:
        try:
            self.function = _stdlib[function]
        except KeyError:
            raise NoSuchFunctionError(function) from None
        self.arguments = arguments
        return_type = self.function.typecheck(self.arguments)
        super().__init__(return_type)

    def eval(self, env : Env) -> Val.Base:
        return self.function(self.arguments, env)

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

    def typecheck(self, arguments : List[Base]) -> Ty.Base:
        if len(arguments) != len(self.argument_types):
            raise Ty.StaticTypeError("{} expects {} arguments".format(name, len(self.argument_types)))
        for i in range(len(self.argument_types)):
            try:
                arguments[i].type.expect(self.argument_types[i])
            except Ty.StaticTypeMismatchError:
                raise Ty.StaticTypeMismatchError(self.argument_types[i], arguments[i].type, "{} argument #{}".format(name, i+1)) from None
        return self.return_type

    def __call__(self, arguments : List[Base], env : Env) -> Val.Base:
        assert len(arguments) == len(self.argument_types)
        argument_values = [arg.eval(env).coerce(ty) for arg, ty in zip(arguments, self.argument_types)]
        ans : Val.Base = self.F(*argument_values)
        try:
            return ans.coerce(self.return_type)
        except Val.RuntimeTypeError:
            raise Val.RuntimeTypeError("miniwdl INTERNAL ERROR: implementation of {} returned {} instead of {}".format(name, str(ans.type), str(self.return_type)))

# Special function for array access arr[index], returning the element type
class BoundsError(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)
class _ArrayGet(_Function):
    def typecheck(self, arguments : List[Base]) -> Ty.Base:
        assert len(arguments) == 2
        if not isinstance(arguments[0].type, Ty.Array):
            raise Ty.StaticTypeError("not an array")
        try:
            arguments[1].type.expect(Ty.Int())
        except Ty.StaticTypeMismatchError:
            raise Ty.StaticTypeMismatchError(Ty.Int(), arguments[1].type, "Array must be accessed by Int index") from None
        return arguments[0].type.item_type

    def __call__(self, arguments : List[Base], env : Env) -> Val.Base:
        assert len(arguments) == 2
        arr = arguments[0].eval(env)
        assert isinstance(arr.type, Ty.Array)
        assert isinstance(arr.value, list)
        idx = arguments[1].eval(env).expect(Ty.Int()).value
        if idx < 0 or idx >= len(arr.value):
            raise BoundsError(idx.value)
        return arr.value[idx] # pyre-ignore
_stdlib["_get"] = _ArrayGet()
