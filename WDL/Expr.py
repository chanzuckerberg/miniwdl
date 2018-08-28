# pyre-strict
from abc import ABC, abstractmethod
from typing import Any, List, Optional, NamedTuple
import WDL.Type as Ty
import WDL.Value as Val
import WDL.StdLib
from collections import namedtuple

class Env:
    pass

class StaticTypeError(Exception):
    def __init__(self, message : str) -> None:
        super().__init__(message)

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
                raise StaticTypeError("empty array has ambiguous type")
            item_type = items[0].type

        for item in items:
            if item.type != item_type:
                raise StaticTypeError("array item type mismatch") #FIXME

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
            raise StaticTypeError("if-condition must have type Boolean")
        self.consequent = items[1]
        self.alternative = items[2]
        if self.consequent.type != self.alternative.type:
            raise StaticTypeError("if consequent & alternative must have the same type")
        super().__init__(self.consequent.type)
    
    def eval(self, env : Env) -> Val.Base:
        c = self.condition.eval(env)
        if c.value == False:
            return self.alternative.eval(env)
        return self.consequent.eval(env)

class Apply(Base):
    function : WDL.StdLib.Function
    arguments : List[Base]

    def __init__(self, function : str, arguments : List[Base]) -> None:
        self.function = WDL.StdLib.functions[function]
        self.arguments = arguments #TODO: check arity and types of arguments
        super().__init__(self.function.return_type) # pyre-ignore

    def eval(self, env : Env) -> Val.Base:
        argument_values = [arg.eval(env) for arg in self.arguments]
        return self.function.F(*argument_values) # pyre-ignore
