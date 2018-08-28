from abc import ABC, abstractmethod
from typing import Any, List, Optional, NamedTuple
import WDL.Type as Ty
import WDL.Value as Val
from collections import namedtuple

class Env:
    pass

class StaticTypeError(Exception):
    pass

class Base(ABC):
    type : Ty.Base

    def __init__(self, type : Ty.Base):
        assert isinstance(type, Ty.Base)
        self.type = type

    @abstractmethod
    def eval(self, env : Env) -> Val.Base:
        pass

# Boolean literal
class Boolean(Base):
    _literal : bool
    def __init__(self, literal : bool):
        super().__init__(Ty.Boolean())
        self._literal = literal
    def eval(self, env : Env) -> Val.Boolean:
        return Val.Boolean(self._literal)

# Integer literal
class Int(Base):
    _literal : int
    def __init__(self, literal : int):
        super().__init__(Ty.Int())
        self._literal = literal
    def eval(self, env : Env) -> Val.Int:
        return Val.Int(self._literal)

# Float literal
class Float(Base):
    _literal : float
    def __init__(self, literal : float):
        super().__init__(Ty.Float())
        self._literal = literal
    def eval(self, env : Env) -> Val.Float:
        return Val.Float(self._literal)

# Array
class Array(Base):
    items : List[Base]

    def __init__(self, items : List[Base], item_type : Optional[Ty.Base] = None):
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

    def __init__(self, items : List[Base]):
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

StdFunction = namedtuple("StdFunction", "argument_types return_type F")
stdlib = {
    "_negate" : StdFunction(argument_types=[Ty.Boolean()], return_type=Ty.Boolean(),
                            F=lambda x: Val.Boolean(not x.value)),
    "_land" : StdFunction(argument_types=[Ty.Boolean(), Ty.Boolean()], return_type=Ty.Boolean(),
                          F=lambda l,r: Val.Boolean(l.value and r.value)),
    "_lor" : StdFunction(argument_types=[Ty.Boolean(), Ty.Boolean()], return_type=Ty.Boolean(),
                         F=lambda l,r: Val.Boolean(l.value or r.value)),
    "_add" : StdFunction(argument_types=[Ty.Int(), Ty.Int()], return_type=Ty.Int(),
                         F=lambda l,r: Val.Int(l.value + r.value)),
    "_sub" : StdFunction(argument_types=[Ty.Int(), Ty.Int()], return_type=Ty.Int(),
                         F=lambda l,r: Val.Int(l.value - r.value)),
    "_mul" : StdFunction(argument_types=[Ty.Int(), Ty.Int()], return_type=Ty.Int(),
                         F=lambda l,r: Val.Int(l.value * r.value)),
    "_div" : StdFunction(argument_types=[Ty.Int(), Ty.Int()], return_type=Ty.Int(),
                         F=lambda l,r: Val.Int(int(l.value / r.value))),
    "_rem" : StdFunction(argument_types=[Ty.Int(), Ty.Int()], return_type=Ty.Int(),
                         F=lambda l,r: Val.Int(l.value % r.value)),
    "_eqeq" : StdFunction(argument_types=[None, None], return_type=Ty.Boolean(),
                          F=lambda l,r: Val.Boolean(l == r)),
    "_neq" : StdFunction(argument_types=[None, None], return_type=Ty.Boolean(),
                         F=lambda l,r: Val.Boolean(l != r)),
    "_lt" : StdFunction(argument_types=[None, None], return_type=Ty.Boolean(),
                        F=lambda l,r: Val.Boolean(l.value < r.value)),
    "_lte" : StdFunction(argument_types=[None, None], return_type=Ty.Boolean(),
                         F=lambda l,r: Val.Boolean(l.value <= r.value)),
    "_gt" : StdFunction(argument_types=[None, None], return_type=Ty.Boolean(),
                        F=lambda l,r: Val.Boolean(l.value > r.value)),
    "_gte" : StdFunction(argument_types=[None, None], return_type=Ty.Boolean(),
                         F=lambda l,r: Val.Boolean(l.value >= r.value)),
    "_get" : StdFunction(argument_types=[None, Ty.Int()], return_type=Ty.Int(), #FIXME
                         F=lambda arr,which: arr.value[which.value])
}
class Apply(Base):
    function : StdFunction
    arguments : List[Base]

    def __init__(self, function : str, arguments : List[Base]):
        self.function = stdlib[function]
        self.arguments = arguments #TODO: check arity and types of arguments
        super(Apply, self).__init__(self.function.return_type)

    def eval(self, env : Env) -> Val.Base:
        argument_values = [arg.eval(env) for arg in self.arguments]
        return self.function.F(*argument_values)
