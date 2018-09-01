# pyre-strict
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Callable, NamedTuple, TypeVar
import WDL.Type as T
import WDL.Value as V

# the source file line/column for each expression, attached to each AST node
SourcePosition = NamedTuple("SourcePosition",
                            [('line',int), ('column',int),
                             ('end_line',int), ('end_column',int)])

# Forward-declare certain types needed for Error definitions, then import them.
TVBase = TypeVar('TVBase', bound='Base')
TVApply = TypeVar('TVApply', bound='Apply')
import WDL.Error as Error

class Env:
    pass

class Base(ABC):
    pos : SourcePosition
    type : T.Base

    def __init__(self, pos : SourcePosition, type : T.Base) -> None:
        self.type = type
        self.pos = pos

    def typecheck(self, expected : T.Base) -> TVBase:
        if expected is not None and self.type != expected:
            raise Error.StaticTypeMismatch(self, expected, self.type)
        return self

    @abstractmethod
    def eval(self, env : Env) -> V.Base:
        pass

# Boolean literal
class Boolean(Base):
    _literal : bool
    def __init__(self, pos : SourcePosition, literal : bool) -> None:
        super().__init__(pos, T.Boolean())
        self._literal = literal
    def eval(self, env : Env) -> V.Boolean:
        return V.Boolean(self._literal)

# Integer literal
class Int(Base):
    _literal : int
    def __init__(self, pos : SourcePosition, literal : int) -> None:
        super().__init__(pos, T.Int())
        self._literal = literal
    def typecheck(self, expected : T.Base) -> Base:
        if expected is not None and isinstance(expected, T.Float):
            return self
        return super().typecheck(expected) # pyre-ignore
    def eval(self, env : Env) -> V.Int:
        return V.Int(self._literal)

# Float literal
class Float(Base):
    _literal : float
    def __init__(self, pos : SourcePosition, literal : float) -> None:
        super().__init__(pos, T.Float())
        self._literal = literal
    def eval(self, env : Env) -> V.Float:
        return V.Float(self._literal)

# Array
class Array(Base):
    items : List[Base]
    item_type : Optional[T.Base]

    def __init__(self, pos : SourcePosition, items : List[Base]) -> None:
        self.pos = pos
        self.items = items
        self.item_type = None
        if len(self.items) > 0:
            # Use the type of the first item as the assumed item type
            self.item_type = self.items[0].type
            # Except, allow a mixture of Int and Float to construct Array[Float]
            if self.item_type == T.Int():
                for item in self.items:
                    if item.type == T.Float():
                        self.item_type = T.Float()
            # Check all items are compatible with this item type
            for item in self.items:
                try:
                    item.typecheck(self.item_type)
                except Error.StaticTypeMismatch:
                    raise Error.StaticTypeMismatch(self, self.item_type, item.type, "inconsistent types within array") from None
        # Our type is AnyArray for a literal empty array, otherwise Array(item_type)
        super(Array, self).__init__(pos, (T.Array(self.item_type) if self.item_type is not None else T.AnyArray()))

    def typecheck(self, expected : T.Base) -> Base:
        if len(self.items) == 0 and expected is not None and isinstance(expected, T.AnyArray):
            # the empty array (with type T.AnyArray) satisfies any array type
            assert self.type == T.AnyArray()
            return self
        return super().typecheck(expected) # pyre-ignore

    def eval(self, env : Env) -> V.Array:
        return V.Array(self.type, [item.eval(env).coerce(self.item_type) for item in self.items])

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
        if self.condition.type != T.Boolean():
            raise Error.StaticTypeMismatch(self, T.Boolean(), self.condition.type, "in if condition")
        # TODO: allow one Int & one Float
        if self.consequent.type != self.alternative.type:
            raise Error.StaticTypeMismatch(self, self.consequent.type, self.alternative.type, "if consequent & alternative must have the same type")
    
    def eval(self, env : Env) -> V.Base:
        if self.condition.eval(env).expect(T.Boolean()).value == False:
            return self.alternative.eval(env)
        return self.consequent.eval(env)

# function applications

# Abstract interface to an internal function implementation
# (see StdLib.py for concrete implementations)
class _Function(ABC):

    # Typecheck the given argument expressions against the function signature.
    # raise an error or return the type of the value that the function will
    # return when applied to these arguments.
    @abstractmethod
    def typecheck(self, expr : TVApply) -> T.Base:
        pass

    @abstractmethod
    def __call__(self, expr : TVApply, env : Env) -> V.Base:
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
            raise Error.NoSuchFunction(self, function) from None
        self.arguments = arguments
        return_type = self.function.typecheck(self)
        super().__init__(pos, return_type)

    def eval(self, env : Env) -> V.Base:
        return self.function(self, env)
