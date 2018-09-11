# pyre-strict
"""
WDL expressions: literal values, arithmetic, comparison, conditionals, string
interpolation, arrays & maps, standard library functions

The abstract syntax tree (AST) for any expression is represented by an instance
of a Python class deriving from ``WDL.Expr.Base``. Any such node may have other
nodes attached "beneath" it. Given a suitable environment, expressions can be
evaluated to a WDL `Value`.
"""
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Callable, NamedTuple, TypeVar, Tuple, Union
import WDL.Type as T
import WDL.Value as V

SourcePosition = NamedTuple("SourcePosition",
                            [('line',int), ('column',int),
                             ('end_line',int), ('end_column',int)])
"""Source file line/column for each expression, attached to each AST node"""

# Forward-declare certain types needed for Error definitions, then import them.
TVBase = TypeVar('TVBase', bound='Base')
TVApply = TypeVar('TVApply', bound='Apply')
import WDL.Error as Error

class TypeEnv:
    """Provides the types of bound identifiers during static analysis, prior to any evaluation"""
    _bindings : Dict[str, T.Base]

    def __init__(self, *bindings : List[Tuple[str, T.Base]]) -> None:
        self._bindings = {}
        for id, type in bindings:
            self._bindings[id] = type

    def __getitem__(self, id : str) -> T.Base:
        """
        Look up the data type of the given identifier
        """
        return self._bindings[id]

    def __setitem__(self, id : str, type : T.Base) -> None:
        self._bindings[id] = type

class Env:
    """Provides the bindings of identifiers to existing values during expression evaluation"""
    _bindings : Dict[str, V.Base]

    def __init__(self, *bindings) -> None:
        self._bindings = {}
        for id, value in bindings:
            self._bindings[id] = value

    def __getitem__(self, id : str) -> V.Base:
        """
        Look up the value bound to the given identifier
        """
        return self._bindings[id]

    def __setitem__(self, id : str, value : V.Base) -> None:
        self._bindings[id] = value

class Base(ABC):
    """Superclass of all expression AST nodes"""
    pos : SourcePosition
    """Source position for this AST node"""
    type : T.Base
    """WDL type of this expression AST node"""

    def __init__(self, pos : SourcePosition, type : T.Base) -> None:
        self.type = type
        self.pos = pos

    def typecheck(self, expected : T.Base) -> TVBase:
        """
        Check that this expression's type is, or can be coerced to,
        `expected`. 

        :raise WDL.Error.StaticTypeMismatch:
        :return: `self`
        """
        if expected is not None and self.type != expected:
            raise Error.StaticTypeMismatch(self, expected, self.type)
        return self

    @abstractmethod
    def eval(self, env : Env) -> V.Base:
        """Evaluate the expression in the given environment"""
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
        """An ``Int`` expression can be coerced to ``Float`` when context demands."""
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

class String(Base):
    """Strings include literals possibly interleaved with interpolated expressions"""
    _parts : List[Union[str,Base]]
    def __init__(self, pos : SourcePosition, parts : List[Union[str,Base]]) -> None:
        super().__init__(pos, T.String())
        self._parts = parts
    def eval(self, env : Env) -> V.String:
        ans = []
        for part in self._parts:
            if isinstance(part, Base):
                # evaluate interpolated expression & stringify
                ans.append(str(part.eval(env)))
            elif type(part) == str:
                # use python builtins to decode escape sequences
                ans.append(str.encode(part).decode('unicode_escape')) # pyre-ignore
            else:
                assert False
        # concatenate the stringified parts and trim the surrounding quotes
        return V.String(''.join(ans)[1:-1])

# Array
class Array(Base):
    items : List[Base]
    """Expression for each item in the array"""
    item_type : Optional[T.Base]
    """Type of the items, or `None` for a literal empty array"""

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
    """A Boolean expression for the condition"""

    consequent : Base
    """Expression evaluated when the condition is true"""

    alternative : Base
    """Expression evaluated when the condition is false"""

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
    """Application of a built-in or standard library function"""
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


# Namespaced identifiers

class Ident(Base):
    """An identifier expected to resolve in the environment given during evaluation"""
    namespace : List[str]
    identifier : str

    def __init__(self, pos : SourcePosition, parts : List[str], type_env : TypeEnv) -> None:
        self.pos = pos
        assert len(parts) > 0
        self.identifier = parts[-1]
        self.namespace = parts[:-1]
        assert self.namespace == [] # placeholder
        try:
            my_type = type_env[self.identifier]
        except KeyError:
            raise Error.UnknownIdentifier(self)
        super().__init__(pos, my_type)

    def eval(self, env : Env) -> V.Base:
        try:
            # TODO: handling missing values
            return env[self.identifier]
        except KeyError:
            raise Error.UnknownIdentifier(self)
