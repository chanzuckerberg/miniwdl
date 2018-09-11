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
    _type : Optional[T.Base] = None

    def __init__(self, pos : SourcePosition) -> None:
        self.pos = pos

    @property
    def type(self) -> T.Base:
        """
        WDL type of this expression.

        Undefined on construction; populated by one invocation of ``infer_type``.
        """
        # Failure of this assertion indicates use of an Expr object without
        # first calling _infer_type
        assert self._type is not None
        return self._type

    @abstractmethod
    def _infer_type(self, type_env : TypeEnv) -> T.Base:
        pass

    def infer_type(self, type_env : TypeEnv) -> TVBase:
        """
        Infer the expression's type within the given type environment. Must be
        invoked exactly once prior to use of other methods.

        :raise WDL.Error.StaticTypeMismatch:
        :return: `self`
        """
        # Failure of this assertion indicates multiple invocations of
        # infer_type
        assert self._type is None
        self._type = self._infer_type(type_env)
        return self

    def typecheck(self, expected : T.Base) -> TVBase:
        """
        Check that this expression's type is, or can be coerced to,
        `expected`. 

        :raise WDL.Error.StaticTypeMismatch:
        :return: `self`
        """
        if self.type != expected:
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
        super().__init__(pos)
        self._literal = literal
    def _infer_type(self, type_env : TypeEnv) -> T.Base:
        return T.Boolean()
    def eval(self, env : Env) -> V.Boolean:
        return V.Boolean(self._literal)

# Integer literal
class Int(Base):
    _literal : int
    def __init__(self, pos : SourcePosition, literal : int) -> None:
        super().__init__(pos)
        self._literal = literal
    def _infer_type(self, type_env : TypeEnv) -> T.Base:
        return T.Int()
    def typecheck(self, expected : T.Base) -> TVBase:
        """An ``Int`` expression can be coerced to ``Float`` when context demands."""
        if expected == T.Float():
            return self
        return super().typecheck(expected)
    def eval(self, env : Env) -> V.Int:
        return V.Int(self._literal)

# Float literal
class Float(Base):
    _literal : float
    def __init__(self, pos : SourcePosition, literal : float) -> None:
        super().__init__(pos)
        self._literal = literal
    def _infer_type(self, type_env : TypeEnv) -> T.Base:
        return T.Float()
    def eval(self, env : Env) -> V.Float:
        return V.Float(self._literal)

class String(Base):
    """Strings include literals possibly interleaved with interpolated expressions"""
    _parts : List[Union[str,Base]]
    def __init__(self, pos : SourcePosition, parts : List[Union[str,Base]]) -> None:
        super().__init__(pos)
        self._parts = parts
    def _infer_type(self, type_env : TypeEnv) -> T.Base:
        for part in self._parts:
            if isinstance(part, Base):
                # TODO: make sure it will make sense to coerce this to a string
                part.infer_type(type_env)
        return T.String()
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

    def __init__(self, pos : SourcePosition, items : List[Base]) -> None:
        super(Array, self).__init__(pos)
        self.items = items

    def _infer_type(self, type_env : TypeEnv) -> T.Base:
        if len(self.items) == 0:
            return T.Array(None)
        for item in self.items:
            item.infer_type(type_env)
        # Use the type of the first item as the assumed item type
        item_type = self.items[0].type
        # Except, allow a mixture of Int and Float to construct Array[Float]
        if item_type == T.Int():
            for item in self.items:
                if item.type == T.Float():
                    item_type = T.Float()
        # Check all items are compatible with this item type
        for item in self.items:
            try:
                item.typecheck(item_type)
            except Error.StaticTypeMismatch:
                raise Error.StaticTypeMismatch(self, item_type, item.type, "inconsistent types within array") from None
        return T.Array(item_type)

    def typecheck(self, expected : Optional[T.Base]) -> Base:
        if len(self.items) == 0 and isinstance(expected, T.Array):
            # the empty array satisfies any array type
            return self
        return super().typecheck(expected) # pyre-ignore

    def eval(self, env : Env) -> V.Array:
        assert isinstance(self.type, T.Array)
        return V.Array(self.type, [item.eval(env).coerce(self.type.item_type) for item in self.items])

# If
class IfThenElse(Base):
    condition : Base
    """A Boolean expression for the condition"""

    consequent : Base
    """Expression evaluated when the condition is true"""

    alternative : Base
    """Expression evaluated when the condition is false"""

    def __init__(self, pos : SourcePosition, condition : Base, consequent : Base, alternative : Base) -> None:
        super().__init__(pos)
        self.condition = condition
        self.consequent = consequent
        self.alternative = alternative

    def _infer_type(self, type_env : TypeEnv) -> T.Base:
        if self.condition.infer_type(type_env).type != T.Boolean():
            raise Error.StaticTypeMismatch(self, T.Boolean(), self.condition.type, "in if condition")
        self_type = self.consequent.infer_type(type_env).type
        assert isinstance(self_type, T.Base) # pyre-ignore
        self.alternative.infer_type(type_env)
        if self_type == T.Int() and self.alternative.type == T.Float():
            self_type = T.Float()
        try:
            self.alternative.typecheck(self_type)
        except Error.StaticTypeMismatch:
            raise Error.StaticTypeMismatch(self, self.consequent.type, self.alternative.type,
                                           "if consequent & alternative must have the same type")
        return self_type
    
    def eval(self, env : Env) -> V.Base:
        if self.condition.eval(env).expect(T.Boolean()).value != False:
            ans = self.consequent.eval(env)
        else:
            ans = self.alternative.eval(env)
        return ans

# function applications

# Abstract interface to an internal function implementation
# (see StdLib.py for concrete implementations)
class _Function(ABC):

    # Typecheck the function invocation (incl. argument expressions); raise an
    # exception or return the type of the value that the function will return
    @abstractmethod
    def infer_type(self, expr : TVApply) -> T.Base:
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
        super().__init__(pos)
        try:
            self.function = _stdlib[function]
        except KeyError:
            raise Error.NoSuchFunction(self, function) from None
        self.arguments = arguments

    def _infer_type(self, type_env : TypeEnv) -> T.Base:
        for arg in self.arguments:
            arg.infer_type(type_env)
        return self.function.infer_type(self)

    def eval(self, env : Env) -> V.Base:
        return self.function(self, env)


# Namespaced identifiers

class Ident(Base):
    """An identifier expected to resolve in the environment given during evaluation"""
    namespace : List[str]
    identifier : str

    def __init__(self, pos : SourcePosition, parts : List[str], type_env : TypeEnv) -> None:
        super().__init__(pos)
        assert len(parts) > 0
        self.identifier = parts[-1]
        self.namespace = parts[:-1]
        assert self.namespace == [] # placeholder

    def _infer_type(self, type_env : TypeEnv) -> T.Base:
        try:
            return type_env[self.identifier]
        except KeyError:
            raise Error.UnknownIdentifier(self)

    def eval(self, env : Env) -> V.Base:
        try:
            # TODO: handling missing values
            return env[self.identifier]
        except KeyError:
            raise Error.UnknownIdentifier(self)
