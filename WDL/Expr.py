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
from typing import Any, List, Optional, Dict, Callable, TypeVar, Tuple, Union
import WDL.Type as T
import WDL.Value as V
import WDL.Env as Env

# Forward-declare certain types needed for Error definitions, then import them.
TVApply = TypeVar('TVApply', bound='Apply')
TVIdent = TypeVar('TVIdent', bound='Ident')
import WDL.Error as Error
from WDL.Error import SourcePosition, SourceNode

TVBase = TypeVar('TVBase', bound='Base')
class Base(SourceNode, ABC):
    """Superclass of all expression AST nodes"""
    _type : Optional[T.Base] = None
    def __init__(self, pos : SourcePosition) -> None:
        super().__init__(pos)

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
    def _infer_type(self, type_env : Env.Types) -> T.Base:
        pass

    def infer_type(self, type_env : Env.Types) -> TVBase:
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
        if not self.type.coerces(expected):
            raise Error.StaticTypeMismatch(self, expected, self.type)
        return self

    @abstractmethod
    def eval(self, env : Env.Values) -> V.Base:
        """Evaluate the expression in the given environment"""
        pass

# Boolean literal
class Boolean(Base):
    value : bool
    """Literal value"""
    def __init__(self, pos : SourcePosition, literal : bool) -> None:
        super().__init__(pos)
        self.value = literal
    def _infer_type(self, type_env : Env.Types) -> T.Base:
        return T.Boolean()
    def eval(self, env : Env.Values) -> V.Boolean:
        return V.Boolean(self.value)

# Integer literal
class Int(Base):
    value : int
    """Literal value"""
    def __init__(self, pos : SourcePosition, literal : int) -> None:
        super().__init__(pos)
        self.value = literal
    def _infer_type(self, type_env : Env.Types) -> T.Base:
        return T.Int()
    def eval(self, env : Env.Values) -> V.Int:
        return V.Int(self.value)

# Float literal
class Float(Base):
    value : float
    """Literal value"""
    def __init__(self, pos : SourcePosition, literal : float) -> None:
        super().__init__(pos)
        self.value = literal
    def _infer_type(self, type_env : Env.Types) -> T.Base:
        return T.Float()
    def eval(self, env : Env.Values) -> V.Float:
        return V.Float(self.value)

class Placeholder(Base):
    """Expression interpolated within a string or command"""
    options : Dict[str,str]
    """Placeholder options (sep, true, false, default)"""
    expr : Base
    """Expression for evaluation"""
    def __init__(self, pos : SourcePosition, options : Dict[str,str], expr : Base) -> None:
        super().__init__(pos)
        self.options = options
        self.expr = expr
    def _infer_type(self, type_env : Env.Types) -> T.Base:
        self.expr.infer_type(type_env)
        if isinstance(self.expr.type, T.Array):
            if 'sep' not in self.options:
                raise Error.StaticTypeMismatch(self, T.Array(None), self.expr.type, "array command placeholder must have 'sep'")
            #if sum(1 for t in [T.Int, T.Float, T.Boolean, T.String, T.File] if isinstance(self.expr.type.item_type, t)) == 0:
            #    raise Error.StaticTypeMismatch(self, T.Array(None), self.expr.type, "cannot use array of complex types for command placeholder")
        elif 'sep' in self.options:
                raise Error.StaticTypeMismatch(self, T.Array(None), self.expr.type, "command placeholder has 'sep' option for non-Array expression")
        if ('true' in self.options or 'false' in self.options):
            if not isinstance(self.expr.type, T.Boolean):
                raise Error.StaticTypeMismatch(self, T.Boolean(), self.expr.type, "command placeholder 'true' and 'false' options used with non-Boolean expression")
            if not ('true' in self.options and 'false' in self.options):
                raise Error.StaticTypeMismatch(self, T.Boolean(), self.expr.type, "command placeholder with only one of 'true' and 'false' options")
        return T.String()
    def eval(self, env : Env.Values) -> V.String:
        v = self.expr.eval(env)
        if isinstance(v, V.Null):
            if 'default' in self.options:
                return V.String(self.options['default'])
            return V.String('')
        if isinstance(v, V.String):
            return v
        if isinstance(v, V.Array):
            return V.String(self.options['sep'].join(str(item.value) for item in v.value))
        if v == V.Boolean(True) and 'true' in self.options:
            return V.String(self.options['true'])
        if v == V.Boolean(False) and 'false' in self.options:
            return V.String(self.options['false'])
        return V.String(str(v))

class String(Base):
    """Text possibly interleaved with expression placeholders for interpolation"""
    parts : List[Union[str,Placeholder]]
    """The parts list begins and ends with matching single- or double- quote marks. Between these is a sequence of literal strings and/or interleaved placeholder expressions. Escape sequences in the literals have NOT been decoded."""
    def __init__(self, pos : SourcePosition, parts : List[Union[str,Placeholder]]) -> None:
        super().__init__(pos)
        self.parts = parts
    def _infer_type(self, type_env : Env.Types) -> T.Base:
        for part in self.parts:
            if isinstance(part, Placeholder):
                part.infer_type(type_env)
        return T.String()
    def typecheck(self, expected : Optional[T.Base]) -> Base:
        return super().typecheck(expected) # pyre-ignore
    def eval(self, env : Env.Values) -> V.String:
        ans = []
        for part in self.parts:
            if isinstance(part, Placeholder):
                # evaluate interpolated expression & stringify
                ans.append(part.eval(env).value)
            elif type(part) == str:
                # use python builtins to decode escape sequences
                ans.append(str.encode(part).decode('unicode_escape'))
            else:
                assert False
        # concatenate the stringified parts and trim the surrounding quotes
        return V.String(''.join(ans)[1:-1]) # pyre-ignore

# Array
class Array(Base):
    items : List[Base]
    """Expression for each item in the array"""

    def __init__(self, pos : SourcePosition, items : List[Base]) -> None:
        super(Array, self).__init__(pos)
        self.items = items

    def _infer_type(self, type_env : Env.Types) -> T.Base:
        if len(self.items) == 0:
            return T.Array(None)
        for item in self.items:
            item.infer_type(type_env)
        # Use the type of the first item as the assumed item type
        item_type = self.items[0].type
        # Except, allow a mixture of Int and Float to construct Array[Float]
        if isinstance(item_type, T.Int):
            for item in self.items:
                if isinstance(item.type, T.Float):
                    item_type = T.Float()
        # Check all items are compatible with this item type
        for item in self.items:
            try:
                item.typecheck(item_type)
            except Error.StaticTypeMismatch:
                raise Error.StaticTypeMismatch(self, item_type, item.type, "inconsistent types within array") from None
            if item.type.optional:
                item_type.optional = True
        return T.Array(item_type, False, True)

    def typecheck(self, expected : Optional[T.Base]) -> Base:
        if len(self.items) == 0 and isinstance(expected, T.Array):
            # the literal empty array satisfies any array type
            # (unless it has the nonempty quantifier)
            if expected.nonempty:
                raise Error.EmptyArray(self)
            return self
        return super().typecheck(expected) # pyre-ignore

    def eval(self, env : Env.Values) -> V.Array:
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

    def _infer_type(self, type_env : Env.Types) -> T.Base:
        if self.condition.infer_type(type_env).type != T.Boolean():
            raise Error.StaticTypeMismatch(self, T.Boolean(), self.condition.type, "in if condition")
        self_type = self.consequent.infer_type(type_env).type
        assert isinstance(self_type, T.Base)
        self.alternative.infer_type(type_env)
        if isinstance(self_type, T.Int) and isinstance(self.alternative.type, T.Float):
            self_type = T.Float()
        try:
            self.alternative.typecheck(self_type)
        except Error.StaticTypeMismatch:
            raise Error.StaticTypeMismatch(self, self.consequent.type, self.alternative.type,
                                           "if consequent & alternative must have the same type") from None
        return self_type
    
    def eval(self, env : Env.Values) -> V.Base:
        try:
            if self.condition.eval(env).expect(T.Boolean()).value != False:
                ans = self.consequent.eval(env)
            else:
                ans = self.alternative.eval(env)
            return ans
        except ReferenceError:
            raise Error.NullValue(self) from None

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
    def __call__(self, expr : TVApply, env : Env.Values) -> V.Base:
        pass
# Table of standard library functions, filled in below and in StdLib.py
_stdlib : Dict[str,_Function] = {}

class Apply(Base):
    """Application of a built-in or standard library function"""
    function_name : str
    function : _Function
    arguments : List[Base]

    def __init__(self, pos : SourcePosition, function : str, arguments : List[Base]) -> None:
        super().__init__(pos)
        try:
            self.function = _stdlib[function]
            self.function_name = function
        except KeyError:
            raise Error.NoSuchFunction(self, function) from None
        self.arguments = arguments

    def _infer_type(self, type_env : Env.Types) -> T.Base:
        for arg in self.arguments:
            arg.infer_type(type_env)
        return self.function.infer_type(self)

    def eval(self, env : Env.Values) -> V.Base:
        return self.function(self, env)


# Namespaced identifiers

class Ident(Base):
    """An identifier expected to resolve in the environment given during evaluation"""
    namespace : List[str]
    name : str

    def __init__(self, pos : SourcePosition, parts : List[str]) -> None:
        super().__init__(pos)
        assert len(parts) > 0
        self.name = parts[-1]
        self.namespace = parts[:-1]

    def _infer_type(self, type_env : Env.Types) -> T.Base:
        if len(self.namespace) > 0 and (self.name in ['left', 'right']):
            # Special case for pair access, IDENT.left or IDENT.right
            # Pair access through non-identifier expressions goes a different
            # path, through the get_left and get_right terminals.
            # TODO: avoid having two paths by ensuring .left and .right can't
            #       parse as Ident
            pair_name = self.namespace[-1]
            pair_namespace = self.namespace[:-1]
            try:
                ans : T.Base = Env.resolve(type_env, pair_namespace, pair_name)
            except KeyError:
                pass
            if isinstance(ans, T.Pair):
                return ans.left_type if self.name == 'left' else ans.right_type
        try:
            ans : T.Base = Env.resolve(type_env, self.namespace, self.name)
            return ans
        except KeyError:
            raise Error.UnknownIdentifier(self) from None

    def eval(self, env : Env.Values) -> V.Base:
        if len(self.namespace) > 0 and (self.name in ['left', 'right']):
            pair_name = self.namespace[-1]
            pair_namespace = self.namespace[:-1]
            try:
                ans : V.Base = Env.resolve(env, pair_namespace, pair_name)
                return ans
            except KeyError:
                pass
        try:
            ans : V.Base = Env.resolve(env, self.namespace, self.name)
            return ans
        except KeyError:
            raise Error.UnknownIdentifier(self) from None
