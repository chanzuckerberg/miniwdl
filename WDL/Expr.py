# pyre-strict
"""
WDL expressions: literal values, arithmetic, comparison, conditionals, string
interpolation, arrays & maps, standard library functions

The abstract syntax tree (AST) for any expression is represented by an instance
of a Python class deriving from ``WDL.Expr.Base``. Any such node may have other
nodes attached "beneath" it. An expression can be evaluated to a Value given
a suitable Env.
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, TypeVar, Tuple, Union, Any
import WDL.Type as T
import WDL.Value as V
import WDL.Env as Env
from WDL.Error import SourcePosition, SourceNode
import WDL.Error as Error

TVBase = TypeVar("TVBase", bound="Base")


class Base(SourceNode, ABC):
    """Superclass of all expression AST nodes"""

    _type: Optional[T.Base] = None

    def __init__(self, pos: SourcePosition) -> None:
        super().__init__(pos)

    @property
    def type(self) -> T.Base:
        """
        :type: WDL.Type.Base

        WDL type of this expression. Undefined on construction; populated by one
        invocation of ``infer_type``.
        """
        # Failure of this assertion indicates use of an Expr object without
        # first calling _infer_type
        assert self._type is not None
        return self._type

    @abstractmethod
    def _infer_type(self, type_env: Env.Types) -> T.Base:
        pass

    def infer_type(self, type_env: Env.Types) -> TVBase:
        """infer_type(self, type_env : Env.Types) -> WDL.Expr.Base

        Infer the expression's type within the given type environment. Must be
        invoked exactly once prior to use of other methods.

        :raise WDL.Error.StaticTypeMismatch: when the expression fails to type-check
        :return: `self`
        """
        # Failure of this assertion indicates multiple invocations of
        # infer_type
        assert self._type is None
        self._type = self._infer_type(type_env)
        assert isinstance(self.type, T.Base), str(self.pos)
        return self

    def typecheck(self, expected: T.Base) -> TVBase:
        """typecheck(self, expected : T.Base) -> WDL.Expr.Base

        Check that this expression's type is, or can be coerced to,
        ``expected``.

        :raise WDL.Error.StaticTypeMismatch:
        :return: `self`
        """
        if not self.type.coerces(expected):
            raise Error.StaticTypeMismatch(self, expected, self.type)
        return self

    @abstractmethod
    def eval(self, env: Env.Values) -> V.Base:
        """Evaluate the expression in the given environment"""
        pass


# Boolean literal


class Boolean(Base):
    value: bool
    """
    :type: bool

    Literal value
    """

    def __init__(self, pos: SourcePosition, literal: bool) -> None:
        super().__init__(pos)
        self.value = literal

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        return T.Boolean()

    def eval(self, env: Env.Values) -> V.Boolean:
        ""
        return V.Boolean(self.value)


# Integer literal


class Int(Base):
    value: int
    """
    :type: int

    Literal value
    """

    def __init__(self, pos: SourcePosition, literal: int) -> None:
        super().__init__(pos)
        self.value = literal

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        return T.Int()

    def eval(self, env: Env.Values) -> V.Int:
        ""
        return V.Int(self.value)


# Float literal


class Float(Base):
    value: float
    """
    :type: float

    Literal value
    """

    def __init__(self, pos: SourcePosition, literal: float) -> None:
        super().__init__(pos)
        self.value = literal

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        return T.Float()

    def eval(self, env: Env.Values) -> V.Float:
        ""
        return V.Float(self.value)


class Placeholder(Base):
    """Expression interpolated within a string or command"""

    options: Dict[str, str]
    """
    :type: Dict[str,str]

    Placeholder options (sep, true, false, default)"""

    expr: Base
    """
    :type: WDL.Expr.Base

    Expression for evaluation
    """

    def __init__(self, pos: SourcePosition, options: Dict[str, str], expr: Base) -> None:
        super().__init__(pos)
        self.options = options
        self.expr = expr

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        self.expr.infer_type(type_env)
        if isinstance(self.expr.type, T.Array):
            if "sep" not in self.options:
                raise Error.StaticTypeMismatch(
                    self, T.Array(None), self.expr.type, "array command placeholder must have 'sep'"
                )
            # if sum(1 for t in [T.Int, T.Float, T.Boolean, T.String, T.File] if isinstance(self.expr.type.item_type, t)) == 0:
            #    raise Error.StaticTypeMismatch(self, T.Array(None), self.expr.type, "cannot use array of complex types for command placeholder")
        elif "sep" in self.options:
            raise Error.StaticTypeMismatch(
                self,
                T.Array(None),
                self.expr.type,
                "command placeholder has 'sep' option for non-Array expression",
            )
        if "true" in self.options or "false" in self.options:
            if not isinstance(self.expr.type, T.Boolean):
                raise Error.StaticTypeMismatch(
                    self,
                    T.Boolean(),
                    self.expr.type,
                    "command placeholder 'true' and 'false' options used with non-Boolean expression",
                )
            if not ("true" in self.options and "false" in self.options):
                raise Error.StaticTypeMismatch(
                    self,
                    T.Boolean(),
                    self.expr.type,
                    "command placeholder with only one of 'true' and 'false' options",
                )
        return T.String()

    def eval(self, env: Env.Values) -> V.String:
        ""
        v = self.expr.eval(env)
        if isinstance(v, V.Null):
            if "default" in self.options:
                return V.String(self.options["default"])
            return V.String("")
        if isinstance(v, V.String):
            return v
        if isinstance(v, V.Array):
            return V.String(self.options["sep"].join(str(item.value) for item in v.value))
        if v == V.Boolean(True) and "true" in self.options:
            return V.String(self.options["true"])
        if v == V.Boolean(False) and "false" in self.options:
            return V.String(self.options["false"])
        return V.String(str(v))


class String(Base):
    """Text, possibly interleaved with expression placeholders for interpolation"""

    parts: List[Union[str, Placeholder]]
    """
    :type: List[Union[str,WDL.Expr.Placeholder]]

    The parts list begins and ends with matching single- or double- quote
    marks. Between these is a sequence of literal strings and/or
    interleaved placeholder expressions. Escape sequences in the literals
    have NOT been decoded.
    """

    def __init__(self, pos: SourcePosition, parts: List[Union[str, Placeholder]]) -> None:
        super().__init__(pos)
        self.parts = parts

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        for part in self.parts:
            if isinstance(part, Placeholder):
                part.infer_type(type_env)
        return T.String()

    def typecheck(self, expected: Optional[T.Base]) -> Base:
        ""
        return super().typecheck(expected)  # pyre-ignore

    def eval(self, env: Env.Values) -> V.String:
        ""
        ans = []
        for part in self.parts:
            if isinstance(part, Placeholder):
                # evaluate interpolated expression & stringify
                ans.append(part.eval(env).value)
            elif isinstance(part, str):
                # use python builtins to decode escape sequences
                ans.append(str.encode(part).decode("unicode_escape"))
            else:
                assert False
        # concatenate the stringified parts and trim the surrounding quotes
        return V.String("".join(ans)[1:-1])  # pyre-ignore


# Array


class Array(Base):
    items: List[Base]
    """
    :type: List[WDL.Expr.Base]

    Expression for each item in the array literal
    """

    def __init__(self, pos: SourcePosition, items: List[Base]) -> None:
        super(Array, self).__init__(pos)
        self.items = items

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        if not self.items:
            return T.Array(None)
        for item in self.items:
            item.infer_type(type_env)
        # Start by assuming the type of the first item is the item type
        item_type: T.Base = self.items[0].type
        # Allow a mixture of Int and Float to construct Array[Float]
        if isinstance(item_type, T.Int):
            for item in self.items:
                if isinstance(item.type, T.Float):
                    item_type = T.Float()
        # If any item is String, assume item type is String
        # If any item has optional quantifier, assume item type is optional
        # If all items have nonempty quantifier, assume item type is nonempty
        all_nonempty = len(self.items) > 0
        for item in self.items:
            if isinstance(item.type, T.String):
                item_type = T.String(optional=item_type.optional)
            if item.type.optional:
                item_type = item_type.copy(optional=True)
            if isinstance(item.type, T.Array) and not item.type.nonempty:
                all_nonempty = False
        if isinstance(item_type, T.Array):
            item_type = item_type.copy(nonempty=all_nonempty)
        # Check all items are coercible to item_type
        for item in self.items:
            try:
                item.typecheck(item_type)
            except Error.StaticTypeMismatch:
                self._type = T.Array(item_type, optional=False, nonempty=True)
                raise Error.StaticTypeMismatch(
                    self, item_type, item.type, "(inconsistent types within array)"
                ) from None
        return T.Array(item_type, optional=False, nonempty=True)

    def typecheck(self, expected: Optional[T.Base]) -> Base:
        ""
        if not self.items and isinstance(expected, T.Array):
            # the literal empty array satisfies any array type
            # (unless it has the nonempty quantifier)
            if expected.nonempty:
                raise Error.EmptyArray(self)
            return self
        return super().typecheck(expected)  # pyre-ignore

    def eval(self, env: Env.Values) -> V.Array:
        ""
        assert isinstance(self.type, T.Array)
        return V.Array(
            self.type, [item.eval(env).coerce(self.type.item_type) for item in self.items]
        )


# If


class IfThenElse(Base):
    condition: Base
    """
    :type: WDL.Expr.Base

    A Boolean expression for the condition
    """

    consequent: Base
    """
    :type: WDL.Expr.Base

    Expression evaluated when the condition is true
    """

    alternative: Base
    """
    :type: WDL.Expr.Base

    Expression evaluated when the condition is false
    """

    def __init__(
        self, pos: SourcePosition, condition: Base, consequent: Base, alternative: Base
    ) -> None:
        super().__init__(pos)
        self.condition = condition
        self.consequent = consequent
        self.alternative = alternative

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        # check for Boolean condition
        if self.condition.infer_type(type_env).type != T.Boolean():
            raise Error.StaticTypeMismatch(
                self, T.Boolean(), self.condition.type, "in if condition"
            )
        # Unify consequent & alternative types. Subtleties:
        # 1. If either is optional, unify to optional
        # 2. If one is Int and the other is Float, unify to Float
        # 3. If one is a nonempty array and the other is a possibly empty
        #    array, unify to possibly empty array
        # 4. Given the specific construct,
        #      if defined(x) then EXPR_WITH_x else SOME_DEFAULT
        #    where x: T?, assume x: T when we infer the type of EXPR_WITH_x
        #    cf. https://github.com/openwdl/wdl/issues/271
        consequent_type_env = type_env
        if (
            isinstance(self.condition, Apply)
            and self.condition.function_name == "defined"
            and len(self.condition.arguments) == 1
            and isinstance(self.condition.arguments[0], Ident)
        ):
            arg: Ident = self.condition.arguments[0]
            consequent_type_env = _retype(
                consequent_type_env, arg.namespace, arg.name, arg.type.copy(optional=False)
            )
        self_type = self.consequent.infer_type(consequent_type_env).type
        assert isinstance(self_type, T.Base)
        self.alternative.infer_type(type_env)
        if isinstance(self_type, T.Int) and isinstance(self.alternative.type, T.Float):
            self_type = T.Float(optional=self_type.optional)
        if self.alternative.type.optional:
            self_type = self_type.copy(optional=True)
        if (
            isinstance(self_type, T.Array)
            and isinstance(self.consequent.type, T.Array)
            and isinstance(self.alternative.type, T.Array)
        ):
            self_type = self_type.copy(
                nonempty=(  # pyre-ignore
                    self.consequent.type.nonempty and self.alternative.type.nonempty  # pyre-ignore
                )
            )
        try:
            self.consequent.typecheck(self_type)
            self.alternative.typecheck(self_type)
        except Error.StaticTypeMismatch:
            raise Error.StaticTypeMismatch(
                self,
                self.consequent.type,  # pyre-ignore
                self.alternative.type,
                " (if consequent & alternative must have the same type)",
            ) from None
        return self_type

    def eval(self, env: Env.Values) -> V.Base:
        ""
        try:
            if self.condition.eval(env).expect(T.Boolean()).value:
                ans = self.consequent.eval(env)
            else:
                ans = self.alternative.eval(env)
            return ans
        except ReferenceError:
            raise Error.NullValue(self) from None


def _retype(type_env: Env.Types, namespace: List[str], name: str, new_type: T.Base) -> Env.Types:
    # Helper function: return type_env with a new type for one particular
    # binding (and everything else the same)
    ans = []
    for node in type_env:
        if isinstance(node, Env.Binding):
            if not namespace and name == node.name:
                ans.append(Env.Binding(node.name, new_type))
            else:
                ans.append(node)
        elif isinstance(node, Env.Namespace):
            if namespace and namespace[0] == node.namespace:
                ans.append(
                    Env.Namespace(
                        namespace[0], _retype(node.bindings, namespace[1:], name, new_type)
                    )
                )
            else:
                ans.append(node)
        else:
            assert False
    return ans


# function applications

# Abstract interface to an internal function implementation
# (see StdLib.py for concrete implementations)


TVApply = TypeVar("TVApply", bound="Apply")


class _Function(ABC):

    # Typecheck the function invocation (incl. argument expressions); raise an
    # exception or return the type of the value that the function will return
    @abstractmethod
    def infer_type(self, expr: TVApply) -> T.Base:
        pass

    @abstractmethod
    def __call__(self, expr: TVApply, env: Env.Values) -> V.Base:
        pass


# Table of standard library functions, filled in below and in StdLib.py
_stdlib: Dict[str, _Function] = {}


class Apply(Base):
    """Application of a built-in or standard library function"""

    function_name: str
    """Name of the function applied

    :type: str"""
    arguments: List[Base]
    """
    :type: List[WDL.Expr.Base]

    Expressions for each function argument
    """

    function: _Function

    def __init__(self, pos: SourcePosition, function: str, arguments: List[Base]) -> None:
        super().__init__(pos)
        try:
            self.function = _stdlib[function]
            self.function_name = function
        except KeyError:
            raise Error.NoSuchFunction(self, function) from None
        self.arguments = arguments

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        for arg in self.arguments:
            arg.infer_type(type_env)
        return self.function.infer_type(self)

    def eval(self, env: Env.Values) -> V.Base:
        ""
        return self.function(self, env)


# Namespaced identifiers


class Ident(Base):
    """An identifier expected to resolve in the environment given during evaluation"""

    namespace: List[str]
    """
    :type: List[str]

    Namespace (empty for an unqualified name)
    """
    name: str
    ":type: str"

    ctx: Any = None
    """
    After typechecking, stores context about the binding from the type
    environment.

    The ``Tree`` typechecker typically stores here a reference to a ``Decl``
    (for value references in tasks and workflows), a ``Call`` (for references
    to a call output), or a ``Scatter`` (for references to a scatter variable).
    """

    def __init__(self, pos: SourcePosition, parts: List[str]) -> None:
        super().__init__(pos)
        assert parts
        self.name = parts[-1]
        self.namespace = parts[:-1]

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        if self.namespace and (self.name in ["left", "right"]):
            # Special case for pair access, IDENT.left or IDENT.right
            # Pair access through non-identifier expressions goes a different
            # path, through the get_left and get_right terminals.
            # TODO: avoid having two paths by ensuring .left and .right can't
            #       parse as Ident
            pair_name = self.namespace[-1]
            pair_namespace = self.namespace[:-1]
            try:
                ans: T.Base = Env.resolve(type_env, pair_namespace, pair_name)
            except KeyError:
                pass
            if isinstance(ans, T.Pair):
                return ans.left_type if self.name == "left" else ans.right_type
        try:
            ans: T.Base = Env.resolve(type_env, self.namespace, self.name)
        except KeyError:
            raise Error.UnknownIdentifier(self) from None
        self.ctx = Env.resolve_ctx(type_env, self.namespace, self.name)
        return ans

    def eval(self, env: Env.Values) -> V.Base:
        ""
        if self.namespace and (self.name in ["left", "right"]):
            pair_name = self.namespace[-1]
            pair_namespace = self.namespace[:-1]
            try:
                ans: V.Base = Env.resolve(env, pair_namespace, pair_name)
                if isinstance(ans, V.Pair):
                    assert ans.value is not None
                    return ans.value[0] if self.name == "left" else ans.value[1]
            except KeyError:
                pass
        try:
            ans: V.Base = Env.resolve(env, self.namespace, self.name)
            return ans
        except KeyError:
            raise Error.UnknownIdentifier(self) from None


# Pair literal


class Pair(Base):
    left: Base
    """
    :type: WDL.Expr.Base

    Left-hand expression in the pair literal
    """
    right: Base
    """
    :type: WDL.Expr.Base

    Right-hand expression in the pair literal
    """

    def __init__(self, pos: SourcePosition, left: Base, right: Base) -> None:
        super().__init__(pos)
        self.left = left
        self.right = right

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        self.left.infer_type(type_env)
        self.right.infer_type(type_env)
        return T.Pair(self.left.type, self.right.type)

    def eval(self, env: Env.Values) -> V.Base:
        ""
        assert isinstance(self.type, T.Pair)
        lv = self.left.eval(env)
        rv = self.right.eval(env)
        return V.Pair(self.type, (lv, rv))


# Map literal


class Map(Base):
    items: List[Tuple[Base, Base]]
    """
    :type: List[Tuple[WDL.Expr.Base,WDL.Expr.Base]]

    Expressions for the map literal keys and values
    """

    def __init__(self, pos: SourcePosition, items: List[Tuple[Base, Base]]) -> None:
        super().__init__(pos)
        self.items = items

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        kty = None
        vty = None
        for k, v in self.items:
            k.infer_type(type_env)
            if kty is None:
                kty = k.type
            else:
                k.typecheck(kty)
            v.infer_type(type_env)
            if vty is None or vty == T.Array(None) or vty == T.Map(None):
                vty = v.type
            else:
                v.typecheck(vty)
        if kty is None:
            return T.Map(None)
        assert vty is not None
        return T.Map((kty, vty))

    def eval(self, env: Env.Values) -> V.Base:
        ""
        assert isinstance(self.type, T.Map)
        eitems = []
        for k, v in self.items:
            eitems.append((k.eval(env), v.eval(env)))
        # TODO: complain of duplicate keys
        return V.Map(self.type, eitems)
