# pyre-strict
"""
WDL expressions composing literal values, arithmetic, comparison, conditionals,
string interpolation, arrays & maps, and function applications. These appear on
the right-hand side of value declarations and in task command substitutions,
task runtime sections, and workflow scatter and conditional sections.

The abstract syntax tree (AST) for any expression is represented by an instance
of a Python class deriving from ``WDL.Expr.Base``. Any such node may have other
nodes attached "beneath" it. An expression can be evaluated to a ``Value``
given a suitable ``Env.Values``.

.. inheritance-diagram:: WDL.Expr
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, TypeVar, Tuple, Union, Any, Iterable
import WDL.Type as T
import WDL.Value as V
import WDL.Env as Env
from WDL.Error import SourcePosition, SourceNode
import WDL.Error as Error

TVBase = TypeVar("TVBase", bound="Base")


class Base(SourceNode, ABC):
    """Superclass of all expression AST nodes"""

    _type: Optional[T.Base] = None
    _check_quant: bool = True

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
        # Abstract protected method called by infer_type(): return the inferred
        # type with no side-effects, obeying self._check_quant.
        pass

    def infer_type(self, type_env: Env.Types, check_quant: bool = True) -> TVBase:
        """infer_type(self, type_env : Env.Types) -> WDL.Expr.Base

        Infer the expression's type within the given type environment. Must be
        invoked exactly once prior to use of other methods.

        :param check_quant: when ``False``, relaxes static validation of the optional (?) and nonempty (+) type quantifiers when `typecheck()` is called on this expression, so for example type ``T?`` can satisfy an expected type ``T``. Applies recursively to the type inference and checking of any sub-expressions.
        :raise WDL.Error.StaticTypeMismatch: when the expression fails to type-check
        :return: `self`
        """
        # Failure of this assertion indicates multiple invocations of
        # infer_type
        assert self._type is None
        # recursive descent into child expressions
        with Error.multi_context() as errors:
            for child in self.children:
                errors.try1(lambda: child.infer_type(type_env, check_quant))
        # invoke derived-class logic
        self._check_quant = check_quant
        self._type = self._infer_type(type_env)
        assert self._type and isinstance(self.type, T.Base)
        return self

    def typecheck(self, expected: T.Base) -> TVBase:
        """typecheck(self, expected : T.Base) -> WDL.Expr.Base

        Check that this expression's type is, or can be coerced to,
        ``expected``.

        :raise WDL.Error.StaticTypeMismatch:
        :return: `self`
        """
        if not self.type.coerces(expected, self._check_quant):
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

    @property
    def children(self) -> Iterable[SourceNode]:
        yield self.expr

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        if isinstance(self.expr.type, T.Array):
            if "sep" not in self.options:
                raise Error.StaticTypeMismatch(
                    self,
                    T.Array(T.Any()),
                    self.expr.type,
                    "array command placeholder must have 'sep'",
                )
            # if sum(1 for t in [T.Int, T.Float, T.Boolean, T.String, T.File] if isinstance(self.expr.type.item_type, t)) == 0:
            #    raise Error.StaticTypeMismatch(self, T.Array(T.Any()), self.expr.type, "cannot use array of complex types for command placeholder")
        elif "sep" in self.options:
            raise Error.StaticTypeMismatch(
                self,
                T.Array(T.Any()),
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

    @property
    def children(self) -> Iterable[SourceNode]:
        for p in self.parts:
            if isinstance(p, Base):
                yield p

    def _infer_type(self, type_env: Env.Types) -> T.Base:
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

    @property
    def children(self) -> Iterable[SourceNode]:
        for it in self.items:
            yield it

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        if not self.items:
            return T.Array(T.Any())
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
        all_stringifiable = True
        for item in self.items:
            if isinstance(item.type, T.String):
                item_type = T.String(optional=item_type.optional)
            if item.type.optional:
                item_type = item_type.copy(optional=True)
            if isinstance(item.type, T.Array) and not item.type.nonempty:
                all_nonempty = False
            if not item.type.coerces(T.String(optional=True)):
                all_stringifiable = False
        if isinstance(item_type, T.Array):
            item_type = item_type.copy(nonempty=all_nonempty)
        # Check all items are coercible to item_type
        for item in self.items:
            try:
                item.typecheck(item_type)
            except Error.StaticTypeMismatch:
                if all_stringifiable:
                    # Last resort: coerce all to strings if possible
                    return T.Array(
                        T.String(optional=item_type.optional), optional=False, nonempty=True
                    )
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

    @property
    def children(self) -> Iterable[SourceNode]:
        yield self.condition
        yield self.consequent
        yield self.alternative

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        # check for Boolean condition
        if self.condition.type != T.Boolean():
            raise Error.StaticTypeMismatch(
                self, T.Boolean(), self.condition.type, "in if condition"
            )
        # Unify consequent & alternative types. Subtleties:
        # 1. If either is optional, unify to optional
        # 2. If one is Int and the other is Float, unify to Float
        # 3. If one is a nonempty array and the other is a possibly empty
        #    array, unify to possibly empty array
        self_type = self.consequent.type
        assert isinstance(self_type, T.Base)
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

    @property
    def children(self) -> Iterable[SourceNode]:
        for arg in self.arguments:
            yield arg

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        return self.function.infer_type(self)

    def eval(self, env: Env.Values) -> V.Base:
        ""
        return self.function(self, env)


# Namespaced identifiers


class Ident(Base):
    """An identifier referencing a named value or call output"""

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

    @property
    def children(self) -> Iterable[SourceNode]:
        return []

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        try:
            ans: T.Base = Env.resolve(type_env, self.namespace, self.name)
        except KeyError:
            raise Error.UnknownIdentifier(self) from None
        # the ctx for each binding in the type environment should be the
        # originating Decl (for inputs/values) or Call (for call outputs)
        self.ctx = Env.resolve_ctx(type_env, self.namespace, self.name)
        return ans

    def eval(self, env: Env.Values) -> V.Base:
        ""
        try:
            ans: V.Base = Env.resolve(env, self.namespace, self.name)
            return ans
        except KeyError:
            raise Error.UnknownIdentifier(self) from None

    @property
    def _ident(self) -> List[str]:
        return self.namespace + [self.name]


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

    @property
    def children(self) -> Iterable[SourceNode]:
        yield self.left
        yield self.right

    def _infer_type(self, type_env: Env.Types) -> T.Base:
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

    @property
    def children(self) -> Iterable[SourceNode]:
        for k, v in self.items:
            yield k
            yield v

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        kty = None
        vty = None
        for k, v in self.items:
            if kty is None:
                kty = k.type
            else:
                k.typecheck(kty)
            if vty is None or vty == T.Array(T.Any()) or vty == T.Map(None):
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


class _LeftName(Base):
    # TODO document me
    name: str

    def __init__(self, pos: SourcePosition, name: str) -> None:
        super().__init__(pos)
        assert name
        self.name = name

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        raise NotImplementedError()

    def eval(self, env: Env.Values) -> V.Base:
        raise NotImplementedError()

    @property
    def _ident(self) -> List[str]:
        return [self.name]


class Get(Base):
    # TODO document me
    innard: Base
    member: Optional[str]

    def __init__(self, pos: SourcePosition, innard: Base, member: Optional[str]) -> None:
        super().__init__(pos)
        assert innard
        self.innard = innard
        self.member = member

    @property
    def children(self) -> Iterable[SourceNode]:
        if self._type:
            # suppress children until resolution/typechecking is complete
            yield self.innard

    def _infer_type(self, type_env: Env.Types) -> T.Base:
        if isinstance(self.innard, _LeftName):
            # innard is a lone "name" -- try to resolve it as an identifier,
            # and if that works, transform innard to Ident("name")
            try:
                Env.resolve(type_env, [], self.innard.name)
                self.innard = Ident(self.innard.pos, [self.innard.name])
            except KeyError:
                if not self.member:
                    raise Error.UnknownIdentifier(self) from None
        # attempt to typecheck innard, disambiguating whether it's an
        # intermediate value, a resolvable identifier, or neither
        try:
            self.innard.infer_type(type_env, self._check_quant)
        except Error.UnknownIdentifier:
            # Fail...there's one case we MAY be able to rescue, where innard is
            # a namespace, and our member completes the path to a named value.
            if not (
                isinstance(self.innard, (_LeftName, Get)) and self.innard._ident and self.member
            ):
                raise
            # attempt to resolve "innard.member" and if that works, transform
            # innard to Ident("innard.member")
            try:
                Env.resolve(type_env, self.innard._ident, self.member)
            except KeyError:
                raise Error.UnknownIdentifier(self) from None
            self.innard = Ident(self.pos, self._ident)
            self.innard.infer_type(type_env, self._check_quant)
            self.member = None
        # now we've typechecked innard
        assert self.innard.type
        if not self.member:
            # no member to access; just propagate innard type
            assert isinstance(self.innard, Ident)
            return self.innard.type
        # now we expect innard to be a pair or struct, whose member we're
        # accessing
        if not isinstance(self.innard.type, (T.Pair, T.StructInstance)):
            raise Error.NoSuchMember(self, self.member)
        if self._check_quant and self.innard.type.optional:
            raise Error.StaticTypeMismatch(
                self.innard, self.innard.type.copy(optional=False), self.innard.type
            )
        if self.member in ["left", "right"]:
            if not isinstance(self.innard.type, T.Pair):
                raise Error.NoSuchMember(self, self.member)
            return (
                self.innard.type.left_type if self.member == "left" else self.innard.type.right_type
            )
        if isinstance(self.innard.type, T.StructInstance):
            try:
                return self.innard.type.members[self.member]
            except KeyError:
                pass
        raise Error.NoSuchMember(self, self.member)

    def eval(self, env: Env.Values) -> V.Base:
        innard_value = self.innard.eval(env)
        if not self.member:
            return innard_value
        if isinstance(innard_value, V.Pair):
            assert self.member in ["left", "right"]
            return innard_value.value[0 if self.member == "left" else 1]
        raise NotImplementedError()

    @property
    def _ident(self) -> List[str]:
        if isinstance(self.innard, (_LeftName, Get)) and self.innard._ident:
            return self.innard._ident + ([self.member] if self.member else [])
        return []
