"""
WDL expressions composing literal values, arithmetic, comparison, conditionals,
string interpolation, arrays & maps, and function applications. These appear on
the right-hand side of value declarations and in task command substitutions,
task runtime sections, and workflow scatter and conditional sections.

The abstract syntax tree (AST) for any expression is represented by an instance
of a Python class deriving from ``WDL.Expr.Base``. Any such node may have other
nodes attached "beneath" it. An expression can be evaluated to a ``Value``
given a suitable ``WDL.Env.Values``.

.. inheritance-diagram:: WDL.Expr
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, TypeVar, Tuple, Union, Any, Iterable
from .Error import SourcePosition, SourceNode
from . import Type, Value, Env, Error, StdLib


class Base(SourceNode, ABC):
    """Superclass of all expression AST nodes"""

    _type: Optional[Type.Base] = None
    _check_quant: bool = True
    _stdlib: "Optional[StdLib.Base]" = None

    @property
    def type(self) -> Type.Base:
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
    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        # Abstract protected method called by infer_type(): return the inferred
        # type with no side-effects, obeying self._check_quant.
        pass

    def infer_type(
        self, type_env: Env.Types, stdlib: "Optional[StdLib.Base]" = None, check_quant: bool = True
    ) -> "Base":
        """infer_type(self, type_env : Env.Types) -> WDL.Expr.Base

        Infer the expression's type within the given type environment. Must be
        invoked exactly once prior to use of other methods.

        :param stdlib: a context-specific standard function library for typechecking
        :param check_quant: when ``False``, disables static validation of the optional (?) type quantifier when `typecheck()` is called on this expression, so for example type ``T?`` can satisfy an expected type ``T``. Applies recursively to the type inference and checking of any sub-expressions.
        :raise WDL.Error.StaticTypeMismatch: when the expression fails to type-check
        :return: `self`
        """
        # Failure of this assertion indicates multiple invocations of
        # infer_type
        assert self._type is None
        # recursive descent into child expressions
        with Error.multi_context() as errors:
            for child in self.children:
                assert isinstance(child, Base)
                errors.try1(lambda: child.infer_type(type_env, stdlib, check_quant))
        # invoke derived-class logic. we pass check_quant and stdlib hackily
        # through instance variables since only some subclasses use them.
        self._check_quant = check_quant
        self._stdlib = stdlib
        self._type = self._infer_type(type_env)
        self._stdlib = None
        assert self._type and isinstance(self.type, Type.Base)
        return self

    def typecheck(self, expected: Type.Base) -> "Base":
        """typecheck(self, expected : Type.Base) -> WDL.Expr.Base

        Check that this expression's type is, or can be coerced to,
        ``expected``.

        :raise WDL.Error.StaticTypeMismatch:
        :return: `self`
        """
        if not self.type.coerces(expected, self._check_quant):
            raise Error.StaticTypeMismatch(self, expected, self.type)
        return self

    @abstractmethod
    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Base:
        # to be overridden by subclasses. eval() calls this and deals with any
        # exceptions raised
        pass

    def eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Base:
        """
        Evaluate the expression in the given environment
        
        :param stdlib: a context-specific standard function library implementation
        """
        try:
            ans = self._eval(env, stdlib)
            ans.expr = self
            return ans
        except Error.RuntimeError:
            raise
        except Exception as exn:
            raise Error.EvalError(self, str(exn)) from exn


class Boolean(Base):
    """
    Boolean literal
    """

    value: bool
    """
    :type: bool

    Literal value
    """

    def __init__(self, pos: SourcePosition, literal: bool) -> None:
        super().__init__(pos)
        self.value = literal

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        return Type.Boolean()

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Boolean:
        ""
        return Value.Boolean(self.value)


class Int(Base):
    """
    Integer literal
    """

    value: int
    """
    :type: int

    Literal value
    """

    def __init__(self, pos: SourcePosition, literal: int) -> None:
        super().__init__(pos)
        self.value = literal

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        return Type.Int()

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Int:
        ""
        return Value.Int(self.value)


# Float literal


class Float(Base):
    """
    Numeric literal
    """

    value: float
    """
    :type: float

    Literal value
    """

    def __init__(self, pos: SourcePosition, literal: float) -> None:
        super().__init__(pos)
        self.value = literal

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        return Type.Float()

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Float:
        ""
        return Value.Float(self.value)


class Placeholder(Base):
    """Holds an expression interpolated within a string or command"""

    options: Dict[str, str]
    """
    :type: Dict[str,str]

    Placeholder options (sep, true, false, default)"""

    expr: Base
    """
    :type: WDL.Expr.Base

    Expression to be evaluated and substituted
    """

    def __init__(self, pos: SourcePosition, options: Dict[str, str], expr: Base) -> None:
        super().__init__(pos)
        self.options = options
        self.expr = expr

    @property
    def children(self) -> Iterable[SourceNode]:
        yield self.expr

    def infer_type(
        self, type_env: Env.Types, stdlib: "Optional[StdLib.Base]" = None, check_quant: bool = True
    ) -> Base:
        # override the + operator with the within-interpolation version which accepts String?
        # operands and produces a String? result
        stdlib = stdlib or StdLib.Base()
        with stdlib._context_override("_add", StdLib.InterpolationAddOperator()):
            return super().infer_type(type_env, stdlib, check_quant)

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        if isinstance(self.expr.type, Type.Array):
            if "sep" not in self.options:
                raise Error.StaticTypeMismatch(
                    self,
                    Type.Array(Type.Any()),
                    self.expr.type,
                    "array command placeholder must have 'sep'",
                )
            # if sum(1 for t in [Type.Int, Type.Float, Type.Boolean, Type.String, Type.File] if isinstance(self.expr.type.item_type, t)) == 0:
            #    raise Error.StaticTypeMismatch(self, Type.Array(Type.Any()), self.expr.type, "cannot use array of complex types for command placeholder")
        elif "sep" in self.options:
            raise Error.StaticTypeMismatch(
                self,
                Type.Array(Type.Any()),
                self.expr.type,
                "command placeholder has 'sep' option for non-Array expression",
            )
        if "true" in self.options or "false" in self.options:
            if not isinstance(self.expr.type, Type.Boolean):
                raise Error.StaticTypeMismatch(
                    self,
                    Type.Boolean(),
                    self.expr.type,
                    "command placeholder 'true' and 'false' options used with non-Boolean expression",
                )
            if not ("true" in self.options and "false" in self.options):
                raise Error.StaticTypeMismatch(
                    self,
                    Type.Boolean(),
                    self.expr.type,
                    "command placeholder with only one of 'true' and 'false' options",
                )
        return Type.String()

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.String:
        ""
        # override the + operator with the within-interpolation version which evaluates to None
        # if either operand is None
        stdlib = stdlib or StdLib.Base()
        with stdlib._context_override("_add", StdLib.InterpolationAddOperator()):
            v = self.expr.eval(env, stdlib)
        if isinstance(v, Value.Null):
            if "default" in self.options:
                return Value.String(self.options["default"])
            return Value.String("")
        if isinstance(v, Value.String):
            return v
        if isinstance(v, Value.Array):
            return Value.String(self.options["sep"].join(str(item.value) for item in v.value))
        if v == Value.Boolean(True) and "true" in self.options:
            return Value.String(self.options["true"])
        if v == Value.Boolean(False) and "false" in self.options:
            return Value.String(self.options["false"])
        return Value.String(str(v))


class String(Base):
    """String literal, possibly interleaved with expression placeholders for interpolation"""

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

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        return Type.String()

    def typecheck(self, expected: Optional[Type.Base]) -> Base:
        ""
        return super().typecheck(expected)  # pyre-ignore

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.String:
        ""
        ans = []
        for part in self.parts:
            if isinstance(part, Placeholder):
                # evaluate interpolated expression & stringify
                ans.append(part.eval(env, stdlib).value)
            elif isinstance(part, str):
                # use python builtins to decode escape sequences
                ans.append(str.encode(part).decode("unicode_escape"))
            else:
                assert False
        # concatenate the stringified parts and trim the surrounding quotes
        return Value.String("".join(ans)[1:-1])


class Array(Base):
    """
    Array literal
    """

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

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        if not self.items:
            return Type.Array(Type.Any())
        # Start by assuming the type of the first item is the item type
        item_type: Type.Base = self.items[0].type
        # Allow a mixture of Int and Float to construct Array[Float]
        if isinstance(item_type, Type.Int):
            for item in self.items:
                if isinstance(item.type, Type.Float):
                    item_type = Type.Float()
        # If any item is String, assume item type is String
        # If any item has optional quantifier, assume item type is optional
        # If all items have nonempty quantifier, assume item type is nonempty
        all_nonempty = len(self.items) > 0
        all_stringifiable = True
        for item in self.items:
            if isinstance(item.type, Type.String):
                item_type = Type.String(optional=item_type.optional)
            if item.type.optional:
                item_type = item_type.copy(optional=True)
            if isinstance(item.type, Type.Array) and not item.type.nonempty:
                all_nonempty = False
            if not item.type.coerces(Type.String(optional=True)):
                all_stringifiable = False
        if isinstance(item_type, Type.Array):
            item_type = item_type.copy(nonempty=all_nonempty)
        # Check all items are coercible to item_type
        for item in self.items:
            try:
                item.typecheck(item_type)
            except Error.StaticTypeMismatch:
                if all_stringifiable:
                    # Last resort: coerce all to strings if possible
                    return Type.Array(
                        Type.String(optional=item_type.optional), optional=False, nonempty=True
                    )
                self._type = Type.Array(item_type, optional=False, nonempty=True)
                raise Error.StaticTypeMismatch(
                    self, item_type, item.type, "(inconsistent types within array)"
                ) from None
        return Type.Array(item_type, optional=False, nonempty=True)

    def typecheck(self, expected: Optional[Type.Base]) -> Base:
        ""
        if not self.items and isinstance(expected, Type.Array):
            # the literal empty array satisfies any array type
            return self
        return super().typecheck(expected)  # pyre-ignore

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Array:
        ""
        assert isinstance(self.type, Type.Array)
        return Value.Array(
            self.type, [item.eval(env, stdlib).coerce(self.type.item_type) for item in self.items]
        )


class Pair(Base):
    """
    Pair literal
    """

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

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        return Type.Pair(self.left.type, self.right.type)

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Base:
        ""
        assert isinstance(self.type, Type.Pair)
        lv = self.left.eval(env, stdlib)
        rv = self.right.eval(env, stdlib)
        return Value.Pair(self.type, (lv, rv))


class Map(Base):
    """
    Map literal
    """

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

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        kty = None
        vty = None
        for k, v in self.items:
            if kty is None:
                kty = k.type
            else:
                k.typecheck(kty)
            if (
                vty is None
                or vty == Type.Array(Type.Any())
                or vty == Type.Map((Type.Any(), Type.Any()))
            ):
                vty = v.type
            else:
                v.typecheck(vty)
        if kty is None:
            return Type.Map((Type.Any(), Type.Any()), literal_keys=set())
        assert vty is not None
        literal_keys = None
        if kty == Type.String():
            # If the keys are string constants, record them in the Type object
            # for potential later use in struct coercion. (Normally the Type
            # encodes the common type of the keys, but not the keys themselves)
            literal_keys = set()
            for k, _ in self.items:
                if (
                    literal_keys is not None
                    and isinstance(k, String)
                    and len(k.parts) == 3
                    and isinstance(k.parts[1], str)
                ):
                    literal_keys.add(k.parts[1])
                else:
                    literal_keys = None
        return Type.Map((kty, vty), literal_keys=literal_keys)

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Base:
        ""
        assert isinstance(self.type, Type.Map)
        eitems = []
        for k, v in self.items:
            eitems.append((k.eval(env, stdlib), v.eval(env, stdlib)))
        # TODO: complain of duplicate keys
        return Value.Map(self.type, eitems)


class Struct(Base):
    """
    Struct literal
    """

    members: Dict[str, Base]
    """
    :type: Dict[str,WDL.Expr.Base]

    The struct literal is modelled initially as a bag of keys and values, which
    can be coerced to a specific struct type during typechecking.
    """

    def __init__(self, pos: SourcePosition, members: List[Tuple[str, Base]]):
        super().__init__(pos)
        self.members = {}
        for (k, v) in members:
            if k in self.members:
                raise Error.MultipleDefinitions(self.pos, "duplicate keys " + k)
            self.members[k] = v

    @property
    def children(self) -> Iterable[SourceNode]:
        return self.members.values()

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        member_types = {}
        for k, v in self.members.items():
            member_types[k] = v.type
        return Type.Object(member_types)

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Base:
        ans = {}
        for k, v in self.members.items():
            ans[k] = v.eval(env, stdlib)
        assert isinstance(self.type, Type.Object)
        return Value.Struct(self.type, ans)


class IfThenElse(Base):
    """
    Ternary conditional expression
    """

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

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        # check for Boolean condition
        if self.condition.type != Type.Boolean():
            raise Error.StaticTypeMismatch(
                self, Type.Boolean(), self.condition.type, "in if condition"
            )
        # Unify consequent & alternative types. Subtleties:
        # 1. If either is optional, unify to optional
        # 2. If one is Int and the other is Float, unify to Float
        # 3. If one is a nonempty array and the other is a possibly empty
        #    array, unify to possibly empty array
        self_type = self.consequent.type
        assert isinstance(self_type, Type.Base)
        if isinstance(self_type, Type.Int) and isinstance(self.alternative.type, Type.Float):
            self_type = Type.Float(optional=self_type.optional)
        if self.alternative.type.optional:
            self_type = self_type.copy(optional=True)
        if (
            isinstance(self_type, Type.Array)
            and isinstance(self.consequent.type, Type.Array)
            and isinstance(self.alternative.type, Type.Array)
        ):
            self_type = self_type.copy(
                nonempty=(self.consequent.type.nonempty and self.alternative.type.nonempty)
            )
        try:
            self.consequent.typecheck(self_type)
            self.alternative.typecheck(self_type)
        except Error.StaticTypeMismatch:
            raise Error.StaticTypeMismatch(
                self,
                self.consequent.type,
                self.alternative.type,
                " (if consequent & alternative must have the same type)",
            ) from None
        return self_type

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Base:
        ""
        if self.condition.eval(env, stdlib).expect(Type.Boolean()).value:
            ans = self.consequent.eval(env, stdlib)
        else:
            ans = self.alternative.eval(env, stdlib)
        return ans


class Ident(Base):
    """
    An identifier referencing a named value or call output.

    ``Ident`` nodes are wrapped in ``Get`` nodes, as discussed below.
    """

    namespace: List[str]
    """
    :type: List[str]

    Namespace (empty for an unqualified name)
    """
    name: str
    ":type: str"

    referee: Optional[SourceNode]
    """
    After typechecking within a task or workflow, stores the AST node to which the identifier
    refers: a ``WDL.Tree.Decl`` for value references; a ``WDL.Tree.Call`` for call outputs; or a
    ``WDL.Tree.Scatter`` for scatter variables.
    """

    def __init__(self, pos: SourcePosition, parts: List[str]) -> None:
        super().__init__(pos)
        assert parts
        self.name = parts[-1]
        self.namespace = parts[:-1]
        self.referee = None

    @property
    def children(self) -> Iterable[SourceNode]:
        return []

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        # The following Env.resolve will never fail, as Get._infer_type does
        # the heavy lifting for us.
        ans: Type.Base = Env.resolve(type_env, self.namespace, self.name)
        # referee comes from the type environment's context values
        referee = Env.resolve_ctx(type_env, self.namespace, self.name)
        if referee:
            assert isinstance(referee, SourceNode)
            assert referee.__class__.__name__ in ["Decl", "Call", "Scatter"]
            self.referee = referee
        return ans

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Base:
        ""
        ans: Value.Base = Env.resolve(env, self.namespace, self.name)
        return ans

    @property
    def _ident(self) -> List[str]:
        return self.namespace + [self.name]


class _LeftName(Base):
    # This AST node is a placeholder involved in disambiguating dot-separated
    # identifiers (e.g. "leftname.midname.rightname") as elaborated in the Get
    # docstring below. The parser, lacking the context to resolve this syntax,
    # creates this node simply to represent the leftmost (sometimes only) name,
    # as the innard of a Get node, potentially (not necessarily) with a
    # member name. Later during typechecking, Get._infer_type folds _LeftName
    # into an `Ident` expression; the library user should never have to work
    # with _LeftName.
    name: str

    def __init__(self, pos: SourcePosition, name: str) -> None:
        super().__init__(pos)
        assert name
        self.name = name

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        raise NotImplementedError()

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Base:
        raise NotImplementedError()

    @property
    def _ident(self) -> List[str]:
        return [self.name]


class Get(Base):
    """
    AST node representing access to a value by identifier (including namespaced
    ones), or accessing a member of a pair or struct as ``.member``.

    The entaglement of these two cases is inherent in WDL. Consider the syntax
    ``leftname.midname.rightname``. One interpretation is that ``leftname`` is
    an identifier for a struct value, and ``.midname.rightname`` represents a
    chain of struct member accesses. But another possibility is that
    ``leftname`` is a call, ``midname`` is a struct output of that call, and
    ``rightname`` is a member of that struct. These cases can't be
    distinguished by the syntax parser alone, but must be resolved during
    typechecking with reference to the calls and identifiers available in the
    environment.

    The typechecker does conveniently resolve such cases, and to minimize the
    extent to which it has to restructure the AST in doing so, all identifiers
    (with or without a namespace) are represented as a ``Get`` node wrapping an
    ``Ident`` node. The ``Get`` node may specify a member name to access, but
    may not if the identifier is to be accessed directly. On the other hand,
    the expression inside a ``Get`` node need not be a simple identifier, e.g.
    ``arr[1].memb.left`` is be represented as:
    ``Get(Get(Apply("_at", Get(Ident("arr")), 1),"memb"),"left")``
    """

    expr: Base
    """
    :type: WDL.Expr.Base

    The expression whose value is accessed
    """
    member: Optional[str]
    """
    :type: Optional[str]

    If the expression is accessing a pair/struct member, then ``expr.type`` is
    ``WDL.Type.Pair`` or ``WDL.Type.StructInstance`` and this field gives the
    desired member name (``left`` or ``right`` for pairs).

    Otherwise the expression accesses ``expr`` directly, and ``member`` is
    ``None``.
    """

    def __init__(self, pos: SourcePosition, expr: Base, member: Optional[str]) -> None:
        super().__init__(pos)
        assert expr
        self.expr = expr
        self.member = member

    @property
    def children(self) -> Iterable[SourceNode]:
        if self._type:
            # suppress children until resolution/typechecking is complete
            yield self.expr

    def _infer_type(self, type_env: Env.Types) -> Type.Base:
        if isinstance(self.expr, _LeftName):
            # expr is a lone "name" -- try to resolve it as an identifier,
            # and if that works, transform it to Ident("name")
            try:
                Env.resolve(type_env, [], self.expr.name)
                self.expr = Ident(self.expr.pos, [self.expr.name])
            except KeyError:
                if not self.member:
                    raise Error.UnknownIdentifier(self) from None
        # attempt to typecheck expr, disambiguating whether it's an
        # intermediate value, a resolvable identifier, or neither
        try:
            self.expr.infer_type(type_env, self._stdlib, self._check_quant)
        except Error.UnknownIdentifier:
            # Fail...there's one case we may be able to rescue, where expr is a
            # _LeftName inside zero or more Gets representing an incomplete
            # namespaced identifier, and our member completes the path to an
            # available named value.
            if not (isinstance(self.expr, (_LeftName, Get)) and self.expr._ident and self.member):
                raise
            # attempt to resolve "expr.member" and if that works, transform
            # expr to Ident("expr.member")
            try:
                Env.resolve(type_env, self.expr._ident, self.member)
            except KeyError:
                raise Error.UnknownIdentifier(self) from None
            self.expr = Ident(self.pos, self._ident)
            self.expr.infer_type(type_env, self._stdlib, self._check_quant)
            self.member = None
        # now we've typechecked expr
        ety = self.expr.type
        assert ety
        if not self.member:
            # no member to access; just propagate expr type
            assert isinstance(self.expr, Ident)
            return ety
        # now we expect expr to be a pair or struct, whose member we're
        # accessing
        if not isinstance(ety, (Type.Pair, Type.StructInstance)):
            raise Error.NoSuchMember(self, self.member)
        if self._check_quant and ety.optional:
            raise Error.StaticTypeMismatch(self.expr, ety.copy(optional=False), ety)
        if self.member in ["left", "right"]:
            if isinstance(ety, Type.Pair):
                return ety.left_type if self.member == "left" else ety.right_type
            raise Error.NoSuchMember(self, self.member)
        if isinstance(ety, Type.StructInstance):
            try:
                assert ety.members is not None
                return ety.members[self.member]
            except KeyError:
                pass
        raise Error.NoSuchMember(self, self.member)

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Base:
        innard_value = self.expr.eval(env, stdlib)
        if not self.member:
            return innard_value
        if isinstance(innard_value, Value.Pair):
            assert self.member in ["left", "right"]
            return innard_value.value[0 if self.member == "left" else 1]
        raise NotImplementedError()

    @property
    def _ident(self) -> List[str]:
        # helper for the resolution logic above -- get the partial identifier
        # recursing into nested Gets, if there's a _LeftName at the bottom.
        if isinstance(self.expr, (_LeftName, Get)) and self.expr._ident:
            return self.expr._ident + ([self.member] if self.member else [])
        return []


_base_stdlib = None  # memoized instance of the defalut WDL.StdLib.Base()


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

    def __init__(self, pos: SourcePosition, function: str, arguments: List[Base]) -> None:
        super().__init__(pos)
        self.function_name = function
        self.arguments = arguments

    @property
    def children(self) -> Iterable[SourceNode]:
        for arg in self.arguments:
            yield arg

    def _infer_type(self, type_env: Env.Types) -> Type.Base:

        global _base_stdlib
        if not _base_stdlib:
            _base_stdlib = StdLib.Base()
        f = getattr(self._stdlib or _base_stdlib, self.function_name, None)
        if not f:
            raise Error.NoSuchFunction(self, self.function_name) from None
        assert isinstance(f, StdLib.Function)
        return f.infer_type(self)

    def _eval(self, env: Env.Values, stdlib: "Optional[StdLib.Base]" = None) -> Value.Base:
        ""

        global _base_stdlib
        if not _base_stdlib:
            _base_stdlib = StdLib.Base()
        stdlib = stdlib or _base_stdlib
        f = getattr(stdlib, self.function_name, None)
        assert isinstance(f, StdLib.Function)
        return f(self, env, stdlib)
