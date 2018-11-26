# pyre-strict
# pylint: disable=protected-access,exec-used
from typing import List, Tuple, Callable, Any
import WDL.Type as T
import WDL.Value as V
import WDL.Expr as E
import WDL.Env as Env
import WDL.Error as Error

# Special function for array access arr[index], returning the element type
#                      or map access map[key], returning the value type


class _Get(E._Function):
    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        lhs = expr.arguments[0]
        rhs = expr.arguments[1]
        if isinstance(lhs.type, T.Array):
            if lhs.type.item_type is None:
                # the user wrote: [][idx]
                raise Error.OutOfBounds(expr)
            try:
                rhs.typecheck(T.Int())
            except Error.StaticTypeMismatch:
                raise Error.StaticTypeMismatch(rhs, T.Int(), rhs.type, "Array index") from None
            return lhs.type.item_type
        if isinstance(lhs.type, T.Map):
            if lhs.type.item_type is None:
                raise Error.OutOfBounds(expr)
            try:
                rhs.typecheck(lhs.type.item_type[0])
            except Error.StaticTypeMismatch:
                raise Error.StaticTypeMismatch(
                    rhs, lhs.type.item_type[0], rhs.type, "Map key"
                ) from None
            return lhs.type.item_type[1]
        raise Error.NotAnArray(lhs)

    def __call__(self, expr: E.Apply, env: E.Env) -> V.Base:
        assert len(expr.arguments) == 2
        lhs = expr.arguments[0]
        rhs = expr.arguments[1]
        if isinstance(lhs.type, T.Array):
            arr = lhs.eval(env)
            assert isinstance(arr, V.Array)
            assert isinstance(arr.type, T.Array)
            assert isinstance(arr.value, list)
            idx = rhs.eval(env).expect(T.Int()).value
            if idx < 0 or idx >= len(arr.value):
                raise Error.OutOfBounds(rhs)
            return arr.value[idx]  # pyre-fixme
        if isinstance(lhs.type, T.Map):
            mp = lhs.eval(env)
            assert isinstance(mp, V.Map)
            assert isinstance(mp.type, T.Map)
            assert mp.type.item_type is not None
            assert isinstance(mp.value, list)
            ans = None
            key = rhs.eval(env).expect(mp.type.item_type[0])
            for k, v in mp.value:
                if key == k:
                    ans = v.expect(mp.type.item_type[1])
            if ans is None:
                raise Error.OutOfBounds(rhs)  # TODO: KeyNotFound
            return ans  # pyre-fixme
        assert False  # pyre-fixme


E._stdlib["_get"] = _Get()

# Pair get (EXPR.left/EXPR.right)
# The special case where EXPR is an identifier goes a different path, through
# Expr.Ident.


class _PairGet(E._Function):
    left: bool

    def __init__(self, left: bool) -> None:
        self.left = left

    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 1
        if not isinstance(expr.arguments[0].type, T.Pair):
            raise Error.NotAPair(expr.arguments[0])
        return expr.arguments[0].type.left_type if self.left else expr.arguments[0].type.right_type

    def __call__(self, expr: E.Apply, env: E.Env) -> V.Base:
        assert len(expr.arguments) == 1
        pair = expr.arguments[0].eval(env)
        assert isinstance(pair.type, T.Pair)
        assert isinstance(pair.value, tuple)
        return pair.value[0] if self.left else pair.value[1]


E._stdlib["_get_left"] = _PairGet(True)
E._stdlib["_get_right"] = _PairGet(False)

# logical && with short-circuit evaluation


class _And(E._Function):
    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        for arg in expr.arguments:
            if not isinstance(arg.type, T.Boolean):
                raise Error.IncompatibleOperand(arg, "non-Boolean operand to &&")
            if arg.type.optional:
                raise Error.IncompatibleOperand(arg, "optional Boolean? operand to &&")
        return T.Boolean()

    def __call__(self, expr: E.Apply, env: E.Env) -> V.Base:
        lhs = expr.arguments[0].eval(env).value
        if not lhs:
            return V.Boolean(False)
        return expr.arguments[1].eval(env)


E._stdlib["_land"] = _And()


class _Or(E._Function):
    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        for arg in expr.arguments:
            if not isinstance(arg.type, T.Boolean):
                raise Error.IncompatibleOperand(arg, "non-Boolean operand to ||")
            if arg.type.optional:
                raise Error.IncompatibleOperand(arg, "optional Boolean? operand to ||")
        return T.Boolean()

    def __call__(self, expr: E.Apply, env: E.Env) -> V.Base:
        lhs = expr.arguments[0].eval(env).value
        if lhs:
            return V.Boolean(True)
        return expr.arguments[1].eval(env)


E._stdlib["_lor"] = _Or()

# _Function helper for simple functions with fixed argument and return types


class _StaticFunction(E._Function):
    name: str
    argument_types: List[T.Base]
    return_type: T.Base
    F: Callable

    def __init__(
        self, name: str, argument_types: List[T.Base], return_type: T.Base, F: Callable
    ) -> None:
        self.name = name
        self.argument_types = argument_types
        self.return_type = return_type
        self.F = F

    def infer_type(self, expr: E.Apply) -> T.Base:
        min_args = len(self.argument_types)
        for ty in reversed(self.argument_types):
            if ty.optional:
                min_args = min_args - 1
            else:
                break
        if len(expr.arguments) < min_args:
            raise Error.WrongArity(expr, len(self.argument_types))
        for i in range(len(expr.arguments)):
            try:
                expr.arguments[i].typecheck(self.argument_types[i])
            except Error.StaticTypeMismatch:
                raise Error.StaticTypeMismatch(
                    expr.arguments[i],
                    self.argument_types[i],
                    expr.arguments[i].type,
                    "for {} argument #{}".format(self.name, i + 1),
                ) from None
        return self.return_type

    def __call__(self, expr: E.Apply, env: E.Env) -> V.Base:
        assert len(expr.arguments) == len(self.argument_types)
        argument_values = [
            arg.eval(env).coerce(ty) for arg, ty in zip(expr.arguments, self.argument_types)
        ]
        ans: V.Base = self.F(*argument_values)
        return ans.coerce(self.return_type)


def _notimpl(one: Any = None, two: Any = None) -> None:
    exec("raise NotImplementedError()")


_static_functions: List[Tuple[str, List[T.Base], T.Base, Any]] = [
    ("_negate", [T.Boolean()], T.Boolean(), lambda x: V.Boolean(not x.value)),  # pyre-fixme
    ("_rem", [T.Int(), T.Int()], T.Int(), lambda l, r: V.Int(l.value % r.value)),  # pyre-fixme
    ("stdout", [], T.File(), _notimpl),
    ("basename", [T.String(), T.String(optional=True)], T.String(), _notimpl),
    # TODO: size() argument is optional to admit a pattern seen in the test corpi:
    #         if (defined(f)) then size(f) else 100
    #       unclear how this should apply generaly to functions other than size().
    #       alternatively, during typechecking, we could infer that the f can't
    #       be null in the consequent branch specifically.
    # FILED https://github.com/openwdl/wdl/issues/271
    ("size", [T.File(optional=True), T.String(optional=True)], T.Float(), _notimpl),
    ("ceil", [T.Float()], T.Int(), _notimpl),
    ("round", [T.Float()], T.Int(), _notimpl),
    ("glob", [T.String()], T.Array(T.File()), _notimpl),
    ("read_int", [T.String()], T.Int(), _notimpl),
    ("read_boolean", [T.String()], T.Boolean(), _notimpl),
    ("read_string", [T.String()], T.String(), _notimpl),
    ("read_float", [T.String()], T.Float(), _notimpl),
    ("read_array", [T.String()], T.Array(None), _notimpl),
    ("read_map", [T.String()], T.Map(None), _notimpl),
    ("read_lines", [T.String()], T.Array(None), _notimpl),
    ("read_tsv", [T.String()], T.Array(T.Array(T.String())), _notimpl),
    ("write_lines", [T.Array(T.String())], T.File(), _notimpl),
    ("write_tsv", [T.Array(T.Array(T.String()))], T.File(), _notimpl),
    ("write_map", [T.Map(None)], T.File(), _notimpl),
    ("range", [T.Int()], T.Array(T.Int()), _notimpl),
    ("sub", [T.String(), T.String(), T.String()], T.String(), _notimpl),
]
for name, argument_types, return_type, F in _static_functions:
    E._stdlib[name] = _StaticFunction(name, argument_types, return_type, F)

# Polymorphic functions

# arithmetic infix operators
# operands may be Int or Float; return Float iff either operand is Float


class _ArithmeticOperator(E._Function):
    name: str
    op: Callable

    def __init__(self, name: str, op: Callable) -> None:
        self.name = name
        self.op = op

    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        rt = T.Int()
        if isinstance(expr.arguments[0].type, T.Float) or isinstance(
            expr.arguments[1].type, T.Float
        ):
            rt = T.Float()
        try:
            expr.arguments[0].typecheck(rt)
            expr.arguments[1].typecheck(rt)
        except Error.StaticTypeMismatch:
            raise Error.IncompatibleOperand(
                expr, "Non-numeric operand to " + self.name + " operator"
            ) from None
        return rt

    def __call__(self, expr: E.Apply, env: E.Env) -> V.Base:
        ans_type = self.infer_type(expr)
        try:
            ans = self.op(
                expr.arguments[0].eval(env).coerce(ans_type).value,
                expr.arguments[1].eval(env).coerce(ans_type).value,
            )
        except ZeroDivisionError:
            # TODO: different runtime error?
            raise Error.IncompatibleOperand(expr.arguments[1], "Division by zero") from None
        if ans_type == T.Int():
            assert isinstance(ans, int)
            return V.Int(ans)
        assert isinstance(ans, float)
        return V.Float(ans)


E._stdlib["_sub"] = _ArithmeticOperator("-", lambda l, r: l - r)  # pyre-ignore
E._stdlib["_mul"] = _ArithmeticOperator("*", lambda l, r: l * r)  # pyre-ignore
E._stdlib["_div"] = _ArithmeticOperator("/", lambda l, r: l // r)  # pyre-ignore


class _AddOperator(_ArithmeticOperator):
    # + operator can also serve as concatenation for String.
    def __init__(self) -> None:
        super().__init__("+", lambda l, r: l + r)  # pyre-ignore

    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        t2 = None
        if isinstance(expr.arguments[0].type, T.String):
            t2 = expr.arguments[1].type
        elif isinstance(expr.arguments[1].type, T.String):
            t2 = expr.arguments[0].type
        if t2 is None:
            # neither operand is a string; defer to _ArithmeticOperator
            return super().infer_type(expr)
        if not t2.coerces(T.String(optional=True)):
            raise Error.IncompatibleOperand(
                expr,
                "Cannot add/concatenate {} and {}".format(
                    str(expr.arguments[0].type), str(expr.arguments[1].type)
                ),
            )
        return T.String()

    def __call__(self, expr: E.Apply, env: E.Env) -> V.Base:
        ans_type = self.infer_type(expr)
        if not isinstance(ans_type, T.String):
            return super().__call__(expr, env)
        ans = self.op(
            str(expr.arguments[0].eval(env).value), str(expr.arguments[1].eval(env).value)
        )
        assert isinstance(ans, str)
        return V.String(ans)


E._stdlib["_add"] = _AddOperator()

# Comparison operators can compare any two operands of the same type.
# Furthermore,
# - given one Int and one Float, coerces the Int to Float for comparison.


class _ComparisonOperator(E._Function):
    name: str
    op: Callable

    def __init__(self, name: str, op: Callable) -> None:
        self.name = name
        self.op = op

    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        if not (
            expr.arguments[0].type == expr.arguments[1].type
            or (expr.arguments[0].type == T.Int() and expr.arguments[1].type == T.Float())
            or (expr.arguments[0].type == T.Float() and expr.arguments[1].type == T.Int())
        ):
            raise Error.IncompatibleOperand(
                expr,
                "Cannot compare {} and {}".format(
                    str(expr.arguments[0].type), str(expr.arguments[1].type)
                ),
            )
        return T.Boolean()

    def __call__(self, expr: E.Apply, env: E.Env) -> V.Base:
        assert len(expr.arguments) == 2
        return V.Boolean(
            # pyre-fixme
            self.op(expr.arguments[0].eval(env).value, expr.arguments[1].eval(env).value)
        )


E._stdlib["_eqeq"] = _ComparisonOperator("==", lambda l, r: l == r)
E._stdlib["_neq"] = _ComparisonOperator("!=", lambda l, r: l != r)
E._stdlib["_lt"] = _ComparisonOperator("<", lambda l, r: l < r)  # pyre-fixme
E._stdlib["_lte"] = _ComparisonOperator("<=", lambda l, r: l <= r)  # pyre-fixme
E._stdlib["_gt"] = _ComparisonOperator(">", lambda l, r: l > r)  # pyre-fixme
E._stdlib["_gte"] = _ComparisonOperator(">=", lambda l, r: l >= r)  # pyre-fixme

# defined(): accepts any type...


class _Defined(E._Function):
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        return T.Boolean()

    def __call__(self, expr: E.Apply, env: Env.Values) -> V.Base:
        if isinstance(expr.arguments[0].eval(env), V.Null):
            return V.Boolean(False)
        return V.Boolean(True)


E._stdlib["defined"] = _Defined()


class _Length(E._Function):
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        if not isinstance(expr.arguments[0].type, T.Array):
            raise Error.StaticTypeMismatch(expr, T.Array(None), expr.arguments[0].type)
        return T.Int()

    def __call__(self, expr: E.Apply, env: Env.Values) -> V.Base:
        v = expr.arguments[0].eval(env)
        if isinstance(v, V.Null):
            return V.Int(0)
        assert isinstance(v.value, list)
        return V.Int(len(v.value))


E._stdlib["length"] = _Length()


class _SelectFirst(E._Function):
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        if not isinstance(expr.arguments[0].type, T.Array):
            raise Error.StaticTypeMismatch(expr.arguments[0], T.Array(None), expr.arguments[0].type)
        if expr.arguments[0].type.item_type is None:
            # TODO: error for 'indeterminate type'
            raise Error.EmptyArray(expr.arguments[0])
        ty = expr.arguments[0].type.item_type
        assert isinstance(ty, T.Base)
        return ty.copy(optional=False)

    def __call__(self, expr: E.Apply, env: Env.Values) -> V.Base:
        raise NotImplementedError()


E._stdlib["select_first"] = _SelectFirst()


class _SelectAll(E._Function):
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        if not isinstance(expr.arguments[0].type, T.Array):
            raise Error.StaticTypeMismatch(expr.arguments[0], T.Array(None), expr.arguments[0].type)
        if expr.arguments[0].type.item_type is None:
            # TODO: error for 'indeterminate type'
            raise Error.EmptyArray(expr.arguments[0])
        ty = expr.arguments[0].type.item_type
        assert isinstance(ty, T.Base)
        return T.Array(ty.copy(optional=False))

    def __call__(self, expr: E.Apply, env: Env.Values) -> V.Base:
        raise NotImplementedError()


E._stdlib["select_all"] = _SelectAll()


class _Zip(E._Function):
    # 'a array -> 'b array -> ('a,'b) array
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 2:
            raise Error.WrongArity(expr, 2)
        if not isinstance(expr.arguments[0].type, T.Array):
            raise Error.StaticTypeMismatch(expr.arguments[0], T.Array(None), expr.arguments[0].type)
        if expr.arguments[0].type.item_type is None:
            # TODO: error for 'indeterminate type'
            raise Error.EmptyArray(expr.arguments[0])
        if not isinstance(expr.arguments[1].type, T.Array):
            raise Error.StaticTypeMismatch(expr.arguments[1], T.Array(None), expr.arguments[0].type)
        if expr.arguments[1].type.item_type is None:
            # TODO: error for 'indeterminate type'
            raise Error.EmptyArray(expr.arguments[1])
        return T.Array(T.Pair(expr.arguments[0].type.item_type, expr.arguments[1].type.item_type))

    def __call__(self, expr: E.Apply, env: Env.Values) -> V.Base:
        raise NotImplementedError()


E._stdlib["zip"] = _Zip()
E._stdlib["cross"] = _Zip()  # TODO


class _Flatten(E._Function):
    # t array array -> t array
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        expr.arguments[0].typecheck(T.Array(None))
        # TODO: won't handle implicit coercion from T to Array[T]
        assert isinstance(expr.arguments[0].type, T.Array)
        if expr.arguments[0].type.item_type is None:
            return T.Array(None)
        if not isinstance(expr.arguments[0].type.item_type, T.Array):
            raise Error.StaticTypeMismatch(
                expr.arguments[0], T.Array(T.Array(None)), expr.arguments[0].type
            )
        return T.Array(expr.arguments[0].type.item_type.item_type)

    def __call__(self, expr: E.Apply, env: Env.Values) -> V.Base:
        raise NotImplementedError()


E._stdlib["flatten"] = _Flatten()


class _Transpose(E._Function):
    # t array array -> t array array
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        expr.arguments[0].typecheck(T.Array(None))
        # TODO: won't handle implicit coercion from T to Array[T]
        assert isinstance(expr.arguments[0].type, T.Array)
        if expr.arguments[0].type.item_type is None:
            return T.Array(None)
        if not isinstance(expr.arguments[0].type.item_type, T.Array):
            raise Error.StaticTypeMismatch(
                expr.arguments[0], T.Array(T.Array(None)), expr.arguments[0].type
            )
        return expr.arguments[0].type

    def __call__(self, expr: E.Apply, env: Env.Values) -> V.Base:
        raise NotImplementedError()


E._stdlib["transpose"] = _Transpose()
