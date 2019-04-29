# pylint: disable=protected-access,exec-used
from typing import List, Tuple, Callable, Any
from abc import ABC, abstractmethod
import WDL.Type as T
import WDL.Value as V
import WDL.Expr as E
import WDL.Env as Env
import WDL.Error as Error


class Base:
    """
    Base class for standard library implementations. An instance has an
    attribute with the name of each available function and a ``Function``
    object providing the type-checking logic and implementation.

    Subclasses may replace these objects with custom context-dependent logic,
    or add new ones. For example, ``stdout()`` is only meaningful in task
    output sections.
    """

    def __init__(self):
        # language built-ins
        self._at = _At()
        self._land = _And()
        self._lor = _Or()
        self._negate = StaticFunction(
            "_negate", [T.Boolean()], T.Boolean(), lambda x: V.Boolean(not x.value)
        )
        self._add = _AddOperator()
        self._sub = _ArithmeticOperator("-", lambda l, r: l - r)
        self._mul = _ArithmeticOperator("*", lambda l, r: l * r)
        self._div = _ArithmeticOperator("/", lambda l, r: l // r)
        self._rem = StaticFunction(
            "_rem", [T.Int(), T.Int()], T.Int(), lambda l, r: V.Int(l.value % r.value)
        )
        self._eqeq = _ComparisonOperator("==", lambda l, r: l == r)
        self._neq = _ComparisonOperator("!=", lambda l, r: l != r)
        self._lt = _ComparisonOperator("<", lambda l, r: l < r)
        self._lte = _ComparisonOperator("<=", lambda l, r: l <= r)
        self._gt = _ComparisonOperator(">", lambda l, r: l > r)
        self._gte = _ComparisonOperator(">=", lambda l, r: l >= r)

        # static stdlib functions
        for (name, argument_types, return_type, F) in [
            ("floor", [T.Float()], T.Int(), _notimpl),
            ("ceil", [T.Float()], T.Int(), _notimpl),
            ("round", [T.Float()], T.Int(), _notimpl),
            ("length", [T.Array(T.Any())], T.Int(), lambda v: V.Int(len(v.value))),
            ("sub", [T.String(), T.String(), T.String()], T.String(), _notimpl),
            ("basename", [T.String(), T.String(optional=True)], T.String(), _notimpl),
            (
                "defined",
                [T.Any(optional=True)],
                T.Boolean(),
                lambda v: V.Boolean(not isinstance(v, V.Null)),
            ),
            # context-dependent:
            ("write_lines", [T.Array(T.String())], T.File(), _notimpl),
            ("write_tsv", [T.Array(T.Array(T.String()))], T.File(), _notimpl),
            ("write_map", [T.Map((T.Any(), T.Any()))], T.File(), _notimpl),
            ("write_json", [T.Any()], T.File(), _notimpl),
            ("stdout", [], T.File(), _notimpl),
            ("stderr", [], T.File(), _notimpl),
            ("glob", [T.String()], T.Array(T.File()), _notimpl),
            ("read_int", [T.File()], T.Int(), _notimpl),
            ("read_boolean", [T.File()], T.Boolean(), _notimpl),
            ("read_string", [T.File()], T.String(), _notimpl),
            ("read_float", [T.File()], T.Float(), _notimpl),
            ("read_array", [T.File()], T.Array(T.Any()), _notimpl),
            ("read_map", [T.File()], T.Map((T.Any(), T.Any())), _notimpl),
            ("read_lines", [T.File()], T.Array(T.Any()), _notimpl),
            ("read_tsv", [T.File()], T.Array(T.Array(T.String())), _notimpl),
            ("read_json", [T.File()], T.Any(), _notimpl),
        ]:
            setattr(self, name, StaticFunction(name, argument_types, return_type, F))

        # polymorphically typed stdlib functions which require specialized
        # infer_type logic
        self.range = _Range()
        self.prefix = _Prefix()
        self.size = _Size()
        self.select_first = _SelectFirst()
        self.select_all = _SelectAll()
        self.zip = _Zip()
        self.cross = _Zip()  # FIXME
        self.flatten = _Flatten()
        self.transpose = _Transpose()


class Function(ABC):
    # Abstract interface to a standard library function implementation

    @abstractmethod
    def infer_type(self, expr: E.Apply) -> T.Base:
        # Typecheck the Apply expression (including the argument expressions);
        # raise an exception or return the function's return type, which may
        # depend on the argument types.
        pass

    @abstractmethod
    def __call__(self, expr: E.Apply, env: Env.Values, stdlib: Base) -> V.Base:
        # Invoke the function, evaluating the arguments as needed
        pass


class EagerFunction(Function):
    # Function helper providing boilerplate for eager argument evaluation

    @abstractmethod
    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        pass

    def __call__(self, expr: E.Apply, env: Env.Values, stdlib: Base) -> V.Base:
        return self._call_eager(expr, [arg.eval(env, stdlib=stdlib) for arg in expr.arguments])


class StaticFunction(EagerFunction):
    # Function helper for static argument and return types

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
        if len(expr.arguments) > len(self.argument_types) or len(expr.arguments) < min_args:
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

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        argument_values = [arg.coerce(ty) for arg, ty in zip(arguments, self.argument_types)]
        ans: V.Base = self.F(*argument_values)
        return ans.coerce(self.return_type)


def _notimpl(one: Any = None, two: Any = None) -> None:
    exec("raise NotImplementedError()")


class _At(EagerFunction):
    # Special function for array access arr[index], returning the element type
    #                   or map access map[key], returning the value type

    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        lhs = expr.arguments[0]
        rhs = expr.arguments[1]
        if isinstance(lhs.type, T.Array):
            if isinstance(lhs, E.Array) and not lhs.items:
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

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        assert len(expr.arguments) == 2 and len(arguments) == 2
        lhs = arguments[0]
        rhs = arguments[1]
        if isinstance(lhs, V.Array):
            assert isinstance(rhs, V.Int)
            if rhs.value < 0 or rhs.value >= len(lhs.value):
                raise Error.OutOfBounds(expr.arguments[1])
            return lhs.value[rhs.value]
        if isinstance(lhs, V.Map):
            mty = expr.arguments[0].type
            assert isinstance(mty, T.Map)
            key = rhs.coerce(mty.item_type[0])
            ans = None
            for k, v in lhs.value:
                if rhs == k:
                    ans = v
            if ans is None:
                raise Error.OutOfBounds(expr.arguments[1])  # TODO: KeyNotFound
            return ans
        assert False


class _And(Function):
    # logical && with short-circuit evaluation
    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        for arg in expr.arguments:
            if not isinstance(arg.type, T.Boolean):
                raise Error.IncompatibleOperand(arg, "non-Boolean operand to &&")
            if expr._check_quant and arg.type.optional:
                raise Error.IncompatibleOperand(arg, "optional Boolean? operand to &&")
        return T.Boolean()

    def __call__(self, expr: E.Apply, env: Env.Values, stdlib: Base) -> V.Base:
        lhs = expr.arguments[0].eval(env, stdlib=stdlib).expect(T.Boolean()).value
        if not lhs:
            return V.Boolean(False)
        return expr.arguments[1].eval(env, stdlib=stdlib).expect(T.Boolean())


class _Or(Function):
    # logical || with short-circuit evaluation
    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        for arg in expr.arguments:
            if not isinstance(arg.type, T.Boolean):
                raise Error.IncompatibleOperand(arg, "non-Boolean operand to ||")
            if expr._check_quant and arg.type.optional:
                raise Error.IncompatibleOperand(arg, "optional Boolean? operand to ||")
        return T.Boolean()

    def __call__(self, expr: E.Apply, env: Env.Values, stdlib: Base) -> V.Base:
        lhs = expr.arguments[0].eval(env, stdlib=stdlib).expect(T.Boolean()).value
        if lhs:
            return V.Boolean(True)
        return expr.arguments[1].eval(env, stdlib=stdlib).expect(T.Boolean())


class _ArithmeticOperator(EagerFunction):
    # arithmetic infix operators
    # operands may be Int or Float; return Float iff either operand is Float

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

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        ans_type = self.infer_type(expr)
        try:
            ans = self.op(arguments[0].coerce(ans_type).value, arguments[1].coerce(ans_type).value)
        except ZeroDivisionError:
            # TODO: different runtime error?
            raise Error.IncompatibleOperand(expr.arguments[1], "Division by zero") from None
        if ans_type == T.Int():
            assert isinstance(ans, int)
            return V.Int(ans)
        assert isinstance(ans, float)
        return V.Float(ans)


class _AddOperator(_ArithmeticOperator):
    # + operator can also serve as concatenation for String.
    def __init__(self) -> None:
        super().__init__("+", lambda l, r: l + r)

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

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        ans_type = self.infer_type(expr)
        if not isinstance(ans_type, T.String):
            return super()._call_eager(expr, arguments)
        # TODO: in a command interpolation, return missing if either operand is missing
        ans = self.op(
            str(arguments[0].coerce(T.String()).value), str(arguments[1].coerce(T.String()).value)
        )
        assert isinstance(ans, str)
        return V.String(ans)


class _ComparisonOperator(EagerFunction):
    # Comparison operators can compare any two operands of the same type.
    # Furthermore, given one Int and one Float, coerces the Int to Float for
    # comparison.

    name: str
    op: Callable

    def __init__(self, name: str, op: Callable) -> None:
        self.name = name
        self.op = op

    def infer_type(self, expr: E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        if (
            (
                expr._check_quant
                and expr.arguments[0].type.optional != expr.arguments[1].type.optional
            )
            or (
                self.name not in ["==", "!="]
                and (expr.arguments[0].type.optional or expr.arguments[1].type.optional)
            )
            or (
                not (
                    expr.arguments[0].type.copy(optional=False)
                    == expr.arguments[1].type.copy(optional=False)
                    or (
                        isinstance(expr.arguments[0].type, T.Int)
                        and isinstance(expr.arguments[1].type, T.Float)
                    )
                    or (
                        isinstance(expr.arguments[0].type, T.Float)
                        and isinstance(expr.arguments[1].type, T.Int)
                    )
                )
            )
        ):
            raise Error.IncompatibleOperand(
                expr,
                "Cannot compare {} and {}".format(
                    str(expr.arguments[0].type), str(expr.arguments[1].type)
                ),
            )
        return T.Boolean()

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        assert len(arguments) == 2
        return V.Boolean(self.op(arguments[0].value, arguments[1].value))


class _Size(EagerFunction):
    # size(): first argument can be File? or Array[File?]

    def infer_type(self, expr: E.Apply) -> T.Base:
        if not expr.arguments:
            raise Error.WrongArity(expr, 1)
        if not expr.arguments[0].type.coerces(T.File(optional=True)):
            if isinstance(expr.arguments[0].type, T.Array):
                if expr.arguments[0].type.optional or not expr.arguments[0].type.item_type.coerces(
                    T.File(optional=True)
                ):
                    raise Error.StaticTypeMismatch(
                        expr.arguments[0], T.Array(T.File(optional=True)), expr.arguments[0].type
                    )
            else:
                raise Error.StaticTypeMismatch(
                    expr.arguments[0], T.File(optional=True), expr.arguments[0].type
                )
        if len(expr.arguments) == 2:
            if expr.arguments[1].type != T.String():
                raise Error.StaticTypeMismatch(
                    expr.arguments[1], T.String(), expr.arguments[1].type
                )
        elif len(expr.arguments) > 2:
            raise Error.WrongArity(expr, 2)
        return T.Float()

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        raise NotImplementedError()


class _SelectFirst(EagerFunction):
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        if not isinstance(expr.arguments[0].type, T.Array) or (
            expr.arguments[0]._check_quant and expr.arguments[0].type.optional
        ):
            raise Error.StaticTypeMismatch(
                expr.arguments[0], T.Array(T.Any()), expr.arguments[0].type
            )
        if isinstance(expr.arguments[0].type.item_type, T.Any):
            # TODO: error for 'indeterminate type'
            raise Error.EmptyArray(expr.arguments[0])
        ty = expr.arguments[0].type.item_type
        assert isinstance(ty, T.Base)
        return ty.copy(optional=False)

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        raise NotImplementedError()


class _SelectAll(EagerFunction):
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        if not isinstance(expr.arguments[0].type, T.Array) or (
            expr.arguments[0]._check_quant and expr.arguments[0].type.optional
        ):
            raise Error.StaticTypeMismatch(
                expr.arguments[0], T.Array(T.Any()), expr.arguments[0].type
            )
        if isinstance(expr.arguments[0].type.item_type, T.Any):
            # TODO: error for 'indeterminate type'
            raise Error.EmptyArray(expr.arguments[0])
        ty = expr.arguments[0].type.item_type
        assert isinstance(ty, T.Base)
        return T.Array(ty.copy(optional=False))

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        raise NotImplementedError()


class _Zip(EagerFunction):
    # 'a array -> 'b array -> ('a,'b) array
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 2:
            raise Error.WrongArity(expr, 2)
        arg0ty: T.Base = expr.arguments[0].type
        if not isinstance(arg0ty, T.Array) or (expr._check_quant and arg0ty.optional):
            raise Error.StaticTypeMismatch(expr.arguments[0], T.Array(T.Any()), arg0ty)
        if isinstance(arg0ty.item_type, T.Any):
            # TODO: error for 'indeterminate type'
            raise Error.EmptyArray(expr.arguments[0])
        arg1ty: T.Base = expr.arguments[1].type
        if not isinstance(arg1ty, T.Array) or (expr._check_quant and arg1ty.optional):
            raise Error.StaticTypeMismatch(expr.arguments[1], T.Array(T.Any()), arg1ty)
        if isinstance(arg1ty.item_type, T.Any):
            # TODO: error for 'indeterminate type'
            raise Error.EmptyArray(expr.arguments[1])
        return T.Array(
            T.Pair(arg0ty.item_type, arg1ty.item_type),
            nonempty=(arg0ty.nonempty or arg1ty.nonempty),
        )

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        raise NotImplementedError()


class _Flatten(EagerFunction):
    # t array array -> t array
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        expr.arguments[0].typecheck(T.Array(T.Any()))
        # TODO: won't handle implicit coercion from T to Array[T]
        assert isinstance(expr.arguments[0].type, T.Array)
        if expr.arguments[0].type.item_type is None:
            return T.Array(T.Any())
        if not isinstance(expr.arguments[0].type.item_type, T.Array):
            raise Error.StaticTypeMismatch(
                expr.arguments[0], T.Array(T.Array(T.Any())), expr.arguments[0].type
            )
        return T.Array(expr.arguments[0].type.item_type.item_type)

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        raise NotImplementedError()


class _Transpose(EagerFunction):
    # t array array -> t array array
    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        expr.arguments[0].typecheck(T.Array(T.Any()))
        # TODO: won't handle implicit coercion from T to Array[T]
        assert isinstance(expr.arguments[0].type, T.Array)
        if expr.arguments[0].type.item_type is None:
            return T.Array(T.Any())
        if not isinstance(expr.arguments[0].type.item_type, T.Array):
            raise Error.StaticTypeMismatch(
                expr.arguments[0], T.Array(T.Array(T.Any())), expr.arguments[0].type
            )
        return expr.arguments[0].type

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        raise NotImplementedError()


class _Range(EagerFunction):
    # int -> int array
    # with special case: if the argument is a positive integer literal or
    # length(a_nonempty_array), then we can say the returned array is nonempty.

    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        expr.arguments[0].typecheck(T.Int())
        nonempty = False
        arg0 = expr.arguments[0]
        if isinstance(arg0, E.Int) and arg0.value > 0:
            nonempty = True
        if isinstance(arg0, E.Apply) and arg0.function_name == "length":
            arg00ty = arg0.arguments[0].type
            if isinstance(arg00ty, T.Array) and arg00ty.nonempty:
                nonempty = True
        return T.Array(T.Int(), nonempty=nonempty)

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        raise NotImplementedError()


class _Prefix(EagerFunction):
    # string -> t array -> string array
    # if input array is nonempty then so is output

    def infer_type(self, expr: E.Apply) -> T.Base:
        if len(expr.arguments) != 2:
            raise Error.WrongArity(expr, 2)
        expr.arguments[0].typecheck(T.String())
        expr.arguments[1].typecheck(T.Array(T.String()))
        return T.Array(
            T.String(),
            nonempty=(
                isinstance(expr.arguments[1].type, T.Array) and expr.arguments[1].type.nonempty
            ),
        )

    def _call_eager(self, expr: E.Apply, arguments: List[V.Base]) -> V.Base:
        raise NotImplementedError()
