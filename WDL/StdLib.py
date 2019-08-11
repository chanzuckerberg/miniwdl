# pylint: disable=protected-access,exec-used
import math
import os
import re
from typing import List, Tuple, Callable
from abc import ABC, abstractmethod
from contextlib import contextmanager
from . import Type, Value, Expr, Env, Error


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
            "_negate", [Type.Boolean()], Type.Boolean(), lambda x: Value.Boolean(not x.value)
        )
        self._add = _AddOperator()
        self._sub = _ArithmeticOperator("-", lambda l, r: l - r)
        self._mul = _ArithmeticOperator("*", lambda l, r: l * r)
        self._div = _ArithmeticOperator("/", lambda l, r: l // r)
        self._rem = StaticFunction(
            "_rem", [Type.Int(), Type.Int()], Type.Int(), lambda l, r: Value.Int(l.value % r.value)
        )
        self._eqeq = _ComparisonOperator("==", lambda l, r: l == r)
        self._neq = _ComparisonOperator("!=", lambda l, r: l != r)
        self._lt = _ComparisonOperator("<", lambda l, r: l < r)
        self._lte = _ComparisonOperator("<=", lambda l, r: l <= r)
        self._gt = _ComparisonOperator(">", lambda l, r: l > r)
        self._gte = _ComparisonOperator(">=", lambda l, r: l >= r)

        # static stdlib functions
        for (name, argument_types, return_type, F) in [
            ("floor", [Type.Float()], Type.Int(), lambda v: Value.Int(math.floor(v.value))),
            ("ceil", [Type.Float()], Type.Int(), lambda v: Value.Int(math.ceil(v.value))),
            ("round", [Type.Float()], Type.Int(), lambda v: Value.Int(round(v.value))),
            ("length", [Type.Array(Type.Any())], Type.Int(), lambda v: Value.Int(len(v.value))),
            ("sub", [Type.String(), Type.String(), Type.String()], Type.String(), _sub),
            ("basename", [Type.String(), Type.String(optional=True)], Type.String(), _basename),
            (
                "defined",
                [Type.Any(optional=True)],
                Type.Boolean(),
                lambda v: Value.Boolean(not isinstance(v, Value.Null)),
            ),
            # context-dependent:
            ("write_lines", [Type.Array(Type.String())], Type.File(), _notimpl),
            ("write_tsv", [Type.Array(Type.Array(Type.String()))], Type.File(), _notimpl),
            ("write_map", [Type.Map((Type.Any(), Type.Any()))], Type.File(), _notimpl),
            ("write_json", [Type.Any()], Type.File(), _notimpl),
            ("stdout", [], Type.File(), _notimpl),
            ("stderr", [], Type.File(), _notimpl),
            ("glob", [Type.String()], Type.Array(Type.File()), _notimpl),
            ("read_int", [Type.File()], Type.Int(), _notimpl),
            ("read_boolean", [Type.File()], Type.Boolean(), _notimpl),
            ("read_string", [Type.File()], Type.String(), _notimpl),
            ("read_float", [Type.File()], Type.Float(), _notimpl),
            ("read_map", [Type.File()], Type.Map((Type.String(), Type.String())), _notimpl),
            ("read_lines", [Type.File()], Type.Array(Type.Any()), _notimpl),
            ("read_tsv", [Type.File()], Type.Array(Type.Array(Type.String())), _notimpl),
            ("read_json", [Type.File()], Type.Any(), _notimpl),
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
        self.cross = _Cross()
        self.flatten = _Flatten()
        self.transpose = _Transpose()

    def _override(self, name: str, fn: "Function") -> None:
        # replace a Function
        assert isinstance(getattr(self, name), Function)
        setattr(self, name, fn)

    def _override_static(self, name: str, f: Callable) -> None:
        # replace the implementation lambda of a StaticFunction (keeping its
        # types etc. the same)
        sf = getattr(self, name)
        assert isinstance(sf, StaticFunction)
        setattr(sf, "F", f)

    @contextmanager
    def _context_override(self, name: str, fn: "Function"):
        # replace a Function only for the life of the contextmanager.
        orig = getattr(self, name)
        self._override(name, fn)
        try:
            yield self
        finally:
            self._override(name, orig)


class Function(ABC):
    # Abstract interface to a standard library function implementation

    @abstractmethod
    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        # Typecheck the Apply expression (including the argument expressions);
        # raise an exception or return the function's return type, which may
        # depend on the argument types.
        pass

    @abstractmethod
    def __call__(
        self, expr: "Expr.Apply", env: Env.Bindings[Value.Base], stdlib: Base
    ) -> Value.Base:
        # Invoke the function, evaluating the arguments as needed
        pass


class EagerFunction(Function):
    # Function helper providing boilerplate for eager argument evaluation.
    # Implementation is responsible for any appropriate type coercion of
    # argument and return values.

    @abstractmethod
    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        pass

    def __call__(
        self, expr: "Expr.Apply", env: Env.Bindings[Value.Base], stdlib: Base
    ) -> Value.Base:
        return self._call_eager(expr, [arg.eval(env, stdlib=stdlib) for arg in expr.arguments])


class StaticFunction(EagerFunction):
    # Function helper for static argument and return types.
    # In this case the boilerplate can handle the coercions.

    name: str
    argument_types: List[Type.Base]
    return_type: Type.Base
    F: Callable

    def __init__(
        self, name: str, argument_types: List[Type.Base], return_type: Type.Base, F: Callable
    ) -> None:
        self.name = name
        self.argument_types = argument_types
        self.return_type = return_type
        self.F = F

    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
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

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        argument_values = [arg.coerce(ty) for arg, ty in zip(arguments, self.argument_types)]
        try:
            ans: Value.Base = self.F(*argument_values)
        except Exception as exn:
            msg = "function evaluation failed"
            if str(exn):
                msg += ", " + str(exn)
            raise Error.EvalError(expr, msg) from exn
        return ans.coerce(self.return_type)


def _notimpl(*args, **kwargs) -> None:
    exec("raise NotImplementedError('function not available in this context')")


def _basename(*args) -> Value.String:
    assert len(args) in (1, 2)
    assert isinstance(args[0], Value.String)
    path = args[0].value
    if len(args) > 1:
        assert isinstance(args[1], Value.String)
        suffix = args[1].value
        if path.endswith(suffix):
            path = path[: -len(suffix)]
    return Value.String(os.path.basename(path))


def _sub(input: Value.String, pattern: Value.String, replace: Value.String) -> Value.String:
    return Value.String(re.compile(pattern.value).sub(replace.value, input.value))


class _At(EagerFunction):
    # Special function for array access arr[index], returning the element type
    #                   or map access map[key], returning the value type

    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        assert len(expr.arguments) == 2
        lhs = expr.arguments[0]
        rhs = expr.arguments[1]
        if isinstance(lhs.type, Type.Array):
            if isinstance(lhs, Expr.Array) and not lhs.items:
                # the user wrote: [][idx]
                raise Error.OutOfBounds(expr)
            try:
                rhs.typecheck(Type.Int())
            except Error.StaticTypeMismatch:
                raise Error.StaticTypeMismatch(rhs, Type.Int(), rhs.type, "Array index") from None
            return lhs.type.item_type
        if isinstance(lhs.type, Type.Map):
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

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        assert len(expr.arguments) == 2 and len(arguments) == 2
        lhs = arguments[0]
        rhs = arguments[1]
        if isinstance(lhs, Value.Map):
            mty = expr.arguments[0].type
            assert isinstance(mty, Type.Map)
            key = rhs.coerce(mty.item_type[0])
            ans = None
            for k, v in lhs.value:
                if rhs == k:
                    ans = v
            if ans is None:
                raise Error.OutOfBounds(expr.arguments[1])  # TODO: KeyNotFound
            return ans
        else:
            lhs = lhs.coerce(Type.Array(Type.Any()))
            rhs = rhs.coerce(Type.Int())
            if rhs.value < 0 or rhs.value >= len(lhs.value):
                raise Error.OutOfBounds(expr.arguments[1])
            return lhs.value[rhs.value]


class _And(Function):
    # logical && with short-circuit evaluation
    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        assert len(expr.arguments) == 2
        for arg in expr.arguments:
            if not isinstance(arg.type, Type.Boolean):
                raise Error.IncompatibleOperand(arg, "non-Boolean operand to &&")
            if expr._check_quant and arg.type.optional:
                raise Error.IncompatibleOperand(arg, "optional Boolean? operand to &&")
        return Type.Boolean()

    def __call__(
        self, expr: "Expr.Apply", env: Env.Bindings[Value.Base], stdlib: Base
    ) -> Value.Base:
        lhs = expr.arguments[0].eval(env, stdlib=stdlib).expect(Type.Boolean()).value
        if not lhs:
            return Value.Boolean(False)
        return expr.arguments[1].eval(env, stdlib=stdlib).expect(Type.Boolean())


class _Or(Function):
    # logical || with short-circuit evaluation
    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        assert len(expr.arguments) == 2
        for arg in expr.arguments:
            if not isinstance(arg.type, Type.Boolean):
                raise Error.IncompatibleOperand(arg, "non-Boolean operand to ||")
            if expr._check_quant and arg.type.optional:
                raise Error.IncompatibleOperand(arg, "optional Boolean? operand to ||")
        return Type.Boolean()

    def __call__(
        self, expr: "Expr.Apply", env: Env.Bindings[Value.Base], stdlib: Base
    ) -> Value.Base:
        lhs = expr.arguments[0].eval(env, stdlib=stdlib).expect(Type.Boolean()).value
        if lhs:
            return Value.Boolean(True)
        return expr.arguments[1].eval(env, stdlib=stdlib).expect(Type.Boolean())


class _ArithmeticOperator(EagerFunction):
    # arithmetic infix operators
    # operands may be Int or Float; return Float iff either operand is Float

    name: str
    op: Callable

    def __init__(self, name: str, op: Callable) -> None:
        self.name = name
        self.op = op

    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        assert len(expr.arguments) == 2
        rt = Type.Int()
        if isinstance(expr.arguments[0].type, Type.Float) or isinstance(
            expr.arguments[1].type, Type.Float
        ):
            rt = Type.Float()
        try:
            expr.arguments[0].typecheck(rt)
            expr.arguments[1].typecheck(rt)
        except Error.StaticTypeMismatch:
            raise Error.IncompatibleOperand(
                expr, "Non-numeric operand to " + self.name + " operator"
            ) from None
        return rt

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        ans_type = self.infer_type(expr)
        ans = self.op(arguments[0].coerce(ans_type).value, arguments[1].coerce(ans_type).value)
        if ans_type == Type.Int():
            assert isinstance(ans, int)
            return Value.Int(ans)
        assert isinstance(ans, float)
        return Value.Float(ans)


class _AddOperator(_ArithmeticOperator):
    # + operator can also serve as concatenation for String.
    def __init__(self) -> None:
        super().__init__("+", lambda l, r: l + r)

    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        assert len(expr.arguments) == 2
        t2 = None
        if isinstance(expr.arguments[0].type, Type.String):
            t2 = expr.arguments[1].type
        elif isinstance(expr.arguments[1].type, Type.String):
            t2 = expr.arguments[0].type
        if t2 is None:
            # neither operand is a string; defer to _ArithmeticOperator
            return super().infer_type(expr)
        if not t2.coerces(Type.String(optional=not expr._check_quant)):
            raise Error.IncompatibleOperand(
                expr,
                "Cannot add/concatenate {} and {}".format(
                    str(expr.arguments[0].type), str(expr.arguments[1].type)
                ),
            )
        return Type.String()

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        ans_type = self.infer_type(expr)
        if not isinstance(ans_type, Type.String):
            return super()._call_eager(expr, arguments)
        ans = self.op(
            str(arguments[0].coerce(Type.String()).value),
            str(arguments[1].coerce(Type.String()).value),
        )
        assert isinstance(ans, str)
        return Value.String(ans)


class InterpolationAddOperator(_AddOperator):
    # + operator within an interpolation; accepts String? operands, evaluating to None if either
    # operand is None.

    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        either_string = sum(1 for arg in expr.arguments if isinstance(arg.type, Type.String)) > 0
        either_optional = sum(1 for arg in expr.arguments if arg.type.optional) > 0
        both_stringifiable = (
            sum(1 for arg in expr.arguments if arg.type.coerces(Type.String(optional=True))) > 1
        )
        return (
            Type.String(optional=True)
            if either_string and either_optional and both_stringifiable
            else super().infer_type(expr)
        )

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        if sum(1 for arg in arguments if isinstance(arg, Value.Null)):
            return Value.Null()
        return super()._call_eager(expr, arguments)


class _ComparisonOperator(EagerFunction):
    # Comparison operators can compare any two operands of the same type.
    # Furthermore, given one Int and one Float, coerces the Int to Float for
    # comparison.

    name: str
    op: Callable

    def __init__(self, name: str, op: Callable) -> None:
        self.name = name
        self.op = op

    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
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
                        isinstance(expr.arguments[0].type, Type.Int)
                        and isinstance(expr.arguments[1].type, Type.Float)
                    )
                    or (
                        isinstance(expr.arguments[0].type, Type.Float)
                        and isinstance(expr.arguments[1].type, Type.Int)
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
        return Type.Boolean()

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        assert len(arguments) == 2
        return Value.Boolean(self.op(arguments[0].value, arguments[1].value))


class _Size(EagerFunction):
    # size(): first argument can be File? or Array[File?]

    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        if not expr.arguments:
            raise Error.WrongArity(expr, 1)
        arg0ty = expr.arguments[0].type
        if not arg0ty.coerces(Type.File(optional=True)):
            if isinstance(arg0ty, Type.Array):
                if arg0ty.optional or not arg0ty.item_type.coerces(Type.File(optional=True)):
                    raise Error.StaticTypeMismatch(
                        expr.arguments[0], Type.Array(Type.File(optional=True)), arg0ty
                    )
            else:
                raise Error.StaticTypeMismatch(expr.arguments[0], Type.File(optional=True), arg0ty)
        if len(expr.arguments) == 2:
            if expr.arguments[1].type != Type.String():
                raise Error.StaticTypeMismatch(
                    expr.arguments[1], Type.String(), expr.arguments[1].type
                )
        elif len(expr.arguments) > 2:
            raise Error.WrongArity(expr, 2)
        return Type.Float()

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        # this default implementation attempts os.path.getsize() on the argument(s)
        files = arguments[0].coerce(Type.Array(Type.File()))
        unit = arguments[1].coerce(Type.String()) if len(arguments) > 1 else None

        ans = sum(float(os.path.getsize(fn.value)) for fn in files.value)

        if unit:
            try:
                ans /= float(_Size.unit_divisors[unit.value])
            except KeyError:
                raise Error.EvalError(expr, "size(): invalid unit " + unit.value)
        return Value.Float(ans)

    unit_divisors = {
        "K": 1000,
        "KB": 1000,
        "Ki": 1024,
        "KiB": 1024,
        "M": 1000000,
        "MB": 1000000,
        "Mi": 1048576,
        "MiB": 1048576,
        "G": 1000000000,
        "GB": 1000000000,
        "Gi": 1073741824,
        "GiB": 1073741824,
        "T": 1000000000000,
        "TB": 1000000000000,
        "Ti": 1099511627776,
        "TiB": 1099511627776,
    }


class _SelectFirst(EagerFunction):
    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        arg0ty = expr.arguments[0].type
        if not isinstance(arg0ty, Type.Array) or (
            expr.arguments[0]._check_quant and arg0ty.optional
        ):
            raise Error.StaticTypeMismatch(expr.arguments[0], Type.Array(Type.Any()), arg0ty)
        if isinstance(arg0ty.item_type, Type.Any):
            raise Error.IndeterminateType(expr.arguments[0], "can't infer item type of empty array")
        return arg0ty.item_type.copy(optional=False)

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        arr = arguments[0].coerce(Type.Array(Type.Any()))
        assert isinstance(arr, Value.Array)
        for arg in arr.value:
            if not isinstance(arg, Value.Null):
                return arg
        raise Error.NullValue(expr)


class _SelectAll(EagerFunction):
    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        arg0ty = expr.arguments[0].type
        if not isinstance(arg0ty, Type.Array) or (
            expr.arguments[0]._check_quant and arg0ty.optional
        ):
            raise Error.StaticTypeMismatch(expr.arguments[0], Type.Array(Type.Any()), arg0ty)
        if isinstance(arg0ty.item_type, Type.Any):
            raise Error.IndeterminateType(expr.arguments[0], "can't infer item type of empty array")
        return Type.Array(arg0ty.item_type.copy(optional=False))

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        arr = arguments[0].coerce(Type.Array(Type.Any()))
        assert isinstance(arr, Value.Array)
        arrty = arr.type
        assert isinstance(arrty, Type.Array)
        return Value.Array(
            arrty.item_type, [arg for arg in arr.value if not isinstance(arg, Value.Null)]
        )


class _ZipOrCross(EagerFunction):
    # 'a array -> 'b array -> ('a,'b) array
    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        if len(expr.arguments) != 2:
            raise Error.WrongArity(expr, 2)
        arg0ty: Type.Base = expr.arguments[0].type
        if not isinstance(arg0ty, Type.Array) or (expr._check_quant and arg0ty.optional):
            raise Error.StaticTypeMismatch(expr.arguments[0], Type.Array(Type.Any()), arg0ty)
        if isinstance(arg0ty.item_type, Type.Any):
            raise Error.IndeterminateType(expr.arguments[0], "can't infer item type of empty array")
        arg1ty: Type.Base = expr.arguments[1].type
        if not isinstance(arg1ty, Type.Array) or (expr._check_quant and arg1ty.optional):
            raise Error.StaticTypeMismatch(expr.arguments[1], Type.Array(Type.Any()), arg1ty)
        if isinstance(arg1ty.item_type, Type.Any):
            raise Error.IndeterminateType(expr.arguments[1], "can't infer item type of empty array")
        return Type.Array(
            Type.Pair(arg0ty.item_type, arg1ty.item_type),
            nonempty=(arg0ty.nonempty or arg1ty.nonempty),
        )

    def _coerce_args(
        self, expr: "Expr.Apply", arguments: List[Value.Base]
    ) -> Tuple[Type.Array, Value.Array, Value.Array]:
        ty = self.infer_type(expr)
        assert isinstance(ty, Type.Array) and isinstance(ty.item_type, Type.Pair)
        lhs = arguments[0].coerce(Type.Array(ty.item_type.left_type))
        rhs = arguments[1].coerce(Type.Array(ty.item_type.right_type))
        assert isinstance(lhs, Value.Array) and isinstance(rhs, Value.Array)
        return (ty, lhs, rhs)


class _Zip(_ZipOrCross):
    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Array:
        ty, lhs, rhs = self._coerce_args(expr, arguments)
        assert isinstance(ty, Type.Array) and isinstance(ty.item_type, Type.Pair)
        if len(lhs.value) != len(rhs.value):
            raise Error.EvalError(expr, "zip(): input arrays must have equal length")
        return Value.Array(
            ty.item_type,
            [Value.Pair(ty.item_type, (lhs.value[i], rhs.value[i])) for i in range(len(lhs.value))],
        )


class _Cross(_ZipOrCross):
    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Array:
        ty, lhs, rhs = self._coerce_args(expr, arguments)
        assert isinstance(ty, Type.Array) and isinstance(ty.item_type, Type.Pair)
        return Value.Array(
            ty.item_type,
            [
                Value.Pair(ty.item_type, (lhs_item, rhs_item))
                for lhs_item in lhs.value
                for rhs_item in rhs.value
            ],
        )


class _Flatten(EagerFunction):
    # t array array -> t array
    # TODO: if any of the input arrays are statically nonempty then so is output
    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        expr.arguments[0].typecheck(Type.Array(Type.Any()))
        # TODO: won't handle implicit coercion from T to Array[T]
        arg0ty = expr.arguments[0].type
        assert isinstance(arg0ty, Type.Array)
        if isinstance(arg0ty.item_type, Type.Any):
            return Type.Array(Type.Any())
        if not isinstance(arg0ty.item_type, Type.Array) or (
            expr._check_quant and arg0ty.item_type.optional
        ):
            raise Error.StaticTypeMismatch(
                expr.arguments[0], Type.Array(Type.Array(Type.Any())), arg0ty
            )
        return Type.Array(arg0ty.item_type.item_type)

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        ty = self.infer_type(expr)
        assert isinstance(ty, Type.Array)
        ans = []
        for row in arguments[0].coerce(Type.Array(ty)).value:
            ans.extend(row.value)
        return Value.Array(ty.item_type, ans)


class _Transpose(EagerFunction):
    # t array array -> t array array
    # TODO: if any of the input arrays are statically nonempty then so is output
    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        expr.arguments[0].typecheck(Type.Array(Type.Any()))
        # TODO: won't handle implicit coercion from T to Array[T]
        arg0ty = expr.arguments[0].type
        assert isinstance(arg0ty, Type.Array)
        if isinstance(arg0ty.item_type, Type.Any):
            return Type.Array(Type.Any())
        if not isinstance(arg0ty.item_type, Type.Array) or (
            expr._check_quant and arg0ty.item_type.optional
        ):
            raise Error.StaticTypeMismatch(
                expr.arguments[0], Type.Array(Type.Array(Type.Any())), arg0ty
            )
        return Type.Array(Type.Array(arg0ty.item_type.item_type))

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        ty = self.infer_type(expr)
        assert isinstance(ty, Type.Array) and isinstance(ty.item_type, Type.Array)
        mat = arguments[0].coerce(ty)
        assert isinstance(mat, Value.Array)
        n = None
        ans = []
        for row in mat.value:
            assert isinstance(row, Value.Array)
            if n is None:
                n = len(row.value)
                ans = [Value.Array(ty.item_type, []) for _ in row.value]
            if len(row.value) != n:
                raise Error.EvalError(expr, "transpose(): ragged input matrix")
            for i in range(len(row.value)):
                ans[i].value.append(row.value[i])
        return Value.Array(ty.item_type, ans)


class _Range(EagerFunction):
    # int -> int array
    # with special case: if the argument is a positive integer literal or
    # length(a_nonempty_array), then we can say the returned array is nonempty.

    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        expr.arguments[0].typecheck(Type.Int())
        nonempty = False
        arg0 = expr.arguments[0]
        if isinstance(arg0, Expr.Int) and arg0.value > 0:
            nonempty = True
        if isinstance(arg0, Expr.Apply) and arg0.function_name == "length":
            arg00ty = arg0.arguments[0].type
            if isinstance(arg00ty, Type.Array) and arg00ty.nonempty:
                nonempty = True
        return Type.Array(Type.Int(), nonempty=nonempty)

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        arg0 = arguments[0].coerce(Type.Int())
        assert isinstance(arg0, Value.Int)
        if arg0.value < 0:
            raise Error.EvalError(expr, "range() got negative argument")
        return Value.Array(Type.Int(), [Value.Int(x) for x in range(arg0.value)])


class _Prefix(EagerFunction):
    # string -> t array -> string array
    # if input array is nonempty then so is output

    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        if len(expr.arguments) != 2:
            raise Error.WrongArity(expr, 2)
        expr.arguments[0].typecheck(Type.String())
        expr.arguments[1].typecheck(Type.Array(Type.String()))
        arg1ty = expr.arguments[1].type
        return Type.Array(
            Type.String(), nonempty=(isinstance(arg1ty, Type.Array) and arg1ty.nonempty)
        )

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        pfx = arguments[0].coerce(Type.String()).value
        return Value.Array(
            Type.String(),
            [Value.String(pfx + s.coerce(Type.String()).value) for s in arguments[1].value],
        )
