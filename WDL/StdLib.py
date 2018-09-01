# pyre-strict
from typing import List, Tuple, Callable
import WDL.Type as T
import WDL.Value as V
import WDL.Expr as E
import WDL.Error as Error

# Special function for array access arr[index], returning the element type
class _ArrayGet(E._Function):
    def typecheck(self, expr : E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        if not isinstance(expr.arguments[0].type, T.Array):
            if isinstance(expr.arguments[0].type, T.AnyArray):
                # the user wrote: [][idx]
                raise Error.OutOfBounds(expr)
            else:
                raise Error.NotAnArray(expr.arguments[0])
        try:
            expr.arguments[1].typecheck(T.Int())
        except Error.StaticTypeMismatch:
            raise Error.StaticTypeMismatch(expr.arguments[1], T.Int(), expr.arguments[1].type, "Array index") from None
        return expr.arguments[0].type.item_type

    def __call__(self, expr : E.Apply, env : E.Env) -> V.Base:
        assert len(expr.arguments) == 2
        arr = expr.arguments[0].eval(env)
        assert isinstance(arr.type, T.AnyArray)
        assert isinstance(arr.value, list)
        idx = expr.arguments[1].eval(env).expect(T.Int()).value
        if idx < 0 or idx >= len(arr.value):
            raise Error.OutOfBounds(expr.arguments[1])
        return arr.value[idx] # pyre-ignore
E._stdlib["_get"] = _ArrayGet()


# _Function helper for simple functions with fixed argument and return types
class _StaticFunction(E._Function):
    name : str
    argument_types : List[T.Base]
    return_type : T.Base
    F : Callable

    def __init__(self, name : str, argument_types : List[T.Base], return_type : T.Base, F : Callable) -> None:
        self.name = name
        self.argument_types = argument_types
        self.return_type = return_type
        self.F = F

    def typecheck(self, expr : E.Apply) -> T.Base:
        if len(expr.arguments) != len(self.argument_types):
            raise Error.WrongArity(expr, len(self.argument_types))
        for i in range(len(self.argument_types)):
            try:
                expr.arguments[i].typecheck(self.argument_types[i])
            except Error.StaticTypeMismatch:
                raise Error.StaticTypeMismatch(expr.arguments[i], self.argument_types[i], expr.arguments[i].type, "{} argument #{}".format(name, i+1)) from None
        return self.return_type

    def __call__(self, expr : E.Apply, env : E.Env) -> V.Base:
        assert len(expr.arguments) == len(self.argument_types)
        argument_values = [arg.eval(env).coerce(ty) for arg, ty in zip(expr.arguments, self.argument_types)]
        ans : V.Base = self.F(*argument_values)
        return ans.coerce(self.return_type)

_static_functions = [
    ("_negate", [T.Boolean()], T.Boolean(), lambda x : V.Boolean(not x.value)),
    ("_land", [T.Boolean(), T.Boolean()], T.Boolean(), lambda l,r: V.Boolean(l.value and r.value)),
    ("_lor", [T.Boolean(), T.Boolean()], T.Boolean(), lambda l,r: V.Boolean(l.value or r.value)), # pyre-ignore
    ("_rem", [T.Int(), T.Int()], T.Int(), lambda l,r: V.Int(l.value % r.value)) # pyre-ignore
]
for name, argument_types, return_type, F in _static_functions:
    E._stdlib[name] = _StaticFunction(name, argument_types, return_type, F)

# Polymorphic functions

# arithmetic infix operators
# operands may be Int or Float; return Float iff either operand is Float
class _ArithmeticOperator(E._Function):
    name : str
    op : Callable

    def __init__(self, name : str, op : Callable) -> None:
        self.name = name
        self.op = op

    def typecheck(self, expr : E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        rt = T.Int()
        if expr.arguments[0].type == T.Float() or expr.arguments[1].type == T.Float():
            rt = T.Float()
        try:
            expr.arguments[0].typecheck(rt)
            expr.arguments[1].typecheck(rt)
        except Error.StaticTypeMismatch:
            raise Error.IncompatibleOperand(expr, "Non-numeric operand to " + self.name + " operator") from None
        return rt


    def __call__(self, expr : E.Apply, env : E.Env) -> V.Base:
        ans_type = self.typecheck(expr)
        ans = self.op(expr.arguments[0].eval(env).coerce(ans_type).value,
                      expr.arguments[1].eval(env).coerce(ans_type).value)
        if ans_type == T.Int():
            assert isinstance(ans, int) # pyre-ignore
            return V.Int(ans)
        assert isinstance(ans, float) # pyre-ignore
        return V.Float(ans)

E._stdlib["_add"] = _ArithmeticOperator("+", lambda l,r: l+r)  # pyre-ignore
E._stdlib["_sub"] = _ArithmeticOperator("-", lambda l,r: l-r)  # pyre-ignore
E._stdlib["_mul"] = _ArithmeticOperator("*", lambda l,r: l*r)  # pyre-ignore
E._stdlib["_div"] = _ArithmeticOperator("/", lambda l,r: l//r) # pyre-ignore

# Comparison operators can compare any two operands of the same type.
# Furthermore,
# - given one Int and one Float, coerces the Int to Float for comparison.
class _ComparisonOperator(E._Function):
    name : str
    op : Callable

    def __init__(self, name : str, op : Callable) -> None:
        self.name = name
        self.op = op

    def typecheck(self, expr : E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        if not (expr.arguments[0].type == expr.arguments[1].type or
                (expr.arguments[0].type == T.Int() and expr.arguments[1].type == T.Float()) or
                (expr.arguments[0].type == T.Float() and expr.arguments[1].type == T.Int())):
           raise Error.IncompatibleOperand(expr, "Cannot compare {} and {}".format(str(expr.arguments[0].type), str(expr.arguments[1].type)))
        return T.Boolean()

    def __call__(self, expr : E.Apply, env : E.Env) -> V.Base:
        assert len(expr.arguments) == 2
        return V.Boolean(self.op(expr.arguments[0].eval(env).value, expr.arguments[1].eval(env).value)) # pyre-ignore

E._stdlib["_eqeq"] = _ComparisonOperator("==", lambda l,r: l == r)
E._stdlib["_neq"] = _ComparisonOperator("!=", lambda l,r: l != r)
E._stdlib["_lt"] = _ComparisonOperator("<", lambda l,r: l < r)
E._stdlib["_lte"] = _ComparisonOperator("<=", lambda l,r: l <= r)
E._stdlib["_gt"] = _ComparisonOperator(">", lambda l,r: l > r)
E._stdlib["_gte"] = _ComparisonOperator(">=", lambda l,r: l >= r)
