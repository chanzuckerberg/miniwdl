from typing import List, Tuple, Callable
import WDL.Type as Ty
import WDL.Value as Val
import WDL.Expr as Expr

_static_functions = [
    ("_negate", [Ty.Boolean()], Ty.Boolean(), lambda x : Val.Boolean(not x.value)),
    ("_land", [Ty.Boolean(), Ty.Boolean()], Ty.Boolean(), lambda l,r: Val.Boolean(l.value and r.value)),
    ("_lor", [Ty.Boolean(), Ty.Boolean()], Ty.Boolean(), lambda l,r: Val.Boolean(l.value or r.value)),
    ("_rem", [Ty.Int(), Ty.Int()], Ty.Int(), lambda l,r: Val.Int(l.value % r.value))
]
for name, argument_types, return_type, F in _static_functions:
    Expr._stdlib[name] = Expr._StaticFunction(name, argument_types, return_type, F)

# Polymorphic functions

# arithmetic infix operators
# operands may be Int or Float; return Float iff either operand is Float
class _ArithmeticOperator(Expr._Function):
    name : str
    op : Callable

    def __init__(self, name : str, op : Callable):
        self.name = name
        self.op = op

    def typecheck(self, expr : Expr.Apply):
        assert len(expr.arguments) == 2
        rt = Ty.Int()
        if expr.arguments[0].type == Ty.Float() or expr.arguments[1].type == Ty.Float():
            rt = Ty.Float()
        try:
            expr.arguments[0].typecheck(rt)
            expr.arguments[1].typecheck(rt)
        except Expr.StaticTypeMismatch:
            raise Expr.IncompatibleOperand(expr, "Non-numeric operand to " + self.name + " operator") from None
        return rt


    def __call__(self, expr : Expr.Apply, env : Expr.Env) -> Val.Base:
        ans_type = self.typecheck(expr)
        ans = self.op(expr.arguments[0].eval(env).coerce(ans_type).value,
                      expr.arguments[1].eval(env).coerce(ans_type).value)
        if ans_type == Ty.Int():
            assert isinstance(ans, int)
            return Val.Int(ans)
        assert isinstance(ans, float)
        return Val.Float(ans)

Expr._stdlib["_add"] = _ArithmeticOperator("+", lambda l,r: l+r)
Expr._stdlib["_sub"] = _ArithmeticOperator("-", lambda l,r: l-r)
Expr._stdlib["_mul"] = _ArithmeticOperator("*", lambda l,r: l*r)
Expr._stdlib["_div"] = _ArithmeticOperator("/", lambda l,r: l//r)

# Comparison operators can compare any two operands of the same type; also,
# given one Int and one Float, coerces the Int to Float for comparison.
class _ComparisonOperator(Expr._Function):
    name : str
    op : Callable

    def __init__(self, name : str, op : Callable):
        self.name = name
        self.op = op

    def typecheck(self, expr : Expr.Apply):
        assert len(expr.arguments) == 2
        if not (expr.arguments[0].type == expr.arguments[1].type or
                (expr.arguments[0].type == Ty.Int() and expr.arguments[1].type == Ty.Float()) or
                (expr.arguments[0].type == Ty.Float() and expr.arguments[1].type == Ty.Int())):
           raise Expr.IncompatibleOperand(expr, "Cannot compare {} and {}".format(str(expr.arguments[0].type), str(expr.arguments[1].type)))
        return Ty.Boolean()

    def __call__(self, expr : Expr.Apply, env : Expr.Env) -> Val.Base:
        assert len(expr.arguments) == 2
        return Val.Boolean(self.op(expr.arguments[0].eval(env).value, expr.arguments[1].eval(env).value))

Expr._stdlib["_eqeq"] = _ComparisonOperator("==", lambda l,r: l == r)
Expr._stdlib["_neq"] = _ComparisonOperator("!=", lambda l,r: l != r)
Expr._stdlib["_lt"] = _ComparisonOperator("<", lambda l,r: l < r)
Expr._stdlib["_lte"] = _ComparisonOperator("<=", lambda l,r: l <= r)
Expr._stdlib["_gt"] = _ComparisonOperator(">", lambda l,r: l > r)
Expr._stdlib["_gte"] = _ComparisonOperator(">=", lambda l,r: l >= r)
