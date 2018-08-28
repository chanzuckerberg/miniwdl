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

    def typecheck(self, arguments : List[Expr.Base]):
        assert len(arguments) == 2
        rt = Ty.Int()
        if arguments[0].type == Ty.Float() or arguments[1].type == Ty.Float():
            rt = Ty.Float()
        try:
            arguments[0].type.expect(rt)
            arguments[1].type.expect(rt)
        except Ty.StaticTypeError:
            raise Ty.StaticTypeError("non-numeric operand to" + self.name + " operator") from None
        return rt


    def apply(self, arguments : List[Expr.Base], env : Expr.Env) -> Val.Base:
        ans_type = self.typecheck(arguments)
        ans = self.op(arguments[0].eval(env).coerce(ans_type).value,
                      arguments[1].eval(env).coerce(ans_type).value)
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

    def typecheck(self, arguments : List[Expr.Base]):
        assert len(arguments) == 2
        if not (arguments[0].type == arguments[1].type or
                (arguments[0].type == Ty.Int() and arguments[1].type == Ty.Float()) or
                (arguments[0].type == Ty.Float() and arguments[1].type == Ty.Int())):
           raise Ty.StaticTypeError("cannot compare {} and {}", str(arguments[0].type), str(arguments[1].type))
        return Ty.Boolean()

    def apply(self, arguments : List[Expr.Base], env : Expr.Env) -> Val.Base:
        assert len(arguments) == 2
        return Val.Boolean(self.op(arguments[0].eval(env).value, arguments[1].eval(env).value))

Expr._stdlib["_eqeq"] = _ComparisonOperator("==", lambda l,r: l == r)
Expr._stdlib["_neq"] = _ComparisonOperator("!=", lambda l,r: l != r)
Expr._stdlib["_lt"] = _ComparisonOperator("<", lambda l,r: l < r)
Expr._stdlib["_lte"] = _ComparisonOperator("<=", lambda l,r: l <= r)
Expr._stdlib["_gt"] = _ComparisonOperator(">", lambda l,r: l > r)
Expr._stdlib["_gte"] = _ComparisonOperator(">=", lambda l,r: l >= r)
