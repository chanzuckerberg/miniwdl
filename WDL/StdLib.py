# pyre-strict
from typing import List, Tuple, Callable, Any
import WDL.Type as T
import WDL.Value as V
import WDL.Expr as E
import WDL.Env as Env
import WDL.Error as Error
import copy

# Special function for array access arr[index], returning the element type
#                      or map access map[key], returning the value type
class _Get(E._Function):
    def infer_type(self, expr : E.Apply) -> T.Base:
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
        elif isinstance(lhs.type, T.Map):
            if lhs.type.item_type is None:
                raise Error.OutOfBounds(expr)
            try:
                rhs.typecheck(lhs.type.item_type[0])
            except Error.StaticTypeMismatch:
                raise Error.StaticTypeMismatch(rhs, lhs.type.item_type[0], rhs.type, "Map key") from None
            return lhs.type.item_type[1]
        else:
            raise Error.NotAnArray(lhs)

    def __call__(self, expr : E.Apply, env : E.Env) -> V.Base:
        assert len(expr.arguments) == 2
        lhs = expr.arguments[0]
        rhs = expr.arguments[1]
        if isinstance(lhs.type, T.Array): # pyre-fixme
            arr = lhs.eval(env)
            assert isinstance(arr, V.Array)
            assert isinstance(arr.type, T.Array)
            assert isinstance(arr.value, list)
            idx = rhs.eval(env).expect(T.Int()).value
            if idx < 0 or idx >= len(arr.value):
                raise Error.OutOfBounds(rhs)
            return arr.value[idx] # pyre-fixme
        elif isinstance(lhs.type, T.Map):
            mp = lhs.eval(env)
            assert isinstance(mp, V.Map)
            assert isinstance(mp.type, T.Map)
            assert mp.type.item_type is not None
            assert isinstance(mp.value, list)
            ans = None
            key = rhs.eval(env).expect(mp.type.item_type[0])
            for k,v in mp.value:
                if key == k:
                    ans = v.expect(mp.type.item_type[1])
            if ans is None:
                raise Error.OutOfBounds(rhs) # TODO: KeyNotFound
            return ans # pyre-fixme
        else:
            assert False
E._stdlib["_get"] = _Get()

# Pair get (EXPR.left/EXPR.right)
# The special case where EXPR is an identifier goes a different path, through
# Expr.Ident.
class _PairGet(E._Function):
    left : bool
    def __init__(self, left : bool) -> None:
        self.left = left
    def infer_type(self, expr : E.Apply) -> T.Base:
        assert len(expr.arguments) == 1
        if not isinstance(expr.arguments[0].type, T.Pair):
            raise Error.NotAPair(expr.arguments[0])
        return expr.arguments[0].type.left_type if self.left else expr.arguments[0].type.right_type
    def __call__(self, expr : E.Apply, env : E.Env) -> V.Base:
        assert len(expr.arguments) == 1
        pair = expr.arguments[0].eval(env)
        assert isinstance(pair.type, T.Pair)
        assert isinstance(pair.value, tuple)
        return pair.value[0] if self.left else pair.value[1]
E._stdlib["_get_left"] = _PairGet(True)
E._stdlib["_get_right"] = _PairGet(False)

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

    def infer_type(self, expr : E.Apply) -> T.Base:
        if len(expr.arguments) != len(self.argument_types):
            raise Error.WrongArity(expr, len(self.argument_types))
        for i in range(len(self.argument_types)):
            try:
                expr.arguments[i].typecheck(self.argument_types[i])
            except Error.StaticTypeMismatch:
                raise Error.StaticTypeMismatch(expr.arguments[i], self.argument_types[i], expr.arguments[i].type, "{} argument #{}".format(self.name, i+1)) from None
        return self.return_type

    def __call__(self, expr : E.Apply, env : E.Env) -> V.Base:
        assert len(expr.arguments) == len(self.argument_types)
        argument_values = [arg.eval(env).coerce(ty) for arg, ty in zip(expr.arguments, self.argument_types)]
        ans : V.Base = self.F(*argument_values)
        return ans.coerce(self.return_type)

_static_functions : List[Tuple[str, List[T.Base], T.Base, Any]] = [
    ("_negate", [T.Boolean()], T.Boolean(), lambda x : V.Boolean(not x.value)), # pyre-fixme
    ("_land", [T.Boolean(), T.Boolean()], T.Boolean(), lambda l,r: V.Boolean(l.value and r.value)), # pyre-fixme
    ("_lor", [T.Boolean(), T.Boolean()], T.Boolean(), lambda l,r: V.Boolean(l.value or r.value)), # pyre-fixme
    ("_rem", [T.Int(), T.Int()], T.Int(), lambda l,r: V.Int(l.value % r.value)), # pyre-fixme
    ("stdout", [], T.String(), lambda: exec('raise NotImplementedError()')),
    ("size", [T.File(), T.String()], T.Float(), lambda file: exec('raise NotImplementedError()')),
    ("ceil", [T.Float()], T.Int(), lambda x: exec('raise NotImplementedError()')),
    ("round", [T.Float()], T.Int(), lambda x: exec('raise NotImplementedError()')),
    ("glob", [T.String()], T.Array(T.File()), lambda pattern: exec('raise NotImplementedError()')),
    ("read_int", [T.String()], T.Int(), lambda pattern: exec('raise NotImplementedError()')),
    ("read_boolean", [T.String()], T.Boolean(), lambda pattern: exec('raise NotImplementedError()')),
    ("read_string", [T.String()], T.String(), lambda pattern: exec('raise NotImplementedError()')),
    ("read_float", [T.String()], T.Float(), lambda pattern: exec('raise NotImplementedError()')),
    ("read_array", [T.String()], T.Array(None), lambda pattern: exec('raise NotImplementedError()')),
    ("read_map", [T.String()], T.Map(None), lambda pattern: exec('raise NotImplementedError()')),
    ("read_lines", [T.String()], T.Array(None), lambda pattern: exec('raise NotImplementedError()')),
    ("read_tsv", [T.String()], T.Array(T.Array(T.String())), lambda pattern: exec('raise NotImplementedError()')),
    ("write_lines", [T.Array(T.String())], T.File(), lambda pattern: exec('raise NotImplementedError()')),
    ("write_tsv", [T.Array(T.Array(T.String()))], T.File(), lambda pattern: exec('raise NotImplementedError()')),
    ("write_map", [T.Map(None)], T.File(), lambda pattern: exec('raise NotImplementedError()')),
    ("range", [T.Int()], T.Array(T.Int()), lambda high: exec('raise NotImplementedError()')),
    ("sub", [T.String(), T.String(), T.String()], T.String(), lambda high: exec('raise NotImplementedError()')),
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

    def infer_type(self, expr : E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        rt = T.Int()
        if isinstance(expr.arguments[0].type, T.Float) or isinstance(expr.arguments[1].type, T.Float):
            rt = T.Float()
        try:
            expr.arguments[0].typecheck(rt)
            expr.arguments[1].typecheck(rt)
        except Error.StaticTypeMismatch:
            raise Error.IncompatibleOperand(expr, "Non-numeric operand to " + self.name + " operator") from None
        return rt


    def __call__(self, expr : E.Apply, env : E.Env) -> V.Base:
        ans_type = self.infer_type(expr)
        ans = self.op(expr.arguments[0].eval(env).coerce(ans_type).value,
                      expr.arguments[1].eval(env).coerce(ans_type).value)
        if ans_type == T.Int():
            assert isinstance(ans, int)
            return V.Int(ans)
        assert isinstance(ans, float)
        return V.Float(ans)

E._stdlib["_sub"] = _ArithmeticOperator("-", lambda l,r: l-r)  # pyre-ignore
E._stdlib["_mul"] = _ArithmeticOperator("*", lambda l,r: l*r)  # pyre-ignore
E._stdlib["_div"] = _ArithmeticOperator("/", lambda l,r: l//r) # pyre-ignore

# + operator can also serve as concatenation for String.
# String+Int and String+Float also permitted
class _AddOperator(_ArithmeticOperator):
    def __init__(self) -> None:
        super().__init__("+", lambda l,r: l+r) # pyre-ignore

    def infer_type(self, expr : E.Apply) -> T.Base:
        assert len(expr.arguments) == 2
        t2 = None
        if isinstance(expr.arguments[0].type, T.String):
            t2 = expr.arguments[1].type
        elif isinstance(expr.arguments[1].type, T.String):
            t2 = expr.arguments[0].type
        if t2 is None:
            # neither operand is a string; defer to _ArithmeticOperator
            return super().infer_type(expr)
        if sum(1 for c in [T.String, T.Int, T.Float] if isinstance(t2, c)) == 0:
            return Error.IncompatibleOperand(expr, "Cannot add/concatenate {} and {}".format(str(expr.arguments[0].type), str(expr.arguments[1].type)))
        return T.String()

    def __call__(self, expr: E.Apply, env : E.Env) -> V.Base:
        ans_type = self.infer_type(expr)
        if not isinstance(ans_type, T.String):
            return super().__call__(expr, env)
        ans = self.op(str(expr.arguments[0].eval(env).value),
                      str(expr.arguments[1].eval(env).value))
        assert isinstance(ans, str)
        return V.String(ans)

E._stdlib["_add"] = _AddOperator()

# Comparison operators can compare any two operands of the same type.
# Furthermore,
# - given one Int and one Float, coerces the Int to Float for comparison.
class _ComparisonOperator(E._Function):
    name : str
    op : Callable

    def __init__(self, name : str, op : Callable) -> None:
        self.name = name
        self.op = op

    def infer_type(self, expr : E.Apply) -> T.Base:
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
E._stdlib["_lt"] = _ComparisonOperator("<", lambda l,r: l < r) # pyre-fixme
E._stdlib["_lte"] = _ComparisonOperator("<=", lambda l,r: l <= r) # pyre-fixme
E._stdlib["_gt"] = _ComparisonOperator(">", lambda l,r: l > r) # pyre-fixme
E._stdlib["_gte"] = _ComparisonOperator(">=", lambda l,r: l >= r) # pyre-fixme

# defined(): accepts any type...
class _Defined(E._Function):
    def infer_type(self, expr : E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        return T.Boolean()

    def __call__(self, expr : E.Apply, env : Env.Values) -> V.Base:
        if isinstance(expr.arguments[0].eval(env), V.Null):
            return V.Boolean(False)
        return V.Boolean(True)
E._stdlib["defined"] = _Defined()

class _Length(E._Function):
    def infer_type(self, expr : E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        if not isinstance(expr.arguments[0].type, T.Array):
            raise Error.StaticTypeMismatch(expr, T.Array(None), expr.arguments[0].type)
        return T.Int()

    def __call__(self, expr : E.Apply, env : Env.Values) -> V.Base:
        v = expr.arguments[0].eval(env)
        if isinstance(v, V.Null):
            return V.Int(0)
        assert isinstance(v.value, list)
        return V.Int(len(v.value))
E._stdlib["length"] = _Length()

class _SelectFirst(E._Function):
    def infer_type(self, expr : E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        if not isinstance(expr.arguments[0].type, T.Array):
            raise Error.StaticTypeMismatch(expr.arguments[0], T.Array(None), expr.arguments[0].type)
        if expr.arguments[0].type.item_type is None:
            raise Error.EmptyArray(expr.arguments[0]) # TODO: error for 'indeterminate type'
        ty = copy.copy(expr.arguments[0].type.item_type)
        assert isinstance(ty, T.Base)
        ty.optional = False
        return ty

    def __call__(self, expr : E.Apply, env : Env.Values) -> V.Base:
        raise NotImplementedError()
E._stdlib["select_first"] = _SelectFirst()

class _SelectAll(E._Function):
    def infer_type(self, expr : E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        if not isinstance(expr.arguments[0].type, T.Array):
            raise Error.StaticTypeMismatch(expr.arguments[0], T.Array(None), expr.arguments[0].type)
        if expr.arguments[0].type.item_type is None:
            raise Error.EmptyArray(expr.arguments[0]) # TODO: error for 'indeterminate type'
        ty = copy.copy(expr.arguments[0].type.item_type)
        assert isinstance(ty, T.Base)
        ty.optional = False
        return T.Array(ty)

    def __call__(self, expr : E.Apply, env : Env.Values) -> V.Base:
        raise NotImplementedError()
E._stdlib["select_all"] = _SelectAll()

class _Zip(E._Function):
    # 'a array -> 'b array -> ('a,'b) array
    def infer_type(self, expr : E.Apply) -> T.Base:
        if len(expr.arguments) != 2:
            raise Error.WrongArity(expr, 2)
        if not isinstance(expr.arguments[0].type, T.Array):
            raise Error.StaticTypeMismatch(expr.arguments[0], T.Array(None), expr.arguments[0].type)
        if expr.arguments[0].type.item_type is None:
            raise Error.EmptyArray(expr.arguments[0]) # TODO: error for 'indeterminate type'
        if not isinstance(expr.arguments[1].type, T.Array):
            raise Error.StaticTypeMismatch(expr.arguments[1], T.Array(None), expr.arguments[0].type)
        if expr.arguments[1].type.item_type is None:
            raise Error.EmptyArray(expr.arguments[1]) # TODO: error for 'indeterminate type'
        return T.Array(T.Pair(expr.arguments[0].type.item_type, expr.arguments[1].type.item_type))

    def __call__(self, expr : E.Apply, env : Env.Values) -> V.Base:
        raise NotImplementedError()
E._stdlib["zip"] = _Zip()
E._stdlib["cross"] = _Zip() # TODO

class _Basename(E._Function):
    def infer_type(self, expr : E.Apply) -> T.Base:
        if len(expr.arguments) not in [1,2]:
            raise Error.WrongArity(expr, 2)
        expr.arguments[0].typecheck(T.String())
        if len(expr.arguments) == 2:
            expr.arguments[1].typecheck(T.String())
        return T.String()

    def __call__(self, expr : E.Apply, env : Env.Values) -> V.Base:
        raise NotImplementedError()
E._stdlib["basename"] = _Basename()

class _Flatten(E._Function):
    # t array array -> t array
    def infer_type(self, expr : E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        expr.arguments[0].typecheck(T.Array(None))
        # TODO: won't handle implicit coercion from T to Array[T]
        assert isinstance(expr.arguments[0].type, T.Array)
        if expr.arguments[0].type.item_type is None:
            return T.Array(None)
        elif not isinstance(expr.arguments[0].type.item_type, T.Array):
            raise Error.StaticTypeMismatch(expr.arguments[0], T.Array(T.Array(None)), expr.arguments[0].type)
        return T.Array(expr.arguments[0].type.item_type.item_type) #pyre-fixme
    def __call__(self, expr : E.Apply, env : Env.Values) -> V.Base:
        raise NotImplementedError()
E._stdlib["flatten"] = _Flatten()

class _Transpose(E._Function):
    # t array array -> t array array
    def infer_type(self, expr : E.Apply) -> T.Base:
        if len(expr.arguments) != 1:
            raise Error.WrongArity(expr, 1)
        expr.arguments[0].typecheck(T.Array(None))
        # TODO: won't handle implicit coercion from T to Array[T]
        assert isinstance(expr.arguments[0].type, T.Array)
        if expr.arguments[0].type.item_type is None:
            return T.Array(None)
        elif not isinstance(expr.arguments[0].type.item_type, T.Array):
            raise Error.StaticTypeMismatch(expr.arguments[0], T.Array(T.Array(None)), expr.arguments[0].type)
        return expr.arguments[0].type
    def __call__(self, expr : E.Apply, env : Env.Values) -> V.Base:
        raise NotImplementedError()
E._stdlib["transpose"] = _Transpose()
