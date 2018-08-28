# pyre-strict
import lark
import WDL._parser
from WDL import Expr as E

# Transformer from lark.Tree to WDL.Expr
class _ExprTransformer(lark.Transformer):
 
    def int(self, items):
        assert items == []

    def boolean_true(self, items):
        assert items == []
        return E.Boolean(True)
    def boolean_false(self, items):
        assert items == []
        return E.Boolean(False)
    def int(self, items):
        assert len(items) == 1
        return E.Int(int(items[0]))
    def float(self, items):
        assert len(items) == 1
        return E.Float(float(items[0]))
    def array(self, items):
        return E.Array(items)

    def negate(self, items):
        return E.Apply("_negate", items)

    def get(self, items):
        return E.Apply("_get", items)

    def ifthenelse(self, items):
        return E.IfThenElse(items)

# _ExprTransformer infix operators        
for op in ["land", "lor", "add", "sub", "mul", "div", "rem",
           "eqeq", "neq", "lt", "lte", "gt", "gte"]:
    def fn(self, items, op=op):
        assert len(items) == 2
        return E.Apply("_"+op, items)
    setattr(_ExprTransformer, op, classmethod(fn))

def parse_expr(txt : str) -> E.Base:
    return _ExprTransformer().transform(WDL._parser.parse(txt))
