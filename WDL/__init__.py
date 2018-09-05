# pyre-strict
"""Toolkit for static analysis of Workflow Description Language (WDL)"""
import lark
import inspect
import WDL._parser
from WDL import Expr as E
import WDL.StdLib

def sp(meta) -> E.SourcePosition:
    return E.SourcePosition(line=meta.line, column=meta.column,
                            end_line=meta.end_line, end_column=meta.end_column)

# Transformer from lark.Tree to WDL.Expr
class _ExprTransformer(lark.Transformer):
    _static_env : E.StaticEnv

    def __init__(self, static_env : E.StaticEnv) -> None:
        self._static_env = static_env

    def boolean_true(self, items, meta) -> E.Base:
        assert items == []
        return E.Boolean(sp(meta), True)
    def boolean_false(self, items, meta) -> E.Base:
        assert items == []
        return E.Boolean(sp(meta), False)
    def int(self, items, meta) -> E.Base:
        assert len(items) == 1
        return E.Int(sp(meta), int(items[0]))
    def float(self, items, meta) -> E.Base:
        assert len(items) == 1
        return E.Float(sp(meta), float(items[0]))
    def string(self, items, meta) -> E.Base:
        parts = []
        for item in items:
            if isinstance(item, E.Base):
                parts.append(item)
            elif item.type.endswith("_FRAGMENT"):
                # for an interpolation fragment, item.value will end with "${"
                # so we strip that off. it'd be nice to make the grammar filter
                # that out since it does later filter out the "}"...
                parts.append(item.value[:-2])
            else:
                parts.append(item.value)
        return E.String(sp(meta), parts)
    def array(self, items, meta) -> E.Base:
        return E.Array(sp(meta), items)

    def apply(self, items, meta) -> E.Base:
        assert len(items) >= 1
        return E.Apply(sp(meta), items[0], items[1:])
    def negate(self, items, meta) -> E.Base:
        return E.Apply(sp(meta), "_negate", items)
    def get(self, items, meta) -> E.Base:
        return E.Apply(sp(meta), "_get", items)

    def ifthenelse(self, items, meta) -> E.Base:
        return E.IfThenElse(sp(meta), items)

    def ident(self, items, meta) -> E.Base:
        return E.Ident(sp(meta), [item.value for item in items], self._static_env)

# have lark pass the 'meta' with line/column numbers to each transformer method
for name, method in inspect.getmembers(_ExprTransformer, inspect.isfunction):
    if not name.startswith('_'):
        setattr(_ExprTransformer, name, lark.v_args(meta=True)(method))

# _ExprTransformer infix operators        
for op in ["land", "lor", "add", "sub", "mul", "div", "rem",
           "eqeq", "neq", "lt", "lte", "gt", "gte"]:
    def fn(self, items, meta, op=op):
        assert len(items) == 2
        return E.Apply(sp(meta), "_"+op, items)
    setattr(_ExprTransformer, op, lark.v_args(meta=True)(classmethod(fn)))

def parse_expr(txt : str, static_env : E.StaticEnv = None) -> E.Base:
    """
    Parse an individual WDL expression into an abstract syntax tree
    
    :param txt: expression text
    :param static_env: provides the types of any identifiers used in the expression
    """
    if static_env is None:
        static_env = E.StaticEnv()
    return _ExprTransformer(static_env).transform(WDL._parser.parse(txt, "expr"))
