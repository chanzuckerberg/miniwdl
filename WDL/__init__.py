"""Toolkit for static analysis of Workflow Description Language (WDL)"""
import lark
import inspect
import WDL._parser
from typing import List
from WDL import Expr as E
from WDL import Type as T
from WDL import Document as D
from WDL.Error import SourcePosition
from WDL import Env
import WDL.StdLib

def sp(meta) -> SourcePosition:
    return SourcePosition(line=meta.line, column=meta.column,
                          end_line=meta.end_line, end_column=meta.end_column)

def to_int(x):
    return int(x)

def to_float(x):
    return float(x)

# Transformer from lark.Tree to WDL.Expr
class _ExprTransformer(lark.Transformer):

    def boolean_true(self, items, meta) -> E.Base:
        return E.Boolean(sp(meta), True)
    def boolean_false(self, items, meta) -> E.Base:
        return E.Boolean(sp(meta), False)
    def int(self, items, meta) -> E.Base:
        assert len(items) == 1
        return E.Int(sp(meta), to_int(items[0]))
    def float(self, items, meta) -> E.Base:
        assert len(items) == 1
        return E.Float(sp(meta), to_float(items[0]))
    def string(self, items, meta) -> E.Base:
        parts = []
        for item in items:
            if isinstance(item, E.Base):
                parts.append(E.Placeholder(item.pos, {}, item))
            elif item.type.endswith("_FRAGMENT"):
                # for an interpolation fragment, item.value will end with "${"
                # so we strip that off. it'd be nice to make the grammar filter
                # that out since it does later filter out the "}"...
                parts.append(item.value[:-2])
            else:
                parts.append(item.value)
        # fix up a parsing quirk -- the opening quote got its own item but the
        # closing quote didn't
        assert len(parts) >= 2
        assert parts[0] in ['"', "'"]
        assert parts[-1][-1] in ['"', "'"] # pyre-fixme
        if len(parts[-1]) > 1:
            parts.append(parts[-1][-1]) # pyre-fixme
            parts[-2] = parts[-2][:-1] # pyre-fixme
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
        assert len(items) == 3
        return E.IfThenElse(sp(meta), *items)

    def ident(self, items, meta) -> E.Base:
        return E.Ident(sp(meta), [item.value for item in items])

# _ExprTransformer infix operators        
for op in ["land", "lor", "add", "sub", "mul", "div", "rem",
           "eqeq", "neq", "lt", "lte", "gt", "gte"]:
    def fn(self, items, meta, op=op):
        assert len(items) == 2
        return E.Apply(sp(meta), "_"+op, items)
    setattr(_ExprTransformer, op, lark.v_args(meta=True)(classmethod(fn))) # pyre-fixme

class _TypeTransformer(lark.Transformer):
    def int_type(self, items, meta):
        optional = False
        if len(items) > 0 and items[0].value == "?":
            optional = True
        return T.Int(optional)
    def float_type(self, items, meta):
        optional = False
        if len(items) > 0 and items[0].value == "?":
            optional = True
        return T.Float(optional)
    def boolean_type(self, items, meta):
        optional = False
        if len(items) > 0 and items[0].value == "?":
            optional = True
        return T.Boolean(optional)
    def string_type(self, items, meta):
        optional = False
        if len(items) > 0 and items[0].value == "?":
            optional = True
        return T.String(optional)
    def file_type(self, items, meta):
        optional = False
        if len(items) > 0 and items[0].value == "?":
            optional = True
        return T.File(optional)
    def array_type(self, items, meta):
        assert len(items) >= 1
        assert isinstance(items[0], WDL.Type.Base)
        optional = False
        nonempty = False
        if len(items) > 1:
            if items[1].value == "?":
                optional = True
            if items[1].value == "+":
                nonempty = True
        return T.Array(items[0], optional, nonempty)

class _DocTransformer(_ExprTransformer, _TypeTransformer):
    def decl(self, items, meta):
        return D.Decl(sp(meta), *items)
    def input_decls(self, items, meta):
        return {"inputs": items}
    def noninput_decls(self, items, meta):
        return {"decls": items}
    def string_literal(self, items, meta):
        assert len(items) == 1
        return items[0].value[1:-1]
    def placeholder_option(self, items, meta):
        assert len(items) == 2
        return (items[0].value, items[1])
    def placeholder(self, items, meta):
        options = dict(items[:-1])
        # TODO: error on duplicate options
        return E.Placeholder(sp(meta), options, items[-1])
    def command(self, items, meta):
        parts = []
        for item in items:
            if isinstance(item, E.Placeholder):
                parts.append(item)
            elif item.type.endswith("_FRAGMENT"):
                parts.append(item.value[:-2])
            else:
                parts.append(item.value)
        return {"command": E.String(sp(meta), parts)}
    def output_decls(self, items, meta):
        return {"outputs": items}
    def meta_kv(self, items, meta):
        return (items[0].value, items[1])
    def meta_object(self, items, meta):
        d = dict()
        for k, v in items:
            assert k not in d # TODO: helpful error for duplicate keys
            d[k] = v
        return d
    def meta_array(self, items, meta):
        return items
    def meta_section(self, items, meta):
        kind = items[0].value
        d = dict()
        d[kind] = items[1]
        return d
    def runtime_kv(self, items, meta):
        return (items[0].value, items[1])
    def runtime_section(self, items, meta):
        d = dict()
        for k,v in items:
            assert k not in d # TODO: helpful error for duplicate keys
            d[k] = v
        return {"runtime": d}
    def task(self, items, meta):
        d = {}
        for item in items:
            if isinstance(item, dict):
                for k,v in item.items():
                    assert k not in d # TODO: helpful error for redundant task sections
                    d[k] = v
            else:
                assert isinstance(item, str)
                assert "name" not in d
                d["name"] = item
        return D.Task(sp(meta), d["name"], d.get("inputs", []), d.get("decls", []), d["command"],
                      d.get("outputs", []), d.get("parameter_meta", {}), d.get("runtime", {}),
                      d.get("meta", {}))
    def tasks(self, items, meta):
        return items
    def call_input(self, items, meta):
        return (items[0].value, items[1])
    def call_inputs(self, items, meta):
        d = dict()
        for k, v in items:
            assert k not in d # TODO: helpful error for duplicate keys
            d[k] = v
        return d
    def call(self, items, meta):
        return D.Call(sp(meta), items[0], None, items[1] if len(items)>1 else dict())
    def call_as(self, items, meta):
        return D.Call(sp(meta), items[0], items[1].value, items[2] if len(items)>2 else dict())
    def scatter(self, items, meta):
        return D.Scatter(sp(meta), items[0].value, items[1], items[2:])
    def workflow(self, items, meta):
        elements = []
        outputs = None
        parameter_meta = dict()
        meta_section = dict()
        for item in items[1:]:
            if isinstance(item, dict):
                if "outputs" in item:
                    assert outputs is None # TODO helpful error message
                    outputs = item["outputs"]
                elif "meta" in item:
                    meta = item["meta"]
                elif "parameter_meta" in item:
                    parameter_meta = item["parameter_meta"]
                else:
                    assert False
            elif isinstance(item, D.Decl) or isinstance(item, D.Call) or isinstance(item, D.Scatter):
                elements.append(item)
            else:
                assert False
        return D.Workflow(sp(meta), items[0].value, elements, outputs, parameter_meta, meta_section)
    def document(self, items, meta):
        tasks = []
        workflow = None
        for item in items:
            if isinstance(item, D.Task):
                tasks.append(item)
            elif isinstance(item, D.Workflow):
                assert workflow is None
                workflow = item
            elif isinstance(item, lark.Tree) and item.data == "version":
                pass
            else:
                assert False
        return D.Document(sp(meta), tasks, workflow)

# have lark pass the 'meta' with line/column numbers to each transformer method
for _klass in [_ExprTransformer, _TypeTransformer, _DocTransformer]:
    for name, method in inspect.getmembers(_klass, inspect.isfunction):
        if not name.startswith('_'):
            setattr(_klass, name, lark.v_args(meta=True)(method)) # pyre-fixme

def parse_expr(txt : str) -> E.Base:
    """
    Parse an individual WDL expression into an abstract syntax tree
    
    :param txt: expression text
    """
    return _ExprTransformer().transform(WDL._parser.parse(txt, "expr")) # pyre-fixme

def parse_tasks(txt : str) -> List[D.Task]:
    """
    Parse zero or more WDL tasks
    """
    return _DocTransformer().transform(WDL._parser.parse(txt, "tasks")) # pyre-fixme

def parse_document(txt : str) -> D.Document:
    """
    Parse a WDL document, zero or more tasks with zero or one workflow.
    """
    return _DocTransformer().transform(WDL._parser.parse(txt, "document")) # pyre-fixme
