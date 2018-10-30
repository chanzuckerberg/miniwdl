"""Toolkit for static analysis of Workflow Description Language (WDL)"""
import os, errno
import lark
import inspect
import WDL._parser
from typing import List
from WDL import Expr as E
from WDL import Type as T
from WDL import Document as D
from WDL import Error as Err
from WDL.Error import SourcePosition
from WDL import Env
import WDL.StdLib

def sp(filename,meta) -> SourcePosition:
    return SourcePosition(filename=filename, line=meta.line, column=meta.column,
                          end_line=meta.end_line, end_column=meta.end_column)

def to_int(x):
    return int(x)

def to_float(x):
    return float(x)

# Transformer from lark.Tree to WDL.Expr
class _ExprTransformer(lark.Transformer):
    def __init__(self, file : str) -> None:
        self.filename = file

    def boolean_true(self, items, meta) -> E.Base:
        return E.Boolean(sp(self.filename, meta), True)
    def boolean_false(self, items, meta) -> E.Base:
        return E.Boolean(sp(self.filename, meta), False)
    def int(self, items, meta) -> E.Base:
        assert len(items) == 1
        return E.Int(sp(self.filename, meta), to_int(items[0]))
    def float(self, items, meta) -> E.Base:
        assert len(items) == 1
        return E.Float(sp(self.filename, meta), to_float(items[0]))
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
        return E.String(sp(self.filename, meta), parts)
    def array(self, items, meta) -> E.Base:
        return E.Array(sp(self.filename, meta), items)

    def apply(self, items, meta) -> E.Base:
        assert len(items) >= 1
        return E.Apply(sp(self.filename, meta), items[0], items[1:])
    def negate(self, items, meta) -> E.Base:
        return E.Apply(sp(self.filename, meta), "_negate", items)
    def get(self, items, meta) -> E.Base:
        return E.Apply(sp(self.filename, meta), "_get", items)

    def pair(self, items, meta) -> E.Base:
        assert len(items) == 2
        return E.Pair(sp(self.filename, meta), items[0], items[1])
    def get_left(self, items, meta) -> E.Base:
        return E.Apply(sp(self.filename, meta), "_get_left", items)
    def get_right(self, items, meta) -> E.Base:
        return E.Apply(sp(self.filename, meta), "_get_right", items)

    def map_kv(self, items, meta) -> E.Base:
        assert len(items) == 2
        return (items[0], items[1])
    def map(self, items, meta) -> E.Base:
        return E.Map(sp(self.filename, meta), items)
    def ifthenelse(self, items, meta) -> E.Base:
        assert len(items) == 3
        return E.IfThenElse(sp(self.filename, meta), *items)

    def ident(self, items, meta) -> E.Base:
        return E.Ident(sp(self.filename, meta), [item.value for item in items])

# _ExprTransformer infix operators        
for op in ["land", "lor", "add", "sub", "mul", "div", "rem",
           "eqeq", "neq", "lt", "lte", "gt", "gte"]:
    def fn(self, items, meta, op=op):
        assert len(items) == 2
        return E.Apply(sp(self.filename, meta), "_"+op, items)
    setattr(_ExprTransformer, op, lark.v_args(meta=True)(classmethod(fn))) # pyre-fixme

class _TypeTransformer(lark.Transformer):
    def __init__(self, file : str) -> None:
        self.filename = file

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
    def map_type(self, items, meta):
        assert len(items) >= 2
        assert isinstance(items[0], WDL.Type.Base)
        assert isinstance(items[1], WDL.Type.Base)
        optional = False
        if len(items) > 2:
            if items[2].value == "?":
                optional = True
        return T.Map((items[0], items[1]), optional)
    def pair_type(self, items, meta):
        assert len(items) >= 2
        assert isinstance(items[0], WDL.Type.Base)
        assert isinstance(items[1], WDL.Type.Base)
        optional = False
        if len(items) > 2:
            if items[2].value == "?":
                optional = True
        return T.Pair(items[0], items[1], optional)

class _DocTransformer(_ExprTransformer, _TypeTransformer):
    def __init__(self, file : str) -> None:
        self.filename = file

    def decl(self, items, meta):
        return D.Decl(sp(self.filename, meta), *items)
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
        if len(options.items()) < len(items) - 1:
            raise Err.MultipleDefinitions(sp(self.filename, meta), "duplicate options in expression placeholder")
        return E.Placeholder(sp(self.filename, meta), options, items[-1])
    def command(self, items, meta):
        parts = []
        for item in items:
            if isinstance(item, E.Placeholder):
                parts.append(item)
            elif item.type.endswith("_FRAGMENT"):
                parts.append(item.value[:-2])
            else:
                parts.append(item.value)
        return {"command": E.String(sp(self.filename, meta), parts)}
    def output_decls(self, items, meta):
        return {"outputs": items}
    def meta_kv(self, items, meta):
        return (items[0].value, items[1])
    def meta_object(self, items, meta):
        d = dict()
        for k, v in items:
            if k in d:
                raise Err.MultipleDefinitions(sp(self.filename, meta), "duplicate keys in meta object")
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
            # TODO: restore duplicate check, cf. https://github.com/gatk-workflows/five-dollar-genome-analysis-pipeline/blob/89f11befc13abae97ab8fb1b457731f390c8728d/tasks_pipelines/qc.wdl#L288
            #if k in d:
            #    raise Err.MultipleDefinitions(sp(self.filename, meta), "duplicate keys in runtime section")
            d[k] = v
        return {"runtime": d}
    def task(self, items, meta):
        d = {}
        for item in items:
            if isinstance(item, dict):
                for k,v in item.items():
                    if k in d:
                        raise Err.MultipleDefinitions(sp(self.filename, meta), "redundant sections in task")
                    d[k] = v
            else:
                assert isinstance(item, str)
                assert "name" not in d
                d["name"] = item
        return D.Task(sp(self.filename, meta), d["name"], d.get("inputs", []), d.get("decls", []), d["command"],
                      d.get("outputs", []), d.get("parameter_meta", {}), d.get("runtime", {}),
                      d.get("meta", {}))
    def tasks(self, items, meta):
        return items
    def call_input(self, items, meta):
        return (items[0].value, items[1])
    def call_inputs(self, items, meta):
        d = dict()
        for k, v in items:
            if k in d:
                raise Err.MultipleDefinitions(sp(self.filename, meta), "duplicate keys in call inputs")
            d[k] = v
        return d
    def call(self, items, meta):
        return D.Call(sp(self.filename, meta), items[0], None, items[1] if len(items)>1 else dict())
    def call_as(self, items, meta):
        return D.Call(sp(self.filename, meta), items[0], items[1].value, items[2] if len(items)>2 else dict())
    def scatter(self, items, meta):
        return D.Scatter(sp(self.filename, meta), items[0].value, items[1], items[2:])
    def conditional(self, items, meta):
        return D.Conditional(sp(self.filename, meta), items[0], items[1:])
    def workflow(self, items, meta):
        elements = []
        outputs = None
        parameter_meta = None
        meta_section = None
        for item in items[1:]:
            if isinstance(item, dict):
                if "outputs" in item:
                    if outputs is not None:
                        raise Err.MultipleDefinitions(sp(self.filename, meta), "redundant sections in workflow")
                    outputs = item["outputs"]
                elif "meta" in item:
                    if meta_section is not None:
                        raise Err.MultipleDefinitions(sp(self.filename, meta), "redundant sections in workflow")
                    meta_section = item["meta"]
                elif "parameter_meta" in item:
                    if parameter_meta is not None:
                        raise Err.MultipleDefinitions(sp(self.filename, meta), "redundant sections in workflow")
                    parameter_meta = item["parameter_meta"]
                else:
                    assert False
            elif isinstance(item, D.Decl) or isinstance(item, D.Call) or isinstance(item, D.Scatter) or isinstance(item, D.Conditional):
                elements.append(item)
            else:
                assert False
        return D.Workflow(sp(self.filename, meta), items[0].value, elements, outputs,
                          parameter_meta or dict(), meta_section or dict())
    def import_doc(self, items, meta):
        uri = items[0]
        if len(items) > 1:
            namespace = items[1]
        else:
            namespace = uri
            try:
                namespace = namespace[namespace.rindex('/')+1:]
            except ValueError:
                pass
            if namespace.endswith(".wdl"):
                namespace = namespace[:-4]
        # TODO: validate namespace
        return {"import": (uri,namespace)}
    def document(self, items, meta):
        imports = []
        tasks = []
        workflow = None
        for item in items:
            if isinstance(item, D.Task):
                tasks.append(item)
            elif isinstance(item, D.Workflow):
                if workflow is not None:
                    raise Err.MultipleDefinitions(sp(self.filename, meta), "Document has multiple workflows")
                workflow = item
            elif isinstance(item, lark.Tree) and item.data == "version":
                pass
            elif isinstance(item, dict) and "import" in item:
                imports.append(item["import"])
            else:
                assert False
        return D.Document(sp(self.filename, meta), imports, tasks, workflow)

# have lark pass the 'meta' with line/column numbers to each transformer method
for _klass in [_ExprTransformer, _TypeTransformer, _DocTransformer]:
    for name, method in inspect.getmembers(_klass, inspect.isfunction):
        if not name.startswith('_'):
            setattr(_klass, name, lark.v_args(meta=True)(method)) # pyre-fixme

def parse_expr(txt : str) -> E.Base:
    """
    Parse an isolated WDL expression text into an abstract syntax tree
    """
    try:
        return _ExprTransformer(txt).transform(WDL._parser.parse(txt, "expr"))
    except lark.exceptions.UnexpectedToken as exn:
        raise Err.ParserError(txt) from exn

def parse_tasks(txt : str) -> List[D.Task]:
    return _DocTransformer('').transform(WDL._parser.parse(txt, "tasks")) # pyre-fixme

def parse_document(txt : str, uri : str = '') -> D.Document:
    """
    Parse WDL document text into an abstract syntax tree. Doesn't descend into
    imported documents nor typecheck the AST.

    :param uri: filename/URI for error reporting (not otherwise used)
    """
    try:
        return _DocTransformer(uri).transform(WDL._parser.parse(txt, "document"))
    except lark.exceptions.UnexpectedCharacters as exn:
        raise Err.ParserError(uri if uri != '' else '(in buffer)') from exn
    except lark.exceptions.UnexpectedToken as exn:
        raise Err.ParserError(uri if uri != '' else '(in buffer)') from exn

def load(uri : str, path : List[str] = []) -> D.Document:
    """
    Parse a WDL document given filename/URI, recursively descend into imported documents, then typecheck the tasks and workflow.

    :param path: local filesystem directories to search for imports, in addition to the current working directory
    """
    for fn in ([uri] + [os.path.join(dn, uri) for dn in reversed(path)]):
        if os.path.exists(fn):
            with open(fn, 'r') as infile:
                # read and parse the document
                doc = parse_document(infile.read(), uri)
                # recursively descend into document's imports, and store the imported
                # documents into doc.imports
                # TODO: limit recursion; prevent mutual recursion
                for i in range(len(doc.imports)):
                    subdoc = load(doc.imports[i][0], [os.path.dirname(fn)]+path)
                    doc.imports[i] = (doc.imports[i][0], doc.imports[i][1], subdoc)
                doc.typecheck()
                return doc
    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), uri)
