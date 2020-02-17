# pylint: skip-file
import inspect
from typing import List, Optional, Set
import lark
from .Error import SourcePosition
from . import Error, Tree, Type, Expr, _grammar

# memoize Lark parsers constructed for version & start symbol
_lark_cache = {}


def parse(grammar: str, txt: str, start: str) -> lark.Tree:
    if (grammar, start) not in _lark_cache:
        _lark_cache[(grammar, start)] = lark.Lark(
            grammar, start=start, parser="lalr", propagate_positions=True
        )
    return _lark_cache[(grammar, start)].parse(txt + ("\n" if not txt.endswith("\n") else ""))


def to_int(x):
    return int(x)


def to_float(x):
    return float(x)


# Transformer from lark.Tree to WDL.Expr


class _SourcePositionTransformerMixin:
    def __init__(self, uri: str = "(buffer)", abspath: str = "(buffer)", *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.uri = uri
        self.abspath = abspath

    def _sp(self, meta):
        return SourcePosition(
            uri=self.uri,
            abspath=self.abspath,
            line=meta.line,
            column=meta.column,
            end_line=meta.end_line,
            end_column=meta.end_column,
        )


class _ExprTransformer(_SourcePositionTransformerMixin, lark.Transformer):
    # pylint: disable=no-self-use,unused-argument

    def boolean_true(self, items, meta) -> Expr.Base:
        return Expr.Boolean(self._sp(meta), True)

    def boolean_false(self, items, meta) -> Expr.Base:
        return Expr.Boolean(self._sp(meta), False)

    def int(self, items, meta) -> Expr.Base:
        assert len(items) == 1
        return Expr.Int(self._sp(meta), to_int(items[0]))

    def float(self, items, meta) -> Expr.Base:
        assert len(items) == 1
        return Expr.Float(self._sp(meta), to_float(items[0]))

    def string(self, items, meta) -> Expr.Base:
        parts = []
        for item in items:
            if isinstance(item, Expr.Base):
                parts.append(Expr.Placeholder(item.pos, {}, item))
            else:
                parts.append(item.value)
        assert len(parts) >= 2
        assert parts[0] in ['"', "'"]
        assert parts[-1] in ['"', "'"]
        return Expr.String(self._sp(meta), parts)

    def string_literal(self, items, meta):
        assert len(items) == 1
        assert items[0].value.startswith('"') or items[0].value.startswith("'")
        return str.encode(items[0].value[1:-1]).decode("unicode_escape")

    def array(self, items, meta) -> Expr.Base:
        return Expr.Array(self._sp(meta), items)

    def apply(self, items, meta) -> Expr.Base:
        assert len(items) >= 1
        assert not items[0].startswith("_")  # TODO enforce in grammar
        return Expr.Apply(self._sp(meta), items[0], items[1:])

    def negate(self, items, meta) -> Expr.Base:
        return Expr.Apply(self._sp(meta), "_negate", items)

    def at(self, items, meta) -> Expr.Base:
        return Expr.Apply(self._sp(meta), "_at", items)

    def pair(self, items, meta) -> Expr.Base:
        assert len(items) == 2
        return Expr.Pair(self._sp(meta), items[0], items[1])

    def map_kv(self, items, meta):
        assert len(items) == 2
        return (items[0], items[1])

    def map(self, items, meta) -> Expr.Base:
        return Expr.Map(self._sp(meta), items)

    def object_kv(self, items, meta):
        assert len(items) == 2
        k = items[0]
        if isinstance(k, lark.Token):
            k = k.value
        assert isinstance(k, str), k
        assert isinstance(items[1], Expr.Base)
        return (k, items[1])

    def obj(self, items, meta) -> Expr.Base:
        return Expr.Struct(self._sp(meta), items)

    def ifthenelse(self, items, meta) -> Expr.Base:
        assert len(items) == 3
        return Expr.IfThenElse(self._sp(meta), *items)

    def left_name(self, items, meta) -> Expr.Base:
        assert len(items) == 1 and isinstance(items[0], str)
        return Expr.Get(self._sp(meta), Expr._LeftName(self._sp(meta), items[0]), None)

    def get_name(self, items, meta) -> Expr.Base:
        assert len(items) == 2 and isinstance(items[0], Expr.Base) and isinstance(items[1], str)
        return Expr.Get(self._sp(meta), items[0], items[1])


# _ExprTransformer infix operators
for op in [
    "land",
    "lor",
    "add",
    "sub",
    "mul",
    "div",
    "rem",
    "eqeq",
    "neq",
    "lt",
    "lte",
    "gt",
    "gte",
]:

    def fn(self, items, meta, op=op):
        assert len(items) == 2
        return Expr.Apply(self._sp(meta), "_" + op, items)

    setattr(_ExprTransformer, op, lark.v_args(meta=True)(classmethod(fn)))  # pyre-fixme


class _TypeTransformer(_SourcePositionTransformerMixin, lark.Transformer):
    # pylint: disable=no-self-use,unused-argument

    def optional(self, items, meta):
        return set(["optional"])

    def nonempty(self, items, meta):
        return set(["nonempty"])

    def optional_nonempty(self, items, meta):
        return set(["optional", "nonempty"])

    def type(self, items, meta):
        quantifiers = set()
        if len(items) > 1 and isinstance(items[-1], set):
            quantifiers = items.pop()
        param = items[1] if len(items) > 1 else None
        param2 = items[2] if len(items) > 2 else None

        if items[0].value == "Array":
            if not param or param2:
                raise Error.InvalidType(self._sp(meta), "Array must have one type parameter")
            if quantifiers - set(["optional", "nonempty"]):
                raise Error.ValidationError(self._sp(meta), "invalid type quantifier(s) for Array")
            ans = Type.Array(param, "optional" in quantifiers, "nonempty" in quantifiers)
            ans.pos = self._sp(meta)
            return ans
        if "nonempty" in quantifiers:
            raise Error.InvalidType(
                self._sp(meta), "invalid type quantifier(s) for " + items[0].value
            )

        atomic_types = {
            "Int": Type.Int,
            "Float": Type.Float,
            "Boolean": Type.Boolean,
            "String": Type.String,
            "File": Type.File,
        }
        if items[0].value in atomic_types:
            if param or param2:
                raise Error.InvalidType(
                    self._sp(meta), items[0] + " type doesn't accept parameters"
                )
            ans = atomic_types[items[0].value]("optional" in quantifiers)
            ans.pos = self._sp(meta)
            return ans

        if items[0].value == "Map":
            if not (param and param2):
                raise Error.InvalidType(self._sp(meta), "Map must have two type parameters")
            ans = Type.Map((param, param2), "optional" in quantifiers)
            ans.pos = self._sp(meta)
            return ans

        if items[0].value == "Pair":
            if not (param and param2):
                raise Error.InvalidType(self._sp(meta), "Pair must have two type parameters")
            ans = Type.Pair(param, param2, "optional" in quantifiers)
            ans.pos = self._sp(meta)
            return ans

        if param or param2:
            raise Error.InvalidType(self._sp(meta), "Unexpected type parameter(s)")

        ans = Type.StructInstance(items[0].value, "optional" in quantifiers)
        ans.pos = self._sp(meta)
        return ans


class _DocTransformer(_ExprTransformer, _TypeTransformer):
    # pylint: disable=no-self-use,unused-argument

    _keywords: Set[str]
    _source_text: str

    def __init__(self, source_text: str, keywords: Set[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._source_text = source_text
        self._keywords = keywords

    def _check_keyword(self, pos, name):
        if name in self._keywords:
            raise Error.SyntaxError(pos, "unexpected keyword {}".format(name))

    def decl(self, items, meta):
        self._check_keyword(self._sp(meta), items[1].value)
        return Tree.Decl(
            self._sp(meta), items[0], items[1].value, (items[2] if len(items) > 2 else None)
        )

    def input_decls(self, items, meta):
        return {"inputs": items}

    def noninput_decl(self, items, meta):
        return {"noninput_decl": items[0]}

    def placeholder_option(self, items, meta):
        assert len(items) == 2
        return (items[0].value, items[1])

    def placeholder(self, items, meta):
        options = dict(items[:-1])
        if len(options.items()) < len(items) - 1:
            raise Error.MultipleDefinitions(
                self._sp(meta), "duplicate options in expression placeholder"
            )
        return Expr.Placeholder(self._sp(meta), options, items[-1])

    def command(self, items, meta):
        parts = []
        for item in items:
            if isinstance(item, Expr.Placeholder):
                parts.append(item)
            else:
                parts.append(item.value)
        return {"command": Expr.String(self._sp(meta), parts, command=True)}

    def output_decls(self, items, meta):
        return {"outputs": items}

    def meta_kv(self, items, meta):
        return (items[0].value, items[1])

    def meta_object(self, items, meta):
        d = dict()
        for k, v in items:
            if k in d:
                raise Error.MultipleDefinitions(self._sp(meta), "duplicate keys in meta object")
            d[k] = v
        return d

    def meta_array(self, items, meta):
        return items

    def meta_section(self, items, meta):
        kind = items[0].value
        assert kind in ["meta", "parameter_meta"]
        d = dict()
        d[kind] = items[1]
        return d

    def runtime_kv(self, items, meta):
        return (items[0].value, items[1])

    def runtime_section(self, items, meta):
        d = dict()
        for k, v in items:
            # TODO: restore duplicate check, cf. https://github.com/gatk-workflows/five-dollar-genome-analysis-pipeline/blob/89f11befc13abae97ab8fb1b457731f390c8728d/tasks_pipelines/qc.wdl#L288
            # if k in d:
            #    raise Error.MultipleDefinitions(self._sp(meta), "duplicate keys in runtime section")
            d[k] = v
        return {"runtime": d}

    def task(self, items, meta):
        d = {"noninput_decls": []}
        for item in items:
            if isinstance(item, dict):
                for k, v in item.items():
                    if k == "noninput_decl":
                        d["noninput_decls"].append(v)
                    elif k in d:
                        raise Error.MultipleDefinitions(
                            self._sp(meta), "redundant sections in task"
                        )
                    else:
                        d[k] = v
            else:
                assert isinstance(item, str)
                assert "name" not in d
                d["name"] = item.value
        self._check_keyword(self._sp(meta), d["name"])
        return Tree.Task(
            self._sp(meta),
            d["name"],
            d.get("inputs", None),
            d["noninput_decls"],
            d["command"],
            d.get("outputs", []),
            d.get("parameter_meta", {}),
            d.get("runtime", {}),
            d.get("meta", {}),
        )

    def tasks(self, items, meta):
        return items

    def namespaced_ident(self, items, meta) -> Expr.Base:
        assert items
        return [item.value for item in items]

    def call_input(self, items, meta):
        return (items[0].value, items[1])

    def call_inputs(self, items, meta):
        d = dict()
        for k, v in items:
            if k in d:
                raise Error.MultipleDefinitions(self._sp(meta), "duplicate keys in call inputs")
            d[k] = v
        return d

    def call(self, items, meta):
        return Tree.Call(self._sp(meta), items[0], None, items[1] if len(items) > 1 else dict())

    def call_as(self, items, meta):
        self._check_keyword(self._sp(meta), items[1].value)
        return Tree.Call(
            self._sp(meta), items[0], items[1].value, items[2] if len(items) > 2 else dict()
        )

    def scatter(self, items, meta):
        self._check_keyword(self._sp(meta), items[0].value)
        return Tree.Scatter(self._sp(meta), items[0].value, items[1], items[2:])

    def conditional(self, items, meta):
        return Tree.Conditional(self._sp(meta), items[0], items[1:])

    def workflow_wildcard_output(self, items, meta):
        return items[0] + ["*"]
        # return Expr.Ident(items[0].pos, items[0].namespace + [items[0].name, "*"])

    def workflow_output_decls(self, items, meta):
        decls = [elt for elt in items if isinstance(elt, Tree.Decl)]
        idents = [elt for elt in items if isinstance(elt, list)]
        assert len(decls) + len(idents) == len(items)
        return {"outputs": decls, "output_idents": idents, "pos": self._sp(meta)}

    def workflow(self, items, meta):
        elements = []
        inputs = None
        outputs = None
        output_idents = None
        output_idents_pos = None
        parameter_meta = None
        meta_section = None
        for item in items[1:]:
            if isinstance(item, dict):
                if "inputs" in item:
                    if inputs is not None:
                        raise Error.MultipleDefinitions(
                            self._sp(meta), "redundant workflow input sections"
                        )
                    inputs = item["inputs"]
                elif "outputs" in item:
                    if outputs is not None:
                        raise Error.MultipleDefinitions(
                            self._sp(meta), "redundant workflow output sections"
                        )
                    outputs = item["outputs"]
                    if "output_idents" in item:
                        assert output_idents is None
                        output_idents = item["output_idents"]
                        output_idents_pos = item["pos"]
                elif "meta" in item:
                    if meta_section is not None:
                        raise Error.MultipleDefinitions(
                            self._sp(meta), "redundant workflow meta sections"
                        )
                    meta_section = item["meta"]
                elif "parameter_meta" in item:
                    if parameter_meta is not None:
                        raise Error.MultipleDefinitions(
                            self._sp(meta), "redundant workflow parameter_meta sections"
                        )
                    parameter_meta = item["parameter_meta"]
                else:
                    assert False
            elif isinstance(item, (Tree.Call, Tree.Conditional, Tree.Decl, Tree.Scatter)):
                elements.append(item)
            else:
                assert False
        self._check_keyword(self._sp(meta), items[0].value)
        return Tree.Workflow(
            self._sp(meta),
            items[0].value,
            inputs,
            elements,
            outputs,
            parameter_meta or dict(),
            meta_section or dict(),
            output_idents,
            output_idents_pos,
        )

    def struct(self, items, meta):
        assert len(items) >= 1
        name = items[0]
        self._check_keyword(self._sp(meta), name)
        members = {}
        for d in items[1:]:
            assert not d.expr
            if d.name in members:
                raise Error.MultipleDefinitions(self._sp(meta), "duplicate members in struct")
            members[d.name] = d.type
        return Tree.StructTypeDef(self._sp(meta), name, members)

    def import_alias(self, items, meta):
        assert len(items) == 2
        self._check_keyword(self._sp(meta), items[1].value)
        return (items[0].value, items[1].value)

    def import_doc(self, items, meta):
        uri = items[0]
        if len(items) > 1 and isinstance(items[1], str):
            namespace = items[1].value
        else:
            namespace = uri
            try:
                namespace = namespace[namespace.rindex("/") + 1 :]
            except ValueError:
                pass
            if namespace.endswith(".wdl"):
                namespace = namespace[:-4]
        self._check_keyword(self._sp(meta), namespace)
        aliases = [p for p in items[1:] if isinstance(p, tuple)]
        return Tree.DocImport(
            pos=self._sp(meta), uri=uri, namespace=namespace, aliases=aliases, doc=None
        )

    def document(self, items, meta):
        imports = []
        structs = {}
        tasks = []
        workflow = None
        for item in items:
            if isinstance(item, Tree.Task):
                tasks.append(item)
            elif isinstance(item, Tree.Workflow):
                if workflow is not None:
                    raise Error.MultipleDefinitions(
                        self._sp(meta), "Document has multiple workflows"
                    )
                workflow = item
            elif isinstance(item, Tree.StructTypeDef):
                if item.name in structs:
                    raise Error.MultipleDefinitions(
                        self._sp(meta), "multiple structs named " + item.name
                    )
                structs[item.name] = item
            elif isinstance(item, lark.Tree) and item.data == "version":
                pass
            elif isinstance(item, Tree.DocImport):
                imports.append(item)
            else:
                assert False
        return Tree.Document(self._source_text, self._sp(meta), imports, structs, tasks, workflow)


# have lark pass the 'meta' with line/column numbers to each transformer method
for _klass in [_ExprTransformer, _TypeTransformer, _DocTransformer]:
    for name, method in inspect.getmembers(_klass, inspect.isfunction):
        if not name.startswith("_"):
            setattr(_klass, name, lark.v_args(meta=True)(method))  # pyre-fixme


def parse_expr(txt: str, version: Optional[str] = None) -> Expr.Base:
    try:
        return _ExprTransformer().transform(parse(_grammar.get(version)[0], txt, "expr"))
    except lark.exceptions.UnexpectedInput as exn:
        pos = SourcePosition(
            uri="(buffer)",
            abspath="(buffer)",
            line=getattr(exn, "line", "?"),
            column=getattr(exn, "column", "?"),
            end_line=getattr(exn, "line", "?"),
            end_column=getattr(exn, "column", "?"),
        )
        raise Error.SyntaxError(pos, str(exn)) from None
    except lark.exceptions.VisitError as exn:
        raise exn.__context__


def parse_tasks(txt: str, version: Optional[str] = None) -> List[Tree.Task]:
    try:
        (grammar, keywords) = _grammar.get(version)
        return _DocTransformer(source_text=txt, keywords=keywords).transform(
            parse(grammar, txt, "tasks")
        )
    except lark.exceptions.VisitError as exn:
        raise exn.__context__


def parse_document(
    txt: str, version: Optional[str] = None, uri: str = "", abspath: str = ""
) -> Tree.Document:
    npos = SourcePosition(uri=uri, abspath=abspath, line=0, column=0, end_line=0, end_column=0)
    if not txt.strip():
        return Tree.Document(txt, npos, [], {}, [], None,)
    if version is None:
        # for now assume the version is 1.0 if the first line is "version <number>"
        # otherwise draft-2
        version = "draft-2"
        for line in txt.split("\n"):
            line = line.strip()
            if line and line[0] != "#":
                if line.startswith("version "):
                    version = line[8:]
                break
    try:
        (grammar, keywords) = _grammar.get(version)
    except KeyError:
        raise Error.SyntaxError(npos, "unknown WDL version " + version) from None
    try:
        return _DocTransformer(
            source_text=txt, uri=uri, abspath=abspath, keywords=keywords
        ).transform(parse(grammar, txt, "document"))
    except lark.exceptions.UnexpectedInput as exn:
        pos = SourcePosition(
            uri=(uri if uri else "(buffer)"),
            abspath=(abspath if abspath else "(buffer)"),
            line=getattr(exn, "line", "?"),
            column=getattr(exn, "column", "?"),
            end_line=getattr(exn, "line", "?"),
            end_column=getattr(exn, "column", "?"),
        )
        raise Error.SyntaxError(pos, str(exn)) from None
    except lark.exceptions.VisitError as exn:
        raise exn.__context__
