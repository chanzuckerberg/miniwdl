# pylint: skip-file
import inspect
from typing import List, Optional
import lark
from WDL.Error import SourcePosition
from WDL import Error as Err
from WDL import Tree as D
from WDL import Type as T
from WDL import Expr as E

common_grammar = r"""
// WDL expressions
// start with rules handling infix operator precedence
?expr: expr_infix

?expr_infix: expr_infix0

?expr_infix0: expr_infix0 "||" expr_infix1 -> lor
            | expr_infix1

?expr_infix1: expr_infix1 "&&" expr_infix2 -> land
            | expr_infix2

?expr_infix2: expr_infix2 "==" expr_infix3 -> eqeq
            | expr_infix2 "!=" expr_infix3 -> neq
            | expr_infix2 "<=" expr_infix3 -> lte
            | expr_infix2 ">=" expr_infix3 -> gte
            | expr_infix2 "<" expr_infix3 -> lt
            | expr_infix2 ">" expr_infix3 -> gt
            | expr_infix3

?expr_infix3: expr_infix3 "+" expr_infix4 -> add
            | expr_infix3 "-" expr_infix4 -> sub
            | expr_infix4

?expr_infix4: expr_infix4 "*" expr_infix5 -> mul
            | expr_infix4 "/" expr_infix5 -> div
            | expr_infix4 "%" expr_infix5 -> rem
            | expr_infix5

?expr_infix5: expr_core

// expression core (everything but infix)
?expr_core: "(" expr ")"
          | literal
          | string
          | "!" expr -> negate

          | "[" [expr ("," expr)*] ","? "]" -> array
          | expr_core "[" expr "]" -> get

          | "(" expr "," expr ")" -> pair
          | expr_core "." "left" -> get_left
          | expr_core "." "right" -> get_right

          | "{" [map_kv ("," map_kv)*] "}" -> map

          | "if" expr "then" expr "else" expr -> ifthenelse

          | ident
          | CNAME "(" [expr ("," expr)*] ")" -> apply

?literal: "true"-> boolean_true
        | "false" -> boolean_false
        | INT -> int
        | SIGNED_INT -> int
        | FLOAT -> float
        | SIGNED_FLOAT -> float

?string: string1 | string2

STRING_INNER1: ("\\\'"|/[^']/)
ESCAPED_STRING1: "'" STRING_INNER1* "'"
string_literal: ESCAPED_STRING | ESCAPED_STRING1

ident: CNAME ("." CNAME)*

?map_key: literal | string
map_kv: map_key ":" expr

// WDL types and declarations
type: _INT QUANT? -> int_type
    | _FLOAT QUANT? -> float_type
    | _BOOLEAN QUANT? -> boolean_type
    | _STRING QUANT? -> string_type
    | _FILE QUANT? -> file_type
    | _ARRAY "[" type "]" ARRAY_QUANT? -> array_type
    | _MAP "[" type "," type "]" QUANT? -> map_type
    | _PAIR "[" type "," type "]" QUANT? -> pair_type
_INT.2: "Int"           // .2 ensures higher priority than CNAME
_FLOAT.2: "Float"
_BOOLEAN.2: "Boolean"
_STRING.2: "String"
_FILE.2: "File"
_ARRAY.2: "Array"
_MAP.2: "Map"
_PAIR.2: "Pair"
QUANT: "?"
ARRAY_QUANT: "+?" | "+" | "?"

unbound_decl: type CNAME -> decl
bound_decl: type CNAME "=" expr -> decl
?any_decl: unbound_decl | bound_decl

// WDL task commands: with {} and <<< >>> command and ${} and ~{} placeholder styles
!?placeholder_key: "default" | "false" | "true" | "sep"
?placeholder_value: string_literal
                  | INT -> int
                  | FLOAT -> float
placeholder_option: placeholder_key "=" placeholder_value
placeholder: placeholder_option* expr

?command: command1 | command2

// task meta/parameter_meta sections (effectively JSON)
meta_object: "{" [meta_kv (","? meta_kv)*] "}"
meta_kv: CNAME ":" meta_value
?meta_value: literal | string
           | meta_object
           | "[" [meta_value ("," meta_value)*] "]" -> meta_array
META_KIND.2: "meta" | "parameter_meta" | "runtime" // .2 ensures higher priority than CNAME
meta_section: META_KIND meta_object

// task runtime section (key-expression pairs)
runtime_section: "runtime" "{" [runtime_kv (","? runtime_kv)*] "}"
runtime_kv: CNAME ":" expr

// WDL tasks
input_decls: "input" "{" any_decl* "}"
?task_sections1: input_decls
               | meta_section
               | runtime_section
               | any_decl+ -> noninput_decls
output_decls: "output" "{" bound_decl* "}"
?task_sections2: output_decls
               | meta_section
               | runtime_section
task: "task" CNAME "{" task_sections1* command task_sections2* "}"

tasks: task*

// WDL workflows
call_input: CNAME "=" expr
call_inputs: "input" ":" [call_input ("," call_input)*] ","?
?call_body: "{" call_inputs? "}"
call: "call" ident call_body? -> call
    | "call" ident "as" CNAME call_body? -> call_as

?inner_workflow_element: bound_decl | call | scatter | conditional
scatter: "scatter" "(" CNAME "in" expr ")" "{" inner_workflow_element* "}"
conditional: "if" "(" expr ")" "{" inner_workflow_element* "}"

?workflow_element: any_decl | call | scatter | conditional | meta_section
workflow: "workflow" CNAME "{" input_decls? workflow_element* workflow_outputs? meta_section?"}"

// WDL document: version, imports, tasks and (at most one) workflow
version: "version" /[^ \t\r\n]+/
import_doc: "import" string_literal ["as" CNAME]
?document_element: import_doc | task | workflow
document: version? document_element*
        | version? document_element*

COMMENT: "#" /[^\r\n]*/ NEWLINE

%import common.INT
%import common.SIGNED_INT
%import common.FLOAT
%import common.SIGNED_FLOAT
%import common.CNAME
%import common.ESCAPED_STRING
%import common.WS
%import common.NEWLINE
%ignore WS
%ignore COMMENT
"""

# pre-1.0 specific productions:
# - interpolated strings and { } and <<< >>> command styles all have placeholders delimited by ${ }
# - workflow outputs can be bare identifiers rather than complete decls
productions_pre_1_0 = r"""
// string (single-quoted)
STRING1_CHAR: "\\'" | /[^'$]/ | /\$[^{$']/
STRING1_FRAGMENT: STRING1_CHAR+
string1: /'/ (STRING1_FRAGMENT? /\$/* "${" expr "}")* STRING1_FRAGMENT? /\$/* /'/ -> string

// string (double-quoted)
STRING2_CHAR: "\\\"" | /[^"$]/ | /\$[^{$"]/
STRING2_FRAGMENT: STRING2_CHAR+
string2: /"/ (STRING2_FRAGMENT? /\$/* "${" expr "}")* STRING2_FRAGMENT? /\$/* /"/ -> string

COMMAND1_CHAR: /[^$}]/ | /\$[^{$]/
COMMAND1_FRAGMENT: COMMAND1_CHAR+
command1: "command" "{" (COMMAND1_FRAGMENT? /\$/* "${" placeholder "}")* COMMAND1_FRAGMENT? /\$/* "}" -> command

COMMAND2_CHAR: /[^$>]/ | /\$[^{$]/ | />[^>]/ | />>[^>]/
COMMAND2_FRAGMENT: COMMAND2_CHAR+
command2: "command" "<<<" (COMMAND2_FRAGMENT? /\$/* "${" placeholder "}")* COMMAND2_FRAGMENT? /\$/* ">>>" -> command

?workflow_outputs: "output" "{" workflow_output_decls "}"
workflow_output_decls: workflow_output_decl*
?workflow_output_decl: bound_decl | ident | workflow_wildcard_output
workflow_wildcard_output: ident "." "*" | ident ".*"
"""

# 1.0+ productions:
# - within interpolated strings and { } task commands, placeholders may be delimited by ${ } or ~{ }
# - within <<< >>> commands, placeholders are delimited by ~{ } only
# - workflow outputs are complete decls
productions_1_0 = r"""
_EITHER_DELIM.2: "~{" | "${"

// string (single-quoted)
STRING1_CHAR: "\\'" | /[^'~$]/ | /\$[^{$~']/ | /\~[^{$~']/
STRING1_FRAGMENT: STRING1_CHAR+
string1: /'/ (STRING1_FRAGMENT? /\$/* /\~/* _EITHER_DELIM expr "}")* STRING1_FRAGMENT? /\$/* /\~/* /'/ -> string

// string (double-quoted)
STRING2_CHAR: "\\\"" | /[^"~$]/ | /\$[^{$~"]/ | /~[^{$~"]/
STRING2_FRAGMENT: STRING2_CHAR+
string2: /"/ (STRING2_FRAGMENT? /\$/* /\~/* _EITHER_DELIM expr "}")* STRING2_FRAGMENT? /\$/* /\~/* /"/ -> string

COMMAND1_CHAR: /[^~$}]/ | /\$[^{$~]/ | /~[^{$~]/
COMMAND1_FRAGMENT: COMMAND1_CHAR+
command1: "command" "{" (COMMAND1_FRAGMENT? /\$/* /\~/* _EITHER_DELIM placeholder "}")* COMMAND1_FRAGMENT? /\$/* /\~/* "}" -> command

COMMAND2_CHAR: /[^~>]/ | /~[^{~]/ | />[^>]/ | />>[^>]/
COMMAND2_FRAGMENT: COMMAND2_CHAR+
command2: "command" "<<<" (COMMAND2_FRAGMENT? /\~/? "~{" placeholder "}")* COMMAND2_FRAGMENT? /\~/* ">>>" -> command

?workflow_outputs: output_decls
"""


def _grammar_for_version(version: Optional[str]) -> str:
    if version == "draft-2":
        return common_grammar + productions_pre_1_0
    return common_grammar + productions_1_0


# memoize Lark parsers constructed for version & start symbol
_lark_cache = {}


def parse(txt: str, start: str, version: Optional[str] = None) -> lark.Tree:
    if (version, start) not in _lark_cache:
        _lark_cache[(version, start)] = lark.Lark(
            _grammar_for_version(version), start=start, parser="lalr", propagate_positions=True
        )
    return _lark_cache[(version, start)].parse(txt)


def sp(filename, meta) -> SourcePosition:
    return SourcePosition(
        filename=filename,
        line=meta.line,
        column=meta.column,
        end_line=meta.end_line,
        end_column=meta.end_column,
    )


def to_int(x):
    return int(x)


def to_float(x):
    return float(x)


# Transformer from lark.Tree to WDL.Expr


class _ExprTransformer(lark.Transformer):
    # pylint: disable=no-self-use,unused-argument

    def __init__(self, file: str) -> None:
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
            else:
                parts.append(item.value)
        assert len(parts) >= 2
        assert parts[0] in ['"', "'"]
        assert parts[-1] in ['"', "'"]
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
        return E.Apply(sp(self.filename, meta), "_" + op, items)

    setattr(_ExprTransformer, op, lark.v_args(meta=True)(classmethod(fn)))  # pyre-fixme


class _TypeTransformer(lark.Transformer):
    # pylint: disable=no-self-use,unused-argument

    def __init__(self, file: str) -> None:
        self.filename = file

    def int_type(self, items, meta):
        optional = False
        if items and items[0].value == "?":
            optional = True
        return T.Int(optional)

    def float_type(self, items, meta):
        optional = False
        if items and items[0].value == "?":
            optional = True
        return T.Float(optional)

    def boolean_type(self, items, meta):
        optional = False
        if items and items[0].value == "?":
            optional = True
        return T.Boolean(optional)

    def string_type(self, items, meta):
        optional = False
        if items and items[0].value == "?":
            optional = True
        return T.String(optional)

    def file_type(self, items, meta):
        optional = False
        if items and items[0].value == "?":
            optional = True
        return T.File(optional)

    def array_type(self, items, meta):
        assert len(items) >= 1
        assert isinstance(items[0], T.Base)
        optional = False
        nonempty = False
        for c in "".join(items[1:]):
            if c == "?":
                optional = True
            if c == "+":
                nonempty = True
        return T.Array(items[0], optional, nonempty)

    def map_type(self, items, meta):
        assert len(items) >= 2
        assert isinstance(items[0], T.Base)
        assert isinstance(items[1], T.Base)
        optional = False
        if len(items) > 2:
            if items[2].value == "?":
                optional = True
        return T.Map((items[0], items[1]), optional)

    def pair_type(self, items, meta):
        assert len(items) >= 2
        assert isinstance(items[0], T.Base)
        assert isinstance(items[1], T.Base)
        optional = False
        if len(items) > 2:
            if items[2].value == "?":
                optional = True
        return T.Pair(items[0], items[1], optional)


class _DocTransformer(_ExprTransformer, _TypeTransformer):
    # pylint: disable=no-self-use,unused-argument

    def __init__(self, file: str) -> None:
        # pylint: disable=super-init-not-called
        self.filename = file

    def decl(self, items, meta):
        return D.Decl(
            sp(self.filename, meta),
            items[0],
            items[1].value,
            (items[2] if len(items) > 2 else None),
        )

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
            raise Err.MultipleDefinitions(
                sp(self.filename, meta), "duplicate options in expression placeholder"
            )
        return E.Placeholder(sp(self.filename, meta), options, items[-1])

    def command(self, items, meta):
        parts = []
        for item in items:
            if isinstance(item, E.Placeholder):
                parts.append(item)
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
                raise Err.MultipleDefinitions(
                    sp(self.filename, meta), "duplicate keys in meta object"
                )
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
        for k, v in items:
            # TODO: restore duplicate check, cf. https://github.com/gatk-workflows/five-dollar-genome-analysis-pipeline/blob/89f11befc13abae97ab8fb1b457731f390c8728d/tasks_pipelines/qc.wdl#L288
            # if k in d:
            #    raise Err.MultipleDefinitions(sp(self.filename, meta), "duplicate keys in runtime section")
            d[k] = v
        return {"runtime": d}

    def task(self, items, meta):
        d = {}
        for item in items:
            if isinstance(item, dict):
                for k, v in item.items():
                    if k in d:
                        raise Err.MultipleDefinitions(
                            sp(self.filename, meta), "redundant sections in task"
                        )
                    d[k] = v
            else:
                assert isinstance(item, str)
                assert "name" not in d
                d["name"] = item.value
        return D.Task(
            sp(self.filename, meta),
            d["name"],
            d.get("inputs", None),
            d.get("decls", []),
            d["command"],
            d.get("outputs", []),
            d.get("parameter_meta", {}),
            d.get("runtime", {}),
            d.get("meta", {}),
        )

    def tasks(self, items, meta):
        return items

    def call_input(self, items, meta):
        return (items[0].value, items[1])

    def call_inputs(self, items, meta):
        d = dict()
        for k, v in items:
            if k in d:
                raise Err.MultipleDefinitions(
                    sp(self.filename, meta), "duplicate keys in call inputs"
                )
            d[k] = v
        return d

    def call(self, items, meta):
        return D.Call(
            sp(self.filename, meta), items[0], None, items[1] if len(items) > 1 else dict()
        )

    def call_as(self, items, meta):
        return D.Call(
            sp(self.filename, meta),
            items[0],
            items[1].value,
            items[2] if len(items) > 2 else dict(),
        )

    def scatter(self, items, meta):
        return D.Scatter(sp(self.filename, meta), items[0].value, items[1], items[2:])

    def conditional(self, items, meta):
        return D.Conditional(sp(self.filename, meta), items[0], items[1:])

    def workflow_wildcard_output(self, items, meta):
        assert isinstance(items[0], E.Ident)
        return E.Ident(items[0].pos, items[0].namespace + [items[0].name, "*"])

    def workflow_output_decls(self, items, meta):
        decls = [elt for elt in items if isinstance(elt, D.Decl)]
        idents = [elt for elt in items if isinstance(elt, E.Ident)]
        assert len(decls) + len(idents) == len(items)
        return {"outputs": decls, "output_idents": idents}

    def workflow(self, items, meta):
        elements = []
        inputs = None
        outputs = None
        output_idents = None
        parameter_meta = None
        meta_section = None
        for item in items[1:]:
            if isinstance(item, dict):
                if "inputs" in item:
                    assert inputs is None
                    inputs = item["inputs"]
                elif "outputs" in item:
                    if outputs is not None:
                        raise Err.MultipleDefinitions(
                            sp(self.filename, meta), "redundant sections in workflow"
                        )
                    outputs = item["outputs"]
                    if "output_idents" in item:
                        assert output_idents is None
                        output_idents = item["output_idents"]
                elif "meta" in item:
                    if meta_section is not None:
                        raise Err.MultipleDefinitions(
                            sp(self.filename, meta), "redundant sections in workflow"
                        )
                    meta_section = item["meta"]
                elif "parameter_meta" in item:
                    if parameter_meta is not None:
                        raise Err.MultipleDefinitions(
                            sp(self.filename, meta), "redundant sections in workflow"
                        )
                    parameter_meta = item["parameter_meta"]
                else:
                    assert False
            elif isinstance(item, (D.Call, D.Conditional, D.Decl, D.Scatter)):
                elements.append(item)
            else:
                assert False
        return D.Workflow(
            sp(self.filename, meta),
            items[0].value,
            inputs,
            elements,
            outputs,
            parameter_meta or dict(),
            meta_section or dict(),
            output_idents,
        )

    def import_doc(self, items, meta):
        uri = items[0]
        if len(items) > 1:
            namespace = items[1].value
        else:
            namespace = uri
            try:
                namespace = namespace[namespace.rindex("/") + 1 :]
            except ValueError:
                pass
            if namespace.endswith(".wdl"):
                namespace = namespace[:-4]
        # TODO: validate namespace
        return {"import": (uri, namespace)}

    def document(self, items, meta):
        imports = []
        tasks = []
        workflow = None
        for item in items:
            if isinstance(item, D.Task):
                tasks.append(item)
            elif isinstance(item, D.Workflow):
                if workflow is not None:
                    raise Err.MultipleDefinitions(
                        sp(self.filename, meta), "Document has multiple workflows"
                    )
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
        if not name.startswith("_"):
            setattr(_klass, name, lark.v_args(meta=True)(method))  # pyre-fixme


def parse_expr(txt: str, version: Optional[str] = None) -> E.Base:
    try:
        return _ExprTransformer(txt).transform(parse(txt, "expr", version))
    except lark.exceptions.UnexpectedInput as exn:
        raise Err.SyntaxError("(buffer)", str(exn)) from None
    except lark.exceptions.VisitError as exn:
        raise exn.__context__


def parse_tasks(txt: str, version: Optional[str] = None) -> List[D.Task]:
    # pyre-fixme
    try:
        return _DocTransformer("").transform(parse(txt, "tasks", version))
    except lark.exceptions.VisitError as exn:
        raise exn.__context__


def parse_document(txt: str, version: Optional[str] = None, uri: str = "") -> D.Document:
    if version is None:
        # for now assume the version is 1.0 if the first line is "version <number>"
        # otherwise draft-2
        version = "draft-2"
        for line in txt.split("\n"):
            line = line.strip()
            if line and line[0] != "#":
                if line.startswith("version ") and line[8].isdigit():
                    version = "1.0"
                break
    if not txt.strip():
        return D.Document(
            SourcePosition(filename=uri, line=0, column=0, end_line=0, end_column=0), [], [], None
        )
    try:
        return _DocTransformer(uri).transform(parse(txt, "document", version))
    except lark.exceptions.UnexpectedInput as exn:
        raise Err.SyntaxError(uri if uri != "" else "(buffer)", str(exn)) from None
    except lark.exceptions.VisitError as exn:
        raise exn.__context__
