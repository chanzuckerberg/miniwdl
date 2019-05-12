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
?literal: "true"-> boolean_true
        | "false" -> boolean_false
        | INT -> int
        | SIGNED_INT -> int
        | FLOAT -> float
        | SIGNED_FLOAT -> float

?string: string1 | string2

STRING_INNER1: ("\\'"|/[^']/)
ESCAPED_STRING1: "'" STRING_INNER1* "'"
string_literal: ESCAPED_STRING | ESCAPED_STRING1

?map_key: literal | string
map_kv: map_key ":" expr

// WDL declarations
_quant: optional | nonempty | optional_nonempty
optional: "?"
nonempty: "+"
optional_nonempty: "+?"

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
?meta_value: literal | string_literal
           | meta_object
           | "[" [meta_value ("," meta_value)*] "]" -> meta_array
META_KIND.2: "meta" | "parameter_meta" | "runtime" // .2 ensures higher priority than CNAME
meta_section: META_KIND meta_object

// task runtime section (key-expression pairs)
runtime_section: "runtime" "{" [runtime_kv (","? runtime_kv)*] "}"
runtime_kv: CNAME ":" expr

// WDL tasks
input_decls: "input" "{" any_decl* "}"
output_decls: "output" "{" bound_decl* "}"
?task_sections1: input_decls
               | output_decls
               | meta_section
               | runtime_section
               | any_decl+ -> noninput_decls
?task_sections2: input_decls
               | output_decls
               | meta_section
               | runtime_section
task: "task" CNAME "{" task_sections1* command task_sections2* "}"

tasks: task*

// WDL workflows
namespaced_ident: CNAME ("." CNAME)* 
call_input: CNAME "=" expr
call_inputs: "input" ":" [call_input ("," call_input)*] ","?
?call_body: "{" call_inputs? "}"
call: "call" namespaced_ident call_body? -> call
    | "call" namespaced_ident "as" CNAME call_body? -> call_as

?inner_workflow_element: bound_decl | call | scatter | conditional
scatter: "scatter" "(" CNAME "in" expr ")" "{" inner_workflow_element* "}"
conditional: "if" "(" expr ")" "{" inner_workflow_element* "}"

?workflow_element: any_decl | call | scatter | conditional | meta_section
workflow: "workflow" CNAME "{" input_decls? workflow_element* workflow_outputs? meta_section?"}"

// WDL document: version, imports, tasks and (at most one) workflow
version: "version" /[^ \t\r\n]+/
import_alias: "alias" CNAME "as" CNAME
import_doc: "import" string_literal ["as" CNAME] import_alias*
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

// WDL expressions
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
// we stuck this last down here so that further language-version-specific
// productions can be added below
?expr_core: "(" expr ")"
          | literal
          | string
          | "!" expr -> negate

          | "[" [expr ("," expr)*] ","? "]" -> array
          | expr_core "[" expr "]" -> at

          | "(" expr "," expr ")" -> pair
          | "{" [map_kv ("," map_kv)*] "}" -> map

          | "if" expr "then" expr "else" expr -> ifthenelse

          | CNAME "(" [expr ("," expr)*] ")" -> apply

          | CNAME -> left_name
          | expr_core "." CNAME -> get_name
"""

# pre-1.0 specific productions:
# - predefined types only
# - interpolated strings and { } and <<< >>> command styles all have placeholders delimited by ${ }
# - workflow outputs can be bare identifiers rather than complete decls
productions_pre_1_0 = r"""
// WDL types
type: BUILTIN_TYPE _quant?
      | BUILTIN_TYPE "[" type ["," type] "]" _quant?
BUILTIN_TYPE.2: "Int" | "Float" | "Boolean" | "String" | "File" | "Array" | "Map" | "Pair"

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
?workflow_output_decl: bound_decl | namespaced_ident | workflow_wildcard_output
workflow_wildcard_output: namespaced_ident "." "*" | namespaced_ident ".*"

?document_element: import_doc | task | workflow
"""

# 1.0+ productions:
# - types can be any CNAME (structs)
# - within interpolated strings and { } task commands, placeholders may be delimited by ${ } or ~{ }
# - within <<< >>> commands, placeholders are delimited by ~{ } only
# - workflow outputs are complete decls
# - struct type definitions
# - struct literals (as object literals)
productions_1_0 = r"""
          | "object" "{" [object_kv ("," object_kv)*] "}" -> obj // appends to expr_core

object_kv:  CNAME ":" expr
          | string_literal ":" expr

// WDL types
type: CNAME _quant?
      | CNAME "[" type ["," type] "]" _quant?

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

// struct definitions
struct: "struct" CNAME "{" unbound_decl* "}"

?document_element: import_doc | task | workflow | struct
"""

_keywords = "Array Float Int Map None Pair String alias as call command else false if import input left meta object output parameter_meta right runtime scatter struct task then true workflow".split(
    " "
)


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

    def string_literal(self, items, meta):
        assert len(items) == 1
        assert items[0].value.startswith('"') or items[0].value.startswith("'")
        return str.encode(items[0].value[1:-1]).decode("unicode_escape")

    def array(self, items, meta) -> E.Base:
        return E.Array(sp(self.filename, meta), items)

    def apply(self, items, meta) -> E.Base:
        assert len(items) >= 1
        return E.Apply(sp(self.filename, meta), items[0], items[1:])

    def negate(self, items, meta) -> E.Base:
        return E.Apply(sp(self.filename, meta), "_negate", items)

    def at(self, items, meta) -> E.Base:
        return E.Apply(sp(self.filename, meta), "_at", items)

    def pair(self, items, meta) -> E.Base:
        assert len(items) == 2
        return E.Pair(sp(self.filename, meta), items[0], items[1])

    def map_kv(self, items, meta):
        assert len(items) == 2
        return (items[0], items[1])

    def map(self, items, meta) -> E.Base:
        return E.Map(sp(self.filename, meta), items)

    def object_kv(self, items, meta):
        assert len(items) == 2
        k = items[0]
        assert isinstance(k, str), k
        assert isinstance(items[1], E.Base)
        return (k, items[1])

    def obj(self, items, meta) -> E.Base:
        return E.Struct(sp(self.filename, meta), items)

    def ifthenelse(self, items, meta) -> E.Base:
        assert len(items) == 3
        return E.IfThenElse(sp(self.filename, meta), *items)

    def left_name(self, items, meta) -> E.Base:
        assert len(items) == 1 and isinstance(items[0], str)
        return E.Get(sp(self.filename, meta), E._LeftName(sp(self.filename, meta), items[0]), None)

    def get_name(self, items, meta) -> E.Base:
        assert len(items) == 2 and isinstance(items[0], E.Base) and isinstance(items[1], str)
        return E.Get(sp(self.filename, meta), items[0], items[1])


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
                raise Err.InvalidType(sp(self.filename, meta), "Array must have one type parameter")
            if quantifiers - set(["optional", "nonempty"]):
                raise Err.ValidationError(
                    sp(self.filename, meta), "invalid type quantifier(s) for Array"
                )
            return T.Array(param, "optional" in quantifiers, "nonempty" in quantifiers)
        if "nonempty" in quantifiers:
            raise Err.InvalidType(
                sp(self.filename, meta), "invalid type quantifier(s) for " + items[0].value
            )

        atomic_types = {
            "Int": T.Int,
            "Float": T.Float,
            "Boolean": T.Boolean,
            "String": T.String,
            "File": T.File,
        }
        if items[0].value in atomic_types:
            if param or param2:
                raise Err.InvalidType(
                    sp(self.filename, meta), items[0] + " type doesn't accept parameters"
                )
            return atomic_types[items[0].value]("optional" in quantifiers)

        if items[0].value == "Map":
            if not (param and param2):
                raise Err.InvalidType(sp(self.filename, meta), "Map must have two type parameters")
            return T.Map((param, param2), "optional" in quantifiers)

        if items[0].value == "Pair":
            if not (param and param2):
                raise Err.InvalidType(sp(self.filename, meta), "Pair must have two type parameters")
            return T.Pair(param, param2, "optional" in quantifiers)

        if param or param2:
            raise Err.InvalidType(sp(self.filename, meta), "Unexpected type parameter(s)")

        return T.StructInstance(items[0].value, "optional" in quantifiers)


def _check_keyword(pos, name):
    if name in _keywords:
        raise Err.SyntaxError(
            pos.filename, "(Ln {}, Col {}) unexpected keyword {}".format(pos.line, pos.column, name)
        )


class _DocTransformer(_ExprTransformer, _TypeTransformer):
    # pylint: disable=no-self-use,unused-argument

    def __init__(self, file: str) -> None:
        # pylint: disable=super-init-not-called
        self.filename = file

    def decl(self, items, meta):
        _check_keyword(sp(self.filename, meta), items[1].value)
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
        _check_keyword(sp(self.filename, meta), d["name"])
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

    def namespaced_ident(self, items, meta) -> E.Base:
        assert items
        return [item.value for item in items]

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
        _check_keyword(sp(self.filename, meta), items[1].value)
        return D.Call(
            sp(self.filename, meta),
            items[0],
            items[1].value,
            items[2] if len(items) > 2 else dict(),
        )

    def scatter(self, items, meta):
        _check_keyword(sp(self.filename, meta), items[0].value)
        return D.Scatter(sp(self.filename, meta), items[0].value, items[1], items[2:])

    def conditional(self, items, meta):
        return D.Conditional(sp(self.filename, meta), items[0], items[1:])

    def workflow_wildcard_output(self, items, meta):
        return items[0] + ["*"]
        # return E.Ident(items[0].pos, items[0].namespace + [items[0].name, "*"])

    def workflow_output_decls(self, items, meta):
        decls = [elt for elt in items if isinstance(elt, D.Decl)]
        idents = [elt for elt in items if isinstance(elt, list)]
        assert len(decls) + len(idents) == len(items)
        return {"outputs": decls, "output_idents": idents, "pos": sp(self.filename, meta)}

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
                        output_idents_pos = item["pos"]
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
        _check_keyword(sp(self.filename, meta), items[0].value)
        return D.Workflow(
            sp(self.filename, meta),
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
        _check_keyword(sp(self.filename, meta), name)
        members = {}
        for d in items[1:]:
            assert not d.expr
            if d.name in members:
                raise Err.MultipleDefinitions(
                    sp(self.filename, meta), "duplicate members in struct"
                )
            members[d.name] = d.type
        return D.StructTypeDef(sp(self.filename, meta), name, members)

    def import_alias(self, items, meta):
        assert len(items) == 2
        _check_keyword(sp(self.filename, meta), items[1].value)
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
        _check_keyword(sp(self.filename, meta), namespace)
        aliases = [p for p in items[1:] if isinstance(p, tuple)]
        return D.DocImport(
            pos=sp(self.filename, meta), uri=uri, namespace=namespace, aliases=aliases, doc=None
        )

    def document(self, items, meta):
        imports = []
        structs = {}
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
            elif isinstance(item, D.StructTypeDef):
                if item.name in structs:
                    raise Err.MultipleDefinitions(
                        sp(self.filename, meta), "multiple structs named " + item.name
                    )
                structs[item.name] = item
            elif isinstance(item, lark.Tree) and item.data == "version":
                pass
            elif isinstance(item, D.DocImport):
                imports.append(item)
            else:
                assert False
        return D.Document(sp(self.filename, meta), imports, structs, tasks, workflow)


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
            SourcePosition(filename=uri, line=0, column=0, end_line=0, end_column=0),
            [],
            {},
            [],
            None,
        )
    try:
        return _DocTransformer(uri).transform(parse(txt, "document", version))
    except lark.exceptions.UnexpectedInput as exn:
        raise Err.SyntaxError(uri if uri != "" else "(buffer)", str(exn)) from None
    except lark.exceptions.VisitError as exn:
        raise exn.__context__
