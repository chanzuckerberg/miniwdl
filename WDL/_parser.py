# pylint: skip-file
import inspect
from typing import List, Optional
import lark
from .Error import SourcePosition
from . import Error, Tree, Type, Expr

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
?task_section: input_decls
             | output_decls
             | meta_section
             | runtime_section
             | any_decl -> noninput_decl
task: "task" CNAME "{" task_section* command task_section* "}"

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

?workflow_element: input_decls | any_decl | call | scatter | conditional | workflow_outputs | meta_section
workflow: "workflow" CNAME "{" workflow_element* "}"

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
          | "{" [map_kv ("," map_kv)*] ","? "}" -> map

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
          | "object" "{" [object_kv ("," object_kv)* ","?] "}" -> obj // appends to expr_core

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
    return _lark_cache[(version, start)].parse(txt + ("\n" if not txt.endswith("\n") else ""))


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


def _check_keyword(pos, name):
    if name in _keywords:
        raise Error.SyntaxError(pos, "unexpected keyword {}".format(name))


class _DocTransformer(_ExprTransformer, _TypeTransformer):
    # pylint: disable=no-self-use,unused-argument

    def decl(self, items, meta):
        _check_keyword(self._sp(meta), items[1].value)
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
        _check_keyword(self._sp(meta), d["name"])
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
        _check_keyword(self._sp(meta), items[1].value)
        return Tree.Call(
            self._sp(meta), items[0], items[1].value, items[2] if len(items) > 2 else dict()
        )

    def scatter(self, items, meta):
        _check_keyword(self._sp(meta), items[0].value)
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
        _check_keyword(self._sp(meta), items[0].value)
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
        _check_keyword(self._sp(meta), name)
        members = {}
        for d in items[1:]:
            assert not d.expr
            if d.name in members:
                raise Error.MultipleDefinitions(self._sp(meta), "duplicate members in struct")
            members[d.name] = d.type
        return Tree.StructTypeDef(self._sp(meta), name, members)

    def import_alias(self, items, meta):
        assert len(items) == 2
        _check_keyword(self._sp(meta), items[1].value)
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
        _check_keyword(self._sp(meta), namespace)
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
        return Tree.Document(self._sp(meta), imports, structs, tasks, workflow)


# have lark pass the 'meta' with line/column numbers to each transformer method
for _klass in [_ExprTransformer, _TypeTransformer, _DocTransformer]:
    for name, method in inspect.getmembers(_klass, inspect.isfunction):
        if not name.startswith("_"):
            setattr(_klass, name, lark.v_args(meta=True)(method))  # pyre-fixme


def parse_expr(txt: str, version: Optional[str] = None) -> Expr.Base:
    try:
        return _ExprTransformer().transform(parse(txt, "expr", version))
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
        return _DocTransformer().transform(parse(txt, "tasks", version))
    except lark.exceptions.VisitError as exn:
        raise exn.__context__


def parse_document(
    txt: str, version: Optional[str] = None, uri: str = "", abspath: str = ""
) -> Tree.Document:
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
        return Tree.Document(
            SourcePosition(uri=uri, abspath=abspath, line=0, column=0, end_line=0, end_column=0),
            [],
            {},
            [],
            None,
        )
    try:
        return _DocTransformer(uri=uri, abspath=abspath).transform(parse(txt, "document", version))
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
