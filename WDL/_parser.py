import lark
import WDL.Expr

grammar = r"""
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

          | "[" [expr ("," expr)*] "]" -> array
          | expr_core "[" expr "]" -> get

          | "if" expr "then" expr "else" expr -> ifthenelse

          | ident
          | CNAME "(" [expr ("," expr)*] ")" -> apply

?literal: "true" -> boolean_true
        | "false" -> boolean_false
        | INT -> int
        | SIGNED_INT -> int
        | FLOAT -> float
        | SIGNED_FLOAT -> float

// string (single-quoted)
STRING1_CHAR: "\\'" | /[^'$]/ | /\$[^{']/
STRING1_END: STRING1_CHAR* "$"? "'"
STRING1_FRAGMENT: STRING1_CHAR* "${"
string1: /'/ [(STRING1_FRAGMENT expr "}")*] STRING1_END -> string

// string (double-quoted)
STRING2_CHAR: "\\\"" | /[^"$]/ | /\$[^{"]/
STRING2_END: STRING2_CHAR* "$"? /"/
STRING2_FRAGMENT: STRING2_CHAR* "${"
string2: /"/ [(STRING2_FRAGMENT expr "}")*] STRING2_END -> string

?string: string1 | string2

STRING_INNER1: ("\\\'"|/[^']/)
ESCAPED_STRING1: "'" STRING_INNER1* "'"
string_literal: ESCAPED_STRING | ESCAPED_STRING1

ident: [CNAME ("." CNAME)*]

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
ARRAY_QUANT: "?" | "+"

unbound_decl: type CNAME -> decl
bound_decl: type CNAME "=" expr -> decl
?any_decl: unbound_decl | bound_decl

// WDL task commands: with {} and <<< >>> command and ${} and ~{} placeholder styles
!?placeholder_key: "default" | "false" | "true" | "sep"
placeholder_option: placeholder_key "=" string_literal
placeholder: placeholder_option* expr

COMMAND1_CHAR: /[^~$}]/ | /\$[^{]/ | /~[^{]/
COMMAND1_END: COMMAND1_CHAR* "$"? "~"? "}"
COMMAND1_FRAGMENT: COMMAND1_CHAR* "${"
                 | COMMAND1_CHAR* "~{"
command1: "command" "{" [(COMMAND1_FRAGMENT placeholder "}")*] COMMAND1_END -> command

COMMAND2_CHAR: /[^~>]/ | /~[^{]/ | />[^>]/ | />>[^>]/
COMMAND2_END : COMMAND2_CHAR* ">"~0..2 ">>>"
COMMAND2_FRAGMENT: COMMAND2_CHAR* "~{"
command2: "command" "<<<" [(COMMAND2_FRAGMENT placeholder "}")*] COMMAND2_END -> command

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
input_decls: "input" "{" [any_decl*] "}"
?task_sections1: input_decls
               | meta_section
               | any_decl+ -> noninput_decls
output_decls: "output" "{" [bound_decl*] "}"
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
scatter: "scatter" "(" CNAME "in" expr ")" "{" [inner_workflow_element*] "}"
conditional: "if" "(" expr ")" "{" [inner_workflow_element*] "}"

?workflow_element: any_decl | call | scatter | conditional | meta_section | output_decls
workflow: "workflow" CNAME "{" workflow_element* "}"

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

larks_by_start = {} # memoize Lark parsers constructed for various start symbols
def parse(txt : str, start : str) -> lark.Tree:
    if start not in larks_by_start:
        larks_by_start[start] = lark.Lark(grammar, start=start, parser="lalr", propagate_positions=True)
    return larks_by_start[start].parse(txt)

