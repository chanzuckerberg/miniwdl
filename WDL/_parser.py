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

          | [CNAME ("." CNAME)*] -> ident
          | CNAME "(" [expr ("," expr)*] ")" -> apply

?literal: "true" -> boolean_true
        | "false" -> boolean_false
        | INT -> int
        | SIGNED_INT -> int
        | FLOAT -> float
        | SIGNED_FLOAT -> float

// string (single-quoted)
STRING1_CHAR: "\\'" | /[^'$]/ | /\$[^{]/
STRING1_END: STRING1_CHAR* "$"? "'"
STRING1_FRAGMENT: STRING1_CHAR* "${"
string1: /'/ [(STRING1_FRAGMENT expr "}")*] STRING1_END -> string

// string (double-quoted)
STRING2_CHAR: "\\\"" | /[^"$]/ | /\$[^{]/
STRING2_END: STRING2_CHAR* "$"? /"/
STRING2_FRAGMENT: STRING2_CHAR* "${"
string2: /"/ [(STRING2_FRAGMENT expr "}")*] STRING2_END -> string

?string: string1 | string2

STRING_INNER1: ("\\\'"|/[^']/)
ESCAPED_STRING1: "'" STRING_INNER1* "'"
string_literal: ESCAPED_STRING | ESCAPED_STRING1

// WDL types and declarations
type: "Int" QUANT? -> int_type
    | "Float" QUANT? -> float_type
    | "Boolean" QUANT? -> boolean_type
    | "String" QUANT? -> string_type
    | "Array[" type "]" ARRAY_QUANT? -> array_type
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

// task meta/runtime sections (JSON-like)
meta_object: "{" [meta_kv (","? meta_kv)*] "}"
meta_kv: CNAME ":" meta_value
?meta_string: ESCAPED_STRING -> string
            | ESCAPED_STRING1 -> string
meta_literal: literal
            | meta_string
?meta_value: meta_literal
           | "[" [meta_value ("," meta_value)*] "]" -> meta_array
           | meta_object
META_KIND: "meta" | "parameter_meta" | "runtime"
meta_section: META_KIND meta_object

// WDL tasks
input_decls: "input" "{" [any_decl*] "}"
?task_sections1: input_decls
               | bound_decl+ -> noninput_decls
               | meta_section
output_decls: "output" "{" [bound_decl*] "}"
?task_sections2: output_decls
               | meta_section
task: "task" CNAME "{" task_sections1* command task_sections2* "}"

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

def parse(txt : str, start : str) -> lark.Tree:
  return lark.Lark(grammar, start=start, parser="lalr", propagate_positions=True).parse(txt)

