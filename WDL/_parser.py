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

          | "true" -> boolean_true
          | "false" -> boolean_false
          | "!" expr -> negate

          | INT -> int
          | SIGNED_INT -> int
          | FLOAT -> float
          | SIGNED_FLOAT -> float

          | string

          | "[" [expr ("," expr)*] "]" -> array
          | expr_core "[" expr "]" -> get

          | "if" expr "then" expr "else" expr -> ifthenelse

          | [CNAME ("." CNAME)*] -> ident
          | CNAME "(" [expr ("," expr)*] ")" -> apply

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

// WDL types and declarations
type: "Int" -> int_type
    | "Float" -> float_type
    | "Boolean" -> boolean_type
    | "String" -> string_type
    | "Array[" type "]" -> array_type

unbound_decl: type CNAME -> decl
bound_decl: type CNAME "=" expr -> decl
?any_decl: unbound_decl | bound_decl
input_decls: "input" "{" [any_decl*] "}"
output_decls: "output" "{" [bound_decl*] "}"

// WDL tasks
!?placeholder_key: "default" | "false" | "true" | "sep"
PLACEHOLDER_VALUE: ESCAPED_STRING | ESCAPED_STRING1
placeholder_option: placeholder_key "=" PLACEHOLDER_VALUE
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

task: "task" CNAME "{" input_decls? [bound_decl*] command output_decls? "}"


STRING_INNER1: ("\\\'"|/[^']/)
ESCAPED_STRING1: "'" STRING_INNER1* "'"

%import common.INT
%import common.SIGNED_INT
%import common.FLOAT
%import common.SIGNED_FLOAT
%import common.CNAME
%import common.ESCAPED_STRING
%import common.WS
%ignore WS
"""

def parse(txt : str, start : str) -> lark.Tree:
  return lark.Lark(grammar, start=start, parser="lalr", propagate_positions=True).parse(txt)

