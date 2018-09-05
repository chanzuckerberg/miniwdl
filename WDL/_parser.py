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

          | string1
          | string2

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

%import common.INT
%import common.SIGNED_INT
%import common.FLOAT
%import common.SIGNED_FLOAT
%import common.CNAME
%import common.WS
%ignore WS
"""

def parse(txt : str, start : str) -> lark.Tree:
  return lark.Lark(grammar, start=start, parser="lalr", propagate_positions=True).parse(txt)

