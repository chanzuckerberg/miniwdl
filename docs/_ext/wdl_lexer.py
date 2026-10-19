"""
Minimal Pygments lexer for WDL code blocks in the docs (Pygments doesn't ship one)
"""

from typing import ClassVar

from pygments.lexer import RegexLexer, bygroups, include, words
from pygments.token import (
    Comment,
    Keyword,
    Name,
    Number,
    Operator,
    Punctuation,
    String,
    Text,
    Whitespace,
)


class WDLLexer(RegexLexer):
    name = "WDL"
    aliases: ClassVar = ["wdl"]
    filenames: ClassVar = ["*.wdl"]

    tokens: ClassVar = {
        "root": [
            (r"\s+", Whitespace),
            (r"#.*$", Comment.Single),
            (r"(version)(\s+)(\S+)", bygroups(Keyword.Namespace, Whitespace, Name.Constant)),
            (r"(command)(\s*)(<<<)", bygroups(Keyword, Whitespace, String.Heredoc), "heredoc"),
            (r"(command)(\s*)(\{)", bygroups(Keyword, Whitespace, String.Heredoc), "command"),
            (
                r"(task|workflow|struct)(\s+)([A-Za-z_]\w*)",
                bygroups(Keyword.Declaration, Whitespace, Name.Class),
            ),
            (r"(call)(\s+)([A-Za-z_][\w.]*)", bygroups(Keyword, Whitespace, Name.Function)),
            (words(("import", "as", "alias"), suffix=r"\b"), Keyword.Namespace),
            (
                words(
                    (
                        "input",
                        "output",
                        "runtime",
                        "requirements",
                        "hints",
                        "meta",
                        "parameter_meta",
                        "scatter",
                        "in",
                        "if",
                        "then",
                        "else",
                        "after",
                        "env",
                        "object",
                        "assert",
                    ),
                    suffix=r"\b",
                ),
                Keyword,
            ),
            (words(("true", "false", "None"), suffix=r"\b"), Keyword.Constant),
            (
                words(
                    (
                        "Array",
                        "Map",
                        "Pair",
                        "Object",
                        "File",
                        "Directory",
                        "String",
                        "Int",
                        "Float",
                        "Boolean",
                    ),
                    suffix=r"\b",
                ),
                Keyword.Type,
            ),
            (r"[A-Za-z_]\w*(?=\s*\()", Name.Builtin),
            (r"[A-Za-z_]\w*", Name),
            (r"\d+\.\d*([eE][+-]?\d+)?|\.\d+([eE][+-]?\d+)?|\d+[eE][+-]?\d+", Number.Float),
            (r"0[xX][0-9a-fA-F]+|\d+", Number.Integer),
            (r'"', String.Double, "dqs"),
            (r"'", String.Single, "sqs"),
            (r"<<<", String.Heredoc, "mlstring"),
            (r"==|!=|<=|>=|&&|\|\||[-+*/%<>=!?]", Operator),
            (r"[{}()\[\],.:]", Punctuation),
        ],
        "placeholder": [
            (r"([~$]\{)", String.Interpol, "interp"),
        ],
        "interp": [
            (r"\}", String.Interpol, "#pop"),
            include("root"),
        ],
        "dqs": [
            include("placeholder"),
            (r"\\.", String.Escape),
            (r'"', String.Double, "#pop"),
            (r'[^"\\~$]+|[~$]', String.Double),
        ],
        "sqs": [
            include("placeholder"),
            (r"\\.", String.Escape),
            (r"'", String.Single, "#pop"),
            (r"[^'\\~$]+|[~$]", String.Single),
        ],
        "heredoc": [
            (r"~\{", String.Interpol, "interp"),
            (r">>>", String.Heredoc, "#pop"),
            (r"[^~>]+|[~>]", Text),
        ],
        "mlstring": [
            (r"~\{", String.Interpol, "interp"),
            (r">>>", String.Heredoc, "#pop"),
            (r"[^~>]+|[~>]", String.Heredoc),
        ],
        "command": [
            include("placeholder"),
            (r"\}", String.Heredoc, "#pop"),
            (r"[^~${}]+|[~$]", Text),
            (r"\{", Text, "command_brace"),
        ],
        "command_brace": [
            include("placeholder"),
            (r"\}", Text, "#pop"),
            (r"[^~${}]+|[~$]", Text),
            (r"\{", Text, "#push"),
        ],
    }


def setup(app):
    app.add_lexer("wdl", WDLLexer)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
