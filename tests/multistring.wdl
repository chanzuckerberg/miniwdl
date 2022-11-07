version development

# Test cases for multi-line string literals (openwdl PR# 414)
# Other test cases can found in tests/test_0eval.py:TestEval:test_multiline_string. Many cases are
# easier to write here without having to reason about two levels of escaping (python+WDL).

# runner.t executes this WDL and you can run it alone in your miniwdl repo clone:
#   python3 -m WDL tests/multistring.wdl --dir /tmp --verbose

workflow test_multistring {
    String speed = "quick"
    String color = "brown"
    String verb = "jumps"

    # Test cases: pairs with a multi-line string and an escaped single-line string; the two should
    # equal each other
    Array[Pair[String, String]] examples = [
        ("""
    The ${speed} ~{color} fox
        ${verb} over \
    the lazy dog.""",
         "The quick brown fox\n    jumps over the lazy dog."),

        ('''
            He continued in another tweet on Aug. 29:
              "When you see 'anonymous source,' stop reading the\
               story, it is fiction!"
            ''',
        "He continued in another tweet on Aug. 29:\n"
        + '  "When you see \'anonymous source,\' stop reading the   story, it is fiction!"\n'),

        ("""
                this is a \
                double-quoted string \
                that contains no newlines""",
         "this is a double-quoted string that contains no newlines"),

        ("""
            text indented by 4 spaces
          """,
         "  text indented by 4 spaces\n"),

        ("""
            '''""" + '''
            """''',
         "'''\"\"\"")
    ]

    scatter (ex in examples) {
        if (ex.left != ex.right) {
            call fail { input: lhs = ex.left, rhs = ex.right }
        }
    }

    output {
        Int cases_ok = length(examples)
    }
}

task fail {
    input {
        String lhs
        String rhs
    }

    command {
        >&2 echo "$(cat ~{write_json(lhs)}) != $(cat ~{write_json(rhs)})"
        exit 1
    }

    output {}
}
