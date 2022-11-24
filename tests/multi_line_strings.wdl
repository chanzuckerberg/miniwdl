version development

# Test cases for multi-line string literals (openwdl PR# 414)
# The integration test suite executes this WDL (via multi_line_strings.t) which you can also run
# standalone in your miniwdl repo clone:
#   python3 -m WDL run tests/multi_line_strings.wdl --dir /tmp --verbose

workflow test_multi_line_strings {
    String speed = "quick"
    String color = "brown"
    String verb = "jumps"

    String spaces = "  "
    String name = "Henry"
    String company = "Acme"

    # Test cases: pairs with a multi-line string and an escaped single-line string; the two should
    # equal each other.
    Array[Pair[String, String]] examples = [
        (<<<
    The ~{speed} ~{color}
        fox ~{verb} over \
            the lazy dog.>>>,
         "The quick brown\n    fox jumps over the lazy dog."),
         (<<<hello  world>>>, "hello  world"),
         (<<<   hello  world   >>>, "hello  world"),
         (<<<   
            hello  world>>>, "hello  world"),
        (<<<   
            hello  world
            >>>, "hello  world"),
        (<<<
        hello  \
        world \
        >>>, "hello  world"),
        (<<<

        hello  world

        >>>, "\nhello  world\n"),
        (<<<
        hello  \
          world
        >>>, "hello  world"),
        (<<<
        this is a

          multi-line string
>>>, "this is a\n\n  multi-line string"),
        (<<<
        this is a
    
        multi-line string
        
 >>>, "this is a\n\nmulti-line string\n"),
        (<<<
        this is a \
            string that \
        contains no newlines
        >>>, "this is a string that contains no newlines"),
        (<<<
        multi-line string \
        with 'single' and "double" quotes
        >>>, "multi-line string with 'single' and \"double\" quotes"),
        (<<<
        ~{spaces}Hello ~{name},
        ~{spaces}Welcome to ~{company}!
        >>>, "  Hello Henry,\n  Welcome to Acme!"),
        (<<<
        \x20 Forced
          indentation
        >>>, "  Forced\n  indentation"),
        (<<<abc\
        >>>, "abc"),
        (<<<abc\\
        >>>, "abc\\"),
        (<<<abc\\>>>, 'abc\\'),
        (<<<abc\\
def>>>, "abc\\\ndef"),
        (<<<abc\\\
                def>>>, "abc\\def")
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
