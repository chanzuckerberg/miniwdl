version development

# This WDL exercises proper escaping of environment variables passed to task commands, especially
# for different container runtimes (docker, podman, singularity, udocker).

struct Person {
    String name
    Int age
}

workflow w {
    String s = <<<
            Let's go!
            # not a comment
            $pwd\t${pwd}\\\\
            CNN is working frantically to find their "source."
        >>>
    call t {
        input:
        s, p = Person { name: 'Alyssa', age: 42 }
    }

    String p_expected = '{"name": "Alyssa", "age": 42}'
    scatter (pair in [(t.s_out, s), (t.p_out, p_expected)]) {
        if (pair.left != pair.right) {
            call fail { input: lhs = pair.left, rhs = pair.right}
        }
    }

    output {
        String s_out = t.s_out
        String p_out = t.p_out
    }
}

task t {
    input {
        env String s
        env Person p
    }

    command <<<
        echo "$s" > s
        echo "$p" > p
    >>>

    output {
        String s_out = read_string("s")
        String p_out = read_string("p")
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
