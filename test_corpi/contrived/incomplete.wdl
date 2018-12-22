version 1.0

workflow has_incomplete_call {
    call sum as add
}

task sum {
    input {
        Int x
        Int y
        String? msg = "hello"
        String? who
    }
    Boolean not_input = defined(who)
    command <<<
        echo $(( ~{x} + ~{y} ))
        echo "~{msg} ~{who} ~{not_input}" 2>&1
    >>>
    output {
        Int z = read_int(stdout())
    }
}
