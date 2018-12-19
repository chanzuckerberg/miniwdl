version 1.0

workflow has_incomplete_call {
    call sum
}

task sum {
    Int x
    Int y
    command <<<
        echo $(( ~{x} + ~{y} ))
    >>>
    output {
        Int z = read_int(stdout())
    }
}
