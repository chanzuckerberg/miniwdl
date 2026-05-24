version 1.2

task deprecated_runtime {
    command <<<
        true
    >>>
    runtime {
        docker: "ubuntu:latest"
        returnCodes: [0, 1]
    }
}
