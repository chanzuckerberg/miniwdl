version 1.0

task hello {
    input {
        String who
    }
    command {
        echo "Hello, ${who}!"
    }
    output {
        String message = read_string(stdout())
    }
}

workflow hello_wf {
    input {
        Array[String] names = ["Alyssa P. Hacker", "Ben Bitdiddle"]
        Boolean extra = false
    }
    scatter (name in names) {
        call hello { input:
            who = name
        }
    }
    if (extra) {
        scatter (name in names) {
            call hello as hello2 { input:
                who = name
            }
        }
    }
    call hello as hello3 { input:
        who = names[0]
    }
}