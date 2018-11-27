# examples contrived to cover obscured Linter code paths otherwise missed
version 1.0

import "empty.wdl" as popular

workflow contrived {
    String popular = "fox"
    call echo as popular { input:
        popular = popular,
        i = 42
    }
}

task echo {
    String popular
    String? opt
    Float? i
    String x = popular + opt
    String y = popular + i

    command {
        echo "~{popular}"
        echo "${x} ${y}"
    }
}
