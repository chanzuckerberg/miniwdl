# examples contrived to cover obscure Linter code paths otherwise missed
version 1.0

import "empty.wdl" as popular
import "empty.wdl" as contrived

workflow contrived {
    String popular = "fox"
    Int contrived = 42
    call popular { input:
        popular = popular,
        i = contrived,
        y = contrived
    }
}

task popular {
    String popular
    String? opt
    Float? i
    String x = popular + opt
    Array[String]+ y = select_all([popular + i])

    command {
        echo "~{popular}"
        echo "${x} ${sep=';' y}"
    }
}
