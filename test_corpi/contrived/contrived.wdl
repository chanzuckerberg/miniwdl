# examples contrived to cover obscure Linter code paths otherwise missed
version 1.0

import "empty.wdl" as popular
import "empty.wdl" as contrived

workflow contrived {
    input {
        String popular = "fox"
        Int? contrived = 42
    }
    Int? fallaciously_optional = 123
    String unbound
    call popular { input:
        popular = popular,
        i = contrived,
        y = contrived
    }
    call popular as contrived
}

task popular {
    input {
        String popular
        String? opt
        Float? i
        Array[String]+ y = select_all([popular + i])
    }
    String? x = popular + opt   # rhs expr is non-optional although opt is...
    String unbound

    command {
        echo "~{popular}"
        echo "${x} ${write_tsv(y)}"
    }
}
