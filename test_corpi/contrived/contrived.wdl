# examples contrived to cover obscure Linter code paths otherwise missed
version 1.0

import "empty.wdl" as popular
import "empty.wdl" as contrived

workflow contrived {
    input {
        String popular = "fox"
        Int? contrived = 42
        Float required
    }
    Int? fallaciously_optional = 123
    call popular { input:
        popular = popular,
        i = contrived,
        y = [select_first([contrived,23])]
    }
    call popular as contrived { input:
        popular = 123
    }

    output {
        Int read_int = read_json(popular.json)
        Array[Boolean] read_array = read_json(contrived.json)
        String left_contents = contrived.left_contents
        String right_contents = contrived.right_contents
    }
}

task popular {
    input {
        String popular
        String? opt
        Float? i
        Array[String]+ y = select_all([popular + i])
    }
    String? x = popular + opt   # rhs expr is non-optional although opt is...
    Array[String] args = prefix("-f", y)
    Pair[String,String] contents = (read_string(popular), size([popular]))

    command {
        echo "~{popular} ~{contents.left}"
        echo "${x} ${write_tsv([y])}"
    }

    output {
        String stdout = stdout()
        File json = write_json(args)
        Array[String] nums = [1]
        String left_contents = contents.left
        String right_contents = contents.right
    }
}
