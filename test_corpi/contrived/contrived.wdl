# examples contrived to cover obscure Linter code paths otherwise missed
version 1.0

import "empty.wdl" as popular
import "empty.wdl" as contrived  # !UnusedImport
# !NameCollision

struct contrived {
}

struct popular {
}

workflow contrived {
    input {
        String popular = "fox" # !NameCollision
        Int? fortytwo = 42
        Float required
    }
    Int? fallaciously_optional = 123
    call popular as c1 { input:
        popular = popular,
        i = fortytwo,
        y = [select_first([fortytwo,23])]
    }
    call popular as c2 { input:
        popular = 123
    }
    Pair[Pair[String,String],Pair[Int,Int]] p2 = ((c2.left_contents, c2.right_contents), (4,2))

    output {
        Int read_int = read_json(c1.json) + p2.right.left + p2.right.right
        Array[Boolean] read_array = read_json(c1.json)
        String left_contents = p2.left.left
        String right_contents = p2.left.right
    }
}

task popular {
    input {
        String popular
        # Lorem ipsum dolor sit (!NameCollision)
        String? opt
        Float? i
        Array[String]+ y = select_all(["${popular + i}"])
    }
    String? x = "${popular + opt}"   # rhs expr is non-optional although opt is...
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
        File? optional_file = "nonexistent.txt"
    }
}
