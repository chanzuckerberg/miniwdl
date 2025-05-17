# cover obscure Linter code paths
version 1.0

import "empty.wdl" as popular
import 'empty.wdl' as contrived  # tests issue #601

struct contrived {
}

struct popular {
}

workflow contrived {
    scatter (popular in [1]) {
    }
    scatter (contrived in [2]) {
    }
}

task popular {
    Int contrived

    command {
        echo "~{contrived}"
    }

    output {
    }
}
