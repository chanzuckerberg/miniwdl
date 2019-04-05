# cover obscure Linter code paths
version 1.0

import "empty.wdl" as popular
import "empty.wdl" as contrived

workflow contrived {
    scatter (popular in []) {
    }
    scatter (contrived in []) {
    }
}

task popular {
    command {
        echo "contrived"
    }

    output {
    }
}
