import "incomplete.wdl"

workflow xx {
    call incomplete.sum { input:
        x = 1,
        y = 1
    }
}
