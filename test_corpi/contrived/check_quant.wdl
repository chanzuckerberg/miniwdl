import "contrived.wdl"

workflow bs {
    Int? x
    Int y = x
    call contrived.contrived
}
