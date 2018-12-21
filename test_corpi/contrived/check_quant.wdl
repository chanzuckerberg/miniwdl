import "contrived.wdl"
import "empty.wdl"

workflow bs {
    Int? x
    Int y = x
    Array[Int] a = [x]
    Array[String]+ a2 = [x]
    call contrived.contrived
    call empty.empty
}
