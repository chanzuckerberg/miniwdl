import "contrived.wdl"
import "empty.wdl"

workflow bs {
    Int? x
    Int y = x
    Float? z
    Array[Int] a = [x]
    Array[String]+ a2 = [x]
    Int zi = round(z)
    call contrived.contrived
    call empty.empty
    output {
        Int i = contrived.read_int
    }
}
