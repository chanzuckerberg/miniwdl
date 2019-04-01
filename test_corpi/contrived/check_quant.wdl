import "contrived.wdl"
import "empty.wdl"

workflow bs {
    Int? x
    Int y = x
    Float? z
    Array[Int]? a = [x]
    Array[String]+ a2 = [x]
    Array[String]+ a3 = x
    Array[Pair[Int,Int]] a4 = zip([select_first(a)],select_all(a))
    Int zi = round(z)
    call contrived.contrived
    call empty.empty
    output {
        Int i = contrived.read_int
    }
}

task arraycoercion {
    File f = write_lines("hello")

    command {
        cat "${f}"
    }
}
