version 1.1

struct Car {
    String make
    String model
}

task wdl11 {
    input {
        Int x
        Array[String] s
    }
    command {
        echo "~{sep=', ' s}"
    }
    # deprecated coercions
    output {
        String y = x + "z"
        String w = "~{'--foo ' + x}"
        Car c = object {
            make: "Toyota",
            model: "Prius"
        }
    }
}
