version development

struct Struct1 {
    Array[String] strings
}

task t {
    input {
        File? f
        String s_in
        Struct1 struct1
    }
    String s = f + "a" + basename(f)
    String s2 = f
    Array[String] a = [f, f]
    Struct1 struct2 = object { strings: [f, f] }
    command {
        echo ~{s_in} ~{s} ~{s2} ~{sep("", a)} ~{struct1} ~{struct2}
    }
    output {}
}

workflow w {
    input {
        File? f
    }
    call t {
        input:
        s_in = f,
        struct1 = Struct1 { strings:[f, f] }
    }
}
