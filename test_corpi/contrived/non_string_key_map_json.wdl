version 1.1

task t {
  input {
    Map[Int, String] m
  }

  command <<<
    true
  >>>

  output {
    Map[Int, String] out = m
    File js = write_json(m)
  }
}

workflow w {
  input {
    Map[Int, String] m
  }

  call t { input: m = m }

  output {
    Map[Int, String] out = t.out
    Map[String, Map[Int, String]] nested = {"x": t.out}
    Map[String, String] ok = {"a": "b"}
  }
}
