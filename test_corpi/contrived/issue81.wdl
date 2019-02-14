version 1.0

workflow testCompoundInputs {
  input {
    File input_file
    Int input_int
  }

  Array[String] input_arr_str = [input_file, input_int]

  call justEcho {
    input:
      input_arr_str = input_arr_str
  }
}

task justEcho {
  input {
    Array[String] input_arr_str
  }

  File writtenFile = write_lines(input_arr_str)

  command <<<
    cat ~{writtenFile}
  >>>
}
