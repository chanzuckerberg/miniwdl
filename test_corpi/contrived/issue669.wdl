version 1.0

task TestTask {

    input {
        Array[String] string_list 
        Array[Boolean] bool_list_1 
        Array[Boolean] bool_list_2 
        Array[Boolean] bool_list_3 
        Array[Float] float_list 
        Array[File] file_list
    }

    command <<<
      # Passing Tests:

      # Trivial Tests:
      cat ~{write_tsv(transpose([string_list]))}
      cat ~{write_tsv(transpose([bool_list_1]))}
      cat ~{write_tsv(transpose([float_list]))}
      cat ~{write_tsv(transpose([file_list]))}
      cat ~{write_tsv(transpose([bool_list_1, bool_list_2, bool_list_3]))}

      # Basic Type coercion tests:
      cat ~{write_tsv(transpose([string_list, file_list]))}
      cat ~{write_tsv(transpose([string_list, file_list, bool_list_1]))}

      cat ~{write_tsv(transpose([string_list, bool_list_1, bool_list_2, bool_list_3]))}
      cat ~{write_tsv(transpose([string_list, bool_list_1, bool_list_2, bool_list_3, float_list]))}

      # Special test case:
      cat ~{write_tsv(transpose([string_list, file_list, float_list]))}

      # Failing Tests:

      # NOTE: This is just a re-ordering of the special test case, which passes.
      cat ~{write_tsv(transpose([string_list, float_list, file_list]))}

      # Original code from WDL:
      # NOTE: This code runs correctly on Cromwell (V87-e3a923f):
      cat ~{write_tsv(transpose([string_list, bool_list_1, bool_list_2, bool_list_3, float_list, file_list]))}
    >>>

    output {
      Array[String] stdout = read_lines(stdout())
    }

    #########################
    runtime {
        cpu:    1 
        docker: "ubuntu:22.04"
    }
}
