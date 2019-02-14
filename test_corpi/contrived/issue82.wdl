version 1.0

workflow checkStringVariableUse {
  input {
    String inputString
  }
  String exampleUseCommand = "Hello ~~{inputString}"

  call justRun {
    input:
      exampleUseCommand = exampleUseCommand
  }
}

task justRun {
  input {
    String exampleUseCommand
  }

  command <<<
    echo ~$~{exampleUseCommand}
  >>>
}
