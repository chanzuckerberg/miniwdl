# UnusedDeclaration warning for run_archive should be suppressed due to
# presence of meta fields indicating this is a dxWDL native applet stub.
task bcl2fastq220 {
    input {
        Array[File] run_archive
    }
    command {
    }
    output {
        Array[File]+ stats = [""]
    }
    meta {
        type: "native"
        id: "applet-xxxx"
    }
}
