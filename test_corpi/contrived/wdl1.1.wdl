version 1.1

struct Car {
    String make
    String model
}

task wdl11 {
    input {
        Int x
        Array[String] s
        File reference_fasta_gz = "ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz"
    }
    command {
        echo "~{sep=', ' s}"
        ls "~{reference_fasta_gz}"
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
