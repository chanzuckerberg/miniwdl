version 1.0

struct Sample {
    String name
    Int lane
    String barcode
}

workflow w {
    input {
        File f
    }
    call t {
        input:
        sample = read_map(f)
    }
}

task t {
    input {
        Sample sample
    }
    command { echo "~{sample}" }
    output {
        Sample samplesheet2 = read_json("samplesheet.json")
        Array[Sample] samplesheets = read_objects("samplesheet.txt")
        Sample alice = {"name": "Alice", "lane": 3, "barcode": "GATTACA"}
    }
}
