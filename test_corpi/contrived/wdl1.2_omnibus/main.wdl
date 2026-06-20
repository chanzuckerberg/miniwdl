version 1.2

import "lib/helpers.wdl" as helper
import "lib/path_text.wdl" as path_text
import "lib/collection_math.wdl" as collection_math

struct Sample {
  String id
  String label
  Map[String, String] annotations

  meta {
    description: "Top-level sample struct; coerced to helper.HelperSample at the imported task call."
  }

  parameter_meta {
    id: "Short sample identifier"
    label: "Human-readable display name"
    annotations: "Small ordered key/value metadata map"
  }
}

workflow wdl12_omnibus {
  input {
    File seed_file = "data/seed.txt"
    File table_file = "data/table.tsv"
    Directory source_dir = "data/source_dir"
    String greeting = "hello from a WDL 1.2 env input"
    String? maybe_suffix = None
    Array[String]+ terms = ["alpha", "beta", "gamma", "delta", "epsilon"]
    Sample sample = Sample {
      id: "sample-A",
      label: "toy omnibus sample",
      annotations: {
        "species": "synthetic",
        "platform": "miniwdl",
        "purpose": "wdl-1.2-demo"
      }
    }
  }

  meta {
    description: "Contrived WDL 1.2 omnibus workflow"
    spec_version: "1.2"
  }

  parameter_meta {
    source_dir: "Source-relative input directory declared in the WDL document"
    seed_file: "Source-relative input file declared in the WDL document"
    table_file: "Source-relative TSV used by read_tsv overloads in the task"
    terms: "Terms used to exercise array and map standard library functions"
  }

  call path_text.path_text_features as paths {
    source_dir
  }

  call collection_math.collection_math_features as collections {
    terms,
    sample = sample,
    maybe_suffix,
    note_text = paths.report.note_text
  }

  call helper.inspect_inputs as inspect {
    greeting,
    sample = sample,
    seed_file,
    table_file,
    source_dir,
    maybe_suffix
  }

  String banner = <<<
    WDL 1.2 omnibus
    sample: ~{sample.id}
    terms: ~{sep(", ", terms)}
  >>>

  call helper.echo_lines as echo_banner {
    lines = [banner, inspect.report.summary_text, paths.report.rewritten_note],
    out_name = "banner.txt"
  }

  if (collections.report.has_beta && collections.report.has_species) {
    call helper.echo_lines as echo_chunks {
      lines = collections.report.chunk_labels,
      out_name = "chunks.txt"
    }
  }

  output {
    InspectReport task_inspection = inspect.report
    PathTextReport path_text = paths.report
    CollectionMathReport collection_math = collections.report
    File echoed_banner = echo_banner.out
    File? echoed_chunks = echo_chunks.out
  }

  hints {
    allow_nested_inputs: true
  }
}
