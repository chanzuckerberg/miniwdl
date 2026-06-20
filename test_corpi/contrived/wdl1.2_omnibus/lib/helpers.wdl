version 1.2

struct HelperSample {
  String id
  String label
  Map[String, String] annotations

  meta {
    description: "Imported struct with the same shape as main.Sample to demonstrate WDL 1.2 struct coercion."
  }

  parameter_meta {
    id: "Short sample identifier"
    label: "Human-readable display name"
    annotations: "Small ordered key/value metadata map"
  }
}

struct InspectReport {
  File summary
  Directory bundle
  String summary_text
  Array[Array[String]] tsv_plain
  Array[Map[String, String]] tsv_objects
  Array[Map[String, String]] renamed_tsv_objects
  String sample_from_env
  String runtime_line
  Int? return_code
}

task inspect_inputs {
  input {
    env String greeting
    env HelperSample sample
    File seed_file
    File table_file
    Directory source_dir
    String? maybe_suffix
    Array[String] tags = ["wdl", "one-point-two", "omnibus"]
  }

  File imported_relative_file = "data/imported_default.txt"
  String note_path = join_paths(source_dir, "note.txt")
  String nested_path = join_paths(source_dir, ["nested", "extra.txt"])
  String suffix_or_default = select_first([maybe_suffix], "no-suffix-supplied")

  command <<<
    set -euo pipefail

    mkdir -p bundle/nested
    cp "~{seed_file}" bundle/seed.txt
    cp "~{note_path}" bundle/note.txt
    cp "~{nested_path}" bundle/nested/extra.txt
    cp "~{imported_relative_file}" bundle/imported-default.txt
    cp "~{table_file}" table.copy.tsv

    {
      printf 'task=%s\n' "~{task.name}"
      printf 'container=%s\n' "~{task.container}"
      printf 'greeting_from_env=%s\n' "$greeting"
      printf 'sample_from_env=%s\n' "$sample"
      printf 'seed_basename=%s\n' "~{basename(seed_file, ".txt")}"
      printf 'tags=%s\n' "~{sep(",", tags)}"
      printf 'tag_args=%s\n' "~{sep(" ", prefix("--tag=", tags))}"
      printf 'suffix_or_default=%s\n' "~{suffix_or_default}"
      printf 'note=%s\n' "$(cat "~{note_path}")"
      printf 'nested=%s\n' "$(cat "~{nested_path}")"
      printf 'imported=%s\n' "$(cat "~{imported_relative_file}")"
    } > bundle/summary.txt

    exit 0
  >>>

  output {
    InspectReport report = InspectReport {
      summary: "bundle/summary.txt",
      bundle: "bundle",
      summary_text: read_string("bundle/summary.txt"),
      tsv_plain: read_tsv("table.copy.tsv"),
      tsv_objects: read_tsv("table.copy.tsv", true),
      renamed_tsv_objects: read_tsv("table.copy.tsv", true, ["name", "count", "group"]),
      sample_from_env: read_lines("bundle/summary.txt")[3],
      runtime_line: read_lines("bundle/summary.txt")[1],
      return_code: task.return_code
    }
  }

  requirements {
    container: "ubuntu:26.04"
    cpu: 1
    memory: "256 MiB"
    return_codes: [0, 1]
  }

  hints {
    max_cpu: 1
    max_memory: "512 MiB"
    short_task: true
    localization_optional: false
    inputs: input {
      seed_file: hints {
        localization_optional: true
      },
      source_dir: hints {
        localization_optional: true
      }
    }
    outputs: output {
      summary: hints {
        max_size: "4 KiB"
      },
      bundle: hints {
        max_size: "16 KiB"
      }
    }
  }

  meta {
    description: "Inspect source-relative inputs, env inputs, task runtime info, and WDL 1.2 TSV overloads."
  }

  parameter_meta {
    greeting: "Serialized into the task environment as $greeting"
    sample: "Struct serialized into the task environment as $sample"
    source_dir: "Directory input joined with relative child paths"
  }
}

task echo_lines {
  input {
    Array[String] lines
    String out_name = "echoed.txt"
  }

  command <<<
    printf '%s\n' ~{sep(" ", quote(lines))} > "~{out_name}"
  >>>

  output {
    File out = out_name
  }

  requirements {
    container: "ubuntu:26.04"
  }

  hints {
    short_task: true
  }
}
