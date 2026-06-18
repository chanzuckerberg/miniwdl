version 1.2

struct PathTextReport {
  String note_text
  String nested_text
  String? first_wdl_version
  Boolean note_mentions_paths
  String rewritten_note
}

workflow path_text_features {
  input {
    Directory source_dir
  }

  String local_note_text = read_string(join_paths(source_dir, "note.txt"))
  File local_nested_file = join_paths(source_dir, ["nested", "extra.txt"])
  String local_nested_text = read_string(local_nested_file)
  String? local_first_wdl_version = find(local_note_text, "WDL [0-9][.][0-9]")
  Boolean local_note_mentions_paths = matches(local_note_text, "source-relative")
  String local_rewritten_note = sub(local_note_text, "source-relative", "source relative")

  output {
    PathTextReport report = PathTextReport {
      note_text: local_note_text,
      nested_text: local_nested_text,
      first_wdl_version: local_first_wdl_version,
      note_mentions_paths: local_note_mentions_paths,
      rewritten_note: local_rewritten_note
    }
  }
}
