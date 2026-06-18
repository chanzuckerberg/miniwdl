version 1.2

struct CollectionSample {
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

struct CollectionMathReport {
  Array[Array[String]] term_chunks
  Array[String] chunk_labels
  Array[String] count_keys
  Array[Int] count_values
  Pair[Array[String], Array[Int]] unzipped_counts
  Boolean has_beta
  Boolean has_zeta
  Boolean has_species
  Boolean has_nested_platform
  Int exponentiated
  Int term_count
  Int annotation_count
  Int note_chars
  String suffix_or_default
  Array[String] prefixed_terms
  Array[String] suffixed_terms
  Array[String] quoted_terms
  Array[String] squoted_terms
  Map[String, Int] indexed_term_map
  Array[Pair[String, Int]] small_cross
  Map[String, Array[String]] grouped_terms
}

workflow collection_math_features {
  input {
    Array[String]+ terms
    CollectionSample sample
    String? maybe_suffix
    String note_text
  }

  Map[String, Int] counts = {"alpha": 5, "beta": 8, "gamma": 13}
  Array[String] local_count_keys = keys(counts)
  Array[Int] local_count_values = values(counts)
  Pair[Array[String], Array[Int]] local_unzipped_counts = unzip(as_pairs(counts))

  Array[Array[String]] local_term_chunks = chunk(terms, 2)
  scatter (term_chunk in local_term_chunks) {
    String local_chunk_label = sep("+", term_chunk)
  }

  Map[String, Map[String, String]] nested_annotations = {"sample": sample.annotations}
  Boolean local_has_beta = contains(terms, "beta")
  Boolean local_has_zeta = contains(terms, "zeta")
  Boolean local_has_species = contains_key(sample.annotations, "species")
  Boolean local_has_nested_platform = contains_key(nested_annotations, ["sample", "platform"])

  Int local_exponentiated = 2 ** 5
  Int local_term_count = length(terms)
  Int local_annotation_count = length(sample.annotations)
  Int local_note_chars = length(note_text)
  String local_suffix_or_default = select_first([maybe_suffix], "fallback-suffix")
  Array[String] local_prefixed_terms = prefix("--term=", terms)
  Array[String] local_suffixed_terms = suffix(".txt", terms)
  Array[String] local_quoted_terms = quote(terms)
  Array[String] local_squoted_terms = squote(terms)
  Array[Pair[String, Int]] indexed_terms = zip(terms, range(length(terms)))
  Map[String, Int] local_indexed_term_map = as_map(indexed_terms)
  Array[Pair[String, Int]] local_small_cross = cross(["x", "y"], [1, 2])
  Map[String, Array[String]] local_grouped_terms = collect_by_key([
    ("odd", "alpha"),
    ("even", "beta"),
    ("odd", "gamma"),
    ("even", "delta")
  ])

  output {
    CollectionMathReport report = CollectionMathReport {
      term_chunks: local_term_chunks,
      chunk_labels: local_chunk_label,
      count_keys: local_count_keys,
      count_values: local_count_values,
      unzipped_counts: local_unzipped_counts,
      has_beta: local_has_beta,
      has_zeta: local_has_zeta,
      has_species: local_has_species,
      has_nested_platform: local_has_nested_platform,
      exponentiated: local_exponentiated,
      term_count: local_term_count,
      annotation_count: local_annotation_count,
      note_chars: local_note_chars,
      suffix_or_default: local_suffix_or_default,
      prefixed_terms: local_prefixed_terms,
      suffixed_terms: local_suffixed_terms,
      quoted_terms: local_quoted_terms,
      squoted_terms: local_squoted_terms,
      indexed_term_map: local_indexed_term_map,
      small_cross: local_small_cross,
      grouped_terms: local_grouped_terms
    }
  }
}
