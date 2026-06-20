# WDL 1.2 omnibus example

This contrived corpus entry is a runnable toy workflow that demonstrates many WDL 1.2 features in one place:

- source-relative `File` and `Directory` declarations
- source-relative imports from `main.wdl` to thematic helper modules under `lib/`
- `Directory` inputs and outputs
- `requirements` and `hints` sections instead of deprecated `runtime`
- task `env` inputs, including a struct serialized into an environment variable
- task-scoped runtime information through `task.name`, `task.container`, and `task.return_code`
- multi-line string declarations
- `struct` `meta` and `parameter_meta`
- struct-to-struct coercion across documents
- call input shorthand and call bodies without `input:`
- `find`, `matches`, `contains`, `contains_key`, `chunk`, `join_paths`, `values`, `length(String|Map)`, `select_first` default, `read_tsv` header overloads, `quote`, `squote`, `prefix`, `suffix`, `zip`, `unzip`, `cross`, `as_map`, `as_pairs`, `collect_by_key`, and `**`

The example is split into thematic modules:

- `main.wdl` wires the feature groups together and exposes a single output surface.
- `lib/path_text.wdl` demonstrates source-relative `Directory` children, `join_paths`, file reads, and regex/string functions.
- `lib/collection_math.wdl` demonstrates collection, map, optional, and arithmetic functions.
- `lib/helpers.wdl` contains runnable tasks demonstrating task `env` inputs, requirements, hints, `Directory` outputs, task runtime info, and TSV reads.

The top-level workflow output block intentionally groups the thematic results into `InspectReport`, `PathTextReport`, and `CollectionMathReport` structs so the output surface stays readable.

Example invocation:

```bash
miniwdl run test_corpi/contrived/wdl1.2_omnibus/main.wdl \
  -i test_corpi/contrived/wdl1.2_omnibus/inputs.json
```

The tasks use `ubuntu:26.04`.
