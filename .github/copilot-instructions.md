You're working on the source code for `miniwdl`, the Workflow Description Language (WDL) runner and developer toolkit.

See CONTRIBUTING.md for an overview of the codebase and development workflow. Note in particular that Python code should be linted with `mypy`, `ruff check`, and `ruff format`.

For many tasks it'll be useful to refer to the WDL 1.2 specification, which you can find under spec/wdl-1.2/SPEC.md. The version changelog is spec/wdl-1.2/CHANGELOG.md, and the older version 1.1 spec is spec/wdl-1.1/SPEC.md.

These development tutorials under docs/ introduce a few common ways the codebase is used and extended.
- trace_identifiers.md -- basic syntax tree traversal
- wdlviz.md -- generating graphviz diagrams from WDL source code
- add_functions.md -- adding new functions to the standard library
- assert.md -- adding a new WDL language feature, with parsing, type-checking, and runtime execution
