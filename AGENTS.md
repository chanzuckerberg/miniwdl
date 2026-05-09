You're working on the source code for `miniwdl`, the Workflow Description Language (WDL) runner and developer toolkit.

Read `CONTRIBUTING.md` for an overview of the codebase and development workflow. In particular:
- If you're not started in a suitable virtualenv, bootstrap one under `venv/`.
- Python code should be linted with `mypy`, `ruff check --fix`, and `ruff format`; excluding test code.
- Testing guidelines:
    - The test suite assumes access to the Internet and dockerd (via unix socket), so make sure you have the necessary user permissions before proceeding.
    - While iterating on a task, it's usually best to run a targeted set of test cases that turns around quickly.
    - It's worth running `make qtest` before final task completion, but it takes several minutes. If the user is conversing with you interactively, then check whether they want any further changes before taking that time.
    - Reserve the full `make test` for user request or when you're sure the diff involves one of the slower unit tests or integration tests skipped by `make qtest`.

For many tasks it'll be useful to refer to the WDL 1.2 specification, which you can find under `spec/wdl-1.2/SPEC.md`. The version changelog is `spec/wdl-1.2/CHANGELOG.md`, and the older version 1.1 spec is `spec/wdl-1.1/SPEC.md`.

These development tutorials under `docs/` introduce a few common ways the codebase is used and extended.
- `trace_identifiers.md` -- basic syntax tree traversal
- `wdlviz.md` -- generating graphviz diagrams from WDL source code
- `add_functions.md `-- adding new functions to the standard library
- `assert.md` -- adding a new WDL language feature, with parsing, type-checking, and runtime execution

Test placement note:
- StdLib-focused tests belong in `tests/test_5stdlib.py` (even when they only exercise expression-level stdlib behavior); keep `tests/test_0eval.py` for general expression/value evaluation coverage.
