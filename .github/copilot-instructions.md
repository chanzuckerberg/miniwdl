You're working on the source code for `miniwdl`, the Workflow Description Language (WDL) runner and developer toolkit.

## Project Overview

miniwdl is a Python 3.8+ implementation of WDL that includes:
- A WDL parser and type checker
- A local workflow runner with Docker/Singularity/Podman support
- Developer toolkit for building WDL-based tools
- Standard library implementation for WDL functions

## Development Setup

1. Clone with submodules: `git clone --recursive` or `git submodule update --init --recursive`
2. Install dependencies: `pip3 install '.[dev]'`
3. Ensure Docker access (user must have Docker control permissions)
4. Test setup: `python3 -m WDL run_self_test`

Key command: `python3 -m WDL ...` invokes the local source tree (equivalent to `miniwdl ...` entry point)

## Code Style and Linting

Python code **must** be linted with:
- `ruff check --ignore E741 WDL` - Check for code issues
- `ruff format --line-length 100 WDL` - Format code
- `mypy WDL` - Type checking

Run `make check` to execute all linting checks, or `make pretty` to auto-format code.

## Testing

### Quick test workflow
- `make qtest` - Fast test suite (fails fast, skips coverage, omits slower cases)
- `pytest -vx --tb=short -n auto tests` - Run Python unit tests
- `prove -v tests/{check,eval,runner,zip}.t` - Run bash-tap integration tests

### Full test suite
- `make test` - Complete test suite with coverage (takes several minutes)
- `make unit_tests` - Python unit tests with coverage
- `make integration_tests` - Integration tests using bash-tap

### Targeted testing
- `python3 -m unittest -f tests/test_5stdlib.py` - Run specific test file
- `python3 -m unittest -f tests.test_5stdlib.TestStdLib.test_glob` - Run specific test

## Architecture & Documentation

See **CONTRIBUTING.md** for:
- Detailed codebase overview (AI-generated)
- Module-by-module documentation
- Code structure guide
- Test suite organization

Key modules:
- `WDL/_parser.py`, `WDL/_grammar.py` - Parsing with Lark
- `WDL/Type.py`, `WDL/Value.py` - Type system and values
- `WDL/Expr.py` - Expression evaluation
- `WDL/Tree.py` - Abstract Syntax Tree definitions
- `WDL/StdLib.py` - Standard library functions
- `WDL/runtime/` - Execution engine (task, workflow, backends, caching)
- `WDL/CLI.py` - Command-line interface

## WDL Specification

Refer to the WDL specifications when implementing features:
- **WDL 1.2**: `spec/wdl-1.2/SPEC.md` (current version)
- **Changelog**: `spec/wdl-1.2/CHANGELOG.md`
- **WDL 1.1**: `spec/wdl-1.1/SPEC.md` (previous version)

## Development Tutorials

These tutorials under `docs/` demonstrate common extension patterns:
- `trace_identifiers.md` - Basic syntax tree traversal
- `wdlviz.md` - Generating graphviz diagrams from WDL source
- `add_functions.md` - Adding new functions to the standard library
- `assert.md` - Adding a new WDL language feature (parsing, type-checking, runtime execution)

## Common Tasks

- **Run WDL locally**: `python3 -m WDL run workflow.wdl`
- **Check WDL syntax**: `python3 -m WDL check workflow.wdl`
- **Generate documentation**: `make doc` (builds to `docs/_build/html/`)
- **Build distribution**: `make bdist`

## Additional Resources

- [Online documentation](https://miniwdl.readthedocs.io/en/latest/)
- [Getting started guide](https://miniwdl.readthedocs.io/en/latest/getting_started.html)
- [Runner reference](https://miniwdl.readthedocs.io/en/latest/runner_reference.html)
