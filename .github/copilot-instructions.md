You're working on the source code for `miniwdl`, the Workflow Description Language (WDL) runner and developer toolkit.

See **CONTRIBUTING.md** for an overview of the codebase and development workflow, including detailed architecture documentation.

## Development Setup

**Container environment**: You need Docker (or Podman/Singularity) access. User must have [permission to control Docker](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user).

**Setup steps**:
1. Clone with submodules: `git clone --recursive` or `git submodule update --init --recursive`
2. Install dependencies: `pip3 install '.[dev]'`
3. Test setup: `python3 -m WDL run_self_test`

**Key command**: `python3 -m WDL ...` invokes the local source tree (equivalent to `miniwdl ...` entry point)

## Code Style and Linting

Python code **must** be linted with:
- `ruff check --ignore E741 WDL` - Check for code issues
- `ruff format --line-length 100 WDL` - Format code (use `make pretty`)
- `mypy WDL` - Type checking

Run `make check` to execute all linting checks.

## Testing

**Quick iteration**: `make qtest` - Fast test suite (fails fast, skips coverage, omits slower cases)

**Full test suite**: `make test` - Complete test suite with coverage (takes several minutes)

**Targeted testing**: 
- `python3 -m unittest -f tests/test_5stdlib.py` - Run specific test file
- `python3 -m unittest -f tests.test_5stdlib.TestStdLib.test_glob` - Run specific test

See CONTRIBUTING.md for details on test suite organization (unit tests, integration tests, WDL spec examples).

## WDL Specification

For implementing WDL features, refer to the specifications:
- **WDL 1.2**: `spec/wdl-1.2/SPEC.md` (current version)
- **Changelog**: `spec/wdl-1.2/CHANGELOG.md`
- **WDL 1.1**: `spec/wdl-1.1/SPEC.md` (previous version)

## Development Tutorials

These tutorials under `docs/` demonstrate common extension patterns:
- `trace_identifiers.md` - Basic syntax tree traversal
- `wdlviz.md` - Generating graphviz diagrams from WDL source
- `add_functions.md` - Adding new functions to the standard library
- `assert.md` - Adding a new WDL language feature (parsing, type-checking, runtime execution)
