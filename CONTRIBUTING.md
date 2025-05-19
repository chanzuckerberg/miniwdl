# Contributing to miniwdl

Feedback and contributions to miniwdl are welcome, via issues and pull requests on this repository.

* [Online documentation](https://miniwdl.readthedocs.io/en/latest/) includes several "codelab" tutorials to start with
* The [Project board](https://github.com/chanzuckerberg/miniwdl/projects/1) shows our current prioritization of [issues](https://github.com/chanzuckerberg/miniwdl/issues)
* [Starter issues](https://github.com/chanzuckerberg/miniwdl/issues?q=is%3Aopen+is%3Aissue+label%3Astarter) are suitable entry points for new contributors
* [Pull request template](https://github.com/chanzuckerberg/miniwdl/blob/main/.github/pull_request_template.md) includes a preparation checklist

## Development environment

To set up your Linux development environment,

1. `git clone --recursive` this repository or your fork thereof, and `cd` into it
2. Install OS packages as illustrated in the [Dockerfile](https://github.com/chanzuckerberg/miniwdl/blob/main/Dockerfile) (OS packages) and PyPI dependencies with `pip3 install '.[dev]'`
3. Invoking user must have [permission to control Docker](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user)
4. Try `python3 -m WDL run_self_test` to test the configuration

Generally, `python3 -m WDL ...` invokes the equivalent of the `miniwdl ...` entry point for the local source tree. Another option is to `pip3 install .` to install the `miniwdl` entry point with the current code revision.

The Makefile has a few typical flows:

- `make` or `make test` runs the full test suite with code coverage report (takes several minutes)
- `make qtest` runs most of the tests more quickly (by omitting some slower cases, not tracking coverage, and failing fast)
- `make pretty` reformats the code with `ruff format`
- `make check` validates the code with `ruff check` and `mypy`

To quickly run only a relevant subset of the tests, you can e.g. `python3 -m unittest -f tests/test_5stdlib.py` or `python3 -m unittest -f tests.test_5stdlib.TestStdLib.test_glob`.

**macOS:** isn't preferred for miniwdl development due to some [test suite incompatibilities](https://github.com/chanzuckerberg/miniwdl/issues/145); but at least simple changes can be prototyped under macOS.

Read the Docs currently builds from the [mlin/miniwdl](https://github.com/mlin/miniwdl) fork of this repository. Locally, `make doc` generates the docs under `docs/_build/html/`.

## Releases, tags, and branches

Release version numbers follow the semantic versioning convention (vA.B.C; A=Major B=Minor C=Patch). Each release is created from a git tag of the version number, then published through GitHub, PyPI, and conda-forge.

The repository has the following principal branches:

* **main** is the trunk, with all diffs pending for the upcoming mainline release
* **release** should be set to the most up-to-date mainline release
* **vA.B.x** track older release lineages, kept in case of need to release patched versions thereof

## Security

Please disclose security issues responsibly by contacting security@chanzuckerberg.com.

## Codebase Overview (AI-generated)

miniwdl is structured around a few key components:

1.  **Parser (`WDL/_parser.py`, `WDL/_grammar.py`):** This component is responsible for parsing WDL source code into an Abstract Syntax Tree (AST). It uses the Lark parsing library. The grammar is versioned to support different WDL specifications.
1.  **Type System (`WDL/Type.py`, `WDL/Value.py`):** WDL has a static type system, implemented in these modules.  `Type.py` defines classes for WDL types (e.g., `Type.Int`, `Type.Array`), and `Value.py` represents WDL values at runtime.
1.  **Environment (`WDL/Env.py`):**  This module provides a data structure for managing identifier bindings during WDL evaluation. It's essentially a stack of namespaces.
1.  **Expression Evaluation (`WDL/Expr.py`):** Handles the evaluation of WDL expressions within the context of an environment. It uses the type system to ensure type safety during evaluation.
1.  **Abstract Syntax Tree (`WDL/Tree.py`):** Defines the classes representing the various elements of the WDL AST (e.g., `Task`, `Workflow`, `Call`, `Scatter`, `Decl`). These classes include methods for type-checking and evaluation.
1.  **Standard Library (`WDL/StdLib.py`):** Implements the WDL standard library functions.
9. **Error Handling (`WDL/Error.py`):** Defines custom exception classes used throughout the codebase.
1.  **Runtime (`WDL/runtime/`):** The core execution engine.  It includes logic for task scheduling, resource management, input/output handling, and containerization.  Key submodules here include:
    *   `task.py`: Handles execution of individual WDL tasks.
    *   `workflow.py`: Manages workflow-level execution, including scatter and conditional sections.
    *   `backend/`:  Contains different containerization backends (Docker Swarm, Singularity, Podman, udocker).
    *   `cache.py`: Checks if the outputs of a task call, given its specific inputs, have already been computed; if so, reuse the cached outputs instead of re-running the task.
    *   `download.py`: Handles downloading input files from URIs.
1.  **Command Line Interface (`WDL/CLI.py`, `WDL/__main__.py`):** Provides the command-line interface for interacting with miniwdl (e.g., `miniwdl run`, `miniwdl check`).

### Code Structure (Detailed)

This section provides a more in-depth look at the individual files.

#### `WDL/`

This directory contains the core WDL parsing, type-checking, and evaluation logic.

*   **`__init__.py`:**  Provides the core `WDL.load()` function for parsing and type-checking a WDL document, as well as functions for converting between JSON and WDL values.
*   **`_grammar.py`:**  Defines the WDL grammar using Lark's EBNF syntax.  It includes grammar definitions for different WDL versions.
*   **`_parser.py`:**  Contains the Lark transformer classes that convert the parsed syntax tree into the WDL AST (defined in `Tree.py`).
*   **`_util.py`:** Utility functions used across the codebase.  Includes functions for logging, file system operations, and handling termination signals.
*   **`CLI.py`:**  Implements the command-line interface, including argument parsing and dispatching to the appropriate functionality (e.g., `check`, `run`, `run_self_test`).
*   **`Env.py`:**  Defines the `Bindings` and `Binding` classes for representing environments (namespaces) during type checking and evaluation.
*   **`Error.py`:**  Defines custom exception classes for various error conditions (syntax errors, type errors, runtime errors, etc.).
*   **`Expr.py`:** Defines the AST classes for WDL expressions (literals, operators, function calls, identifiers, etc.). Includes methods for type inference and evaluation.
*   **`Lint.py`:** Implements linting rules to detect potential issues in WDL code.
*   **`StdLib.py`:** Implements the WDL standard library functions.
*   **`Tree.py`:** Defines the AST classes for WDL documents, tasks, workflows, and their components (declarations, calls, scatter/conditional sections, etc.). Includes methods for type checking and other static analysis.
*   **`Type.py`:**  Defines the classes for WDL types (Int, String, Array, Map, Pair, Struct, etc.).
*   **`Value.py`:** Defines the classes for WDL values (corresponding to the types defined in `Type.py`).
*   **`Walker.py`:**  Provides helper classes for traversing the WDL AST, useful for implementing linters and other analyses.
*   **`Zip.py`:** Provides methods for creating zip archive from a main WDL file and any imported files.

#### `WDL/runtime/`

This directory contains the runtime implementation for executing WDL tasks and workflows.

*   **`__init__.py`:** Provides the `run()` function, the main entry point for executing a WDL task or workflow.
*   **`_statusbar.py`:**  Implements the status bar displayed during workflow execution (showing progress, resource usage, etc.).
*   **`backend/`:**  This subdirectory contains implementations of different containerization backends.
    *   **`cli_subprocess.py`:**  Abstract base class for container backends that use command-line subprocesses (Docker Swarm, Singularity, Podman, udocker).
    *   **`docker_swarm.py`:**  Implementation using Docker Swarm.
    *   **`podman.py`:** Implementation using Podman.
    *   **`singularity.py`:** Implementation using Singularity.
    *   **`udocker.py`:** Implementation using udocker.
*   **`cache.py`:**  Implements call caching logic.
*   **`config.py`:** Handles loading and parsing of the configuration file (`miniwdl.cfg`).
*   **`config_templates/`:**  Contains the default configuration file (`default.cfg`).
*   **`download.py`:** Implements logic for downloading input files from URIs, including support for pluggable downloaders.
*   **`error.py`:**  Defines runtime-specific exception classes.
*   **`task_container.py`:**  Defines the abstract base class for task container implementations.
*   **`task.py`:**  Implements the logic for running individual WDL tasks, including setting up the container environment, copying input files, and executing the command.
*   **`workflow.py`:** Implements the logic for running WDL workflows, including managing the workflow state machine, scheduling tasks, and handling scatter/conditional sections.

### Test Suite

The miniwdl test suite is located in the `tests/` directory and is organized into several categories:

*   **Unit Tests (test_\*.py):** These Python files use the `unittest` framework to test individual components of miniwdl, such as the parser, type checker, expression evaluator, standard library functions, and runtime.  These include:
    - true unit tests of isolated components
    - artificial WDL sources exercising various aspects of the runtime
    - tests of the parser and linter on a collection of WDL from external sources found under `test_corpi/`
*   **WDL Spec Examples (spec_tests/):** Tests all the examples embedded in the WDL SPEC.md (extracted following the [WDL Markdown Test Specification](https://github.com/openwdl/wdl-tests/blob/main/docs/MarkdownTests.md)). The `config.yaml` file marks several examples xfail or skip, for various reasons commented there.
*   **Bash-TAP Tests (\*.t):**  These files are shell scripts that use the `bash-tap` framework to run commands and check their output.  They primarily exercise the `miniwdl` command-line interface (parsing arguments, executing workflows/tasks, etc.) and associated shell integration.
*   **Applied Tests (applied/):** These are bash-tap that run `miniwdl run` on complete WDL workflows, drawn from various sources, with specific input JSON files.  These tests are used to ensure that miniwdl correctly executes realistic workflows.
