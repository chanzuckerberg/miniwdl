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

---

# Adding WDL 1.2 Standard Library Functions

This guide provides comprehensive instructions for adding new standard library functions from WDL 1.2. The focus is on the several new functions still needing implementation:

**String functions:** `find`, `matches`
**File functions:** `join_paths`
**Array functions:** `contains`, `chunk`
**Map functions:** `contains_key`, `values`
**Operators:** `**` (exponentiation)

## Quick Reference

- **Source code:** `WDL/StdLib.py`
- **Unit tests:** `tests/test_5stdlib.py`
- **Spec tests:** `tests/spec_tests/` (extracted from WDL specification)
- **Tutorial:** `docs/add_functions.md`
- **WDL 1.2 spec:** `spec/wdl-1.2/SPEC.md` (stdlib starts at line 7375)
- **Changelog:** `spec/wdl-1.2/CHANGELOG.md`

## Implementation Process

### 1. Review the Specification

Start by reading the function's definition in `spec/wdl-1.2/SPEC.md`:
- Signature (parameter types, return type)
- Behavior and semantics
- Example WDL code with expected inputs/outputs
- Note: **The spec has many bugs!** Check `tests/spec_tests/config.yaml` for known issues marked as `xfail`

### 2. Choose Implementation Approach

Functions in `WDL/StdLib.py` fall into three categories:

#### a) Static Functions (simple, fixed types)
Use the `@static()` decorator for functions with fixed argument and return types:

```python
@static([Type.Float()], Type.Int())
def floor(v: Value.Float) -> Value.Int:
    return Value.Int(math.floor(v.value))
```

Key points:
- First argument to `@static()` is list of parameter types
- Second argument is return type
- Implementation receives `Value.*` objects, must return `Value.*` objects
- Include PEP 484 type hints for Python types
- Handle type coercion automatically via the decorator

#### b) Polymorphic Functions (varying types, simple logic)
Use `StaticFunction` class when types vary but type-checking logic is straightforward:

```python
self.min = _ArithmeticOperator("min", lambda l, r: min(l, r))
```

#### c) Complex Polymorphic Functions (custom type inference)
Subclass `EagerFunction` when you need custom type inference:

```python
class _MyFunction(EagerFunction):
    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        # Validate arguments and determine return type
        # Raise Error.WrongArity, Error.StaticTypeMismatch, etc.
        return return_type

    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        # Implement runtime evaluation
        # arguments are already evaluated
        # Raise Error.EvalError for runtime failures
        return result_value
```

Then instantiate in `Base.__init__`:
```python
self.my_function = _MyFunction()
```

### 3. Version Gating

WDL 1.2 functions should only be available in version 1.2+:

```python
if self.wdl_version not in ["draft-2", "1.0"]:
    # WDL 1.1+ functions
    self.min = ...

# For WDL 1.2+ (assuming version string convention continues):
if self.wdl_version not in ["draft-2", "1.0", "1.1"]:
    # WDL 1.2+ functions
    self.find = ...
```

Check existing version gating patterns around line 141 in `StdLib.py`.

### 4. Error Handling

Use appropriate error types from `WDL.Error`:

**Static (type-checking) errors:**
- `Error.WrongArity(expr, expected_count)` - wrong number of arguments
- `Error.StaticTypeMismatch(expr, expected_type, actual_type, context)` - type doesn't match
- `Error.IndeterminateType(expr, message)` - can't infer type (e.g., empty array)

**Runtime errors:**
- `Error.EvalError(expr, message)` - runtime evaluation failures
- `Error.InputError(message)` - invalid input data
- `Error.OutOfBounds(expr, message)` - index out of range, key not found

### 5. File System Access

Functions that read/write files must use:
- `self._devirtualize_filename(filename)` - convert WDL File path to local path for reading
- `self._virtualize_filename(filename)` - convert local path to WDL File value for output
- `self._write_dir` - directory for creating temporary files

Example:
```python
@static([Type.File()], Type.String())
def my_read_func(file: Value.File) -> Value.String:
    with open(self._devirtualize_filename(file.value), "r") as f:
        content = f.read()
    return Value.String(content)
```

### 6. Testing Strategy

#### a) Unit Tests (`tests/test_5stdlib.py`)

Add tests using the `_test_task()` helper which:
- Parses WDL task source
- Type-checks the document
- Executes in a Docker container
- Returns outputs as JSON

```python
def test_my_function(self):
    # Basic functionality
    outputs = self._test_task(R"""
    version 1.2
    task test_my_function {
        command {}
        output {
            String result = my_function("input")
        }
    }
    """)
    self.assertEqual(outputs["result"], "expected_value")

    # Error cases
    self._test_task(R"""
    version 1.2
    task test_error {
        command {}
        output {
            String result = my_function(42)  # wrong type
        }
    }
    """, expected_exception=WDL.Error.StaticTypeMismatch)
```

Test coverage should include:
- Basic functionality with typical inputs
- Edge cases (empty arrays, null values, boundary conditions)
- Type coercion scenarios
- Error conditions (wrong arity, type mismatches, runtime errors)
- Optional parameters if applicable
- Version gating (ensure unavailable in WDL 1.0/1.1)

#### b) Spec Tests

The spec tests in `tests/spec_tests/` are auto-extracted from the WDL specification. Check:
1. `tests/spec_tests/config.yaml` for the function's test status
2. Many tests are marked `xfail` due to **bugs in the spec itself**
3. When your implementation is ready, remove the test from the `xfail` list
4. Document any spec bugs you discover in comments in `config.yaml`

Common spec bugs to watch for:
- Wrong expected output values
- Typos in test code
- Missing struct/type definitions
- Incorrect syntax
- Python-style ternary instead of WDL ternary
