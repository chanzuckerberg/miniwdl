# Linter Testing Framework

The miniwdl linter testing framework provides utilities to easily test custom linters with WDL code fragments. This framework is designed to make it simple to write comprehensive tests for your custom linters.

## Overview

The testing framework consists of several key functions:

- `validate_linter()` - Main function for testing linters with WDL code
- `create_test_wdl()` - Utility to generate test WDL fragments
- `assert_lint_*()` - Helper functions for making assertions about lint results

## Basic Usage

### Testing a Linter

The primary function is `validate_linter()`, which allows you to test a linter with a WDL code fragment:

```python
from WDL.Lint import validate_linter, Linter, LintSeverity, LintCategory

# Define your custom linter
class TaskNamingLinter(Linter):
    """Ensures task names follow naming conventions"""
    
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        if not obj.name.islower():
            self.add(
                obj,
                f"Task name '{obj.name}' should be lowercase",
                obj.pos,
                severity=LintSeverity.MINOR
            )

# Test with code that should trigger the linter
validate_linter(
    TaskNamingLinter,
    """
    task BadTaskName {
      command { echo "hello" }
    }
    """,
    expected_lint=["Task name 'BadTaskName' should be lowercase"]
)

# Test with code that should not trigger the linter
validate_linter(
    TaskNamingLinter,
    """
    task good_task_name {
      command { echo "hello" }
    }
    """,
    expected_lint=[]
)
```

### Using Expected Count

Instead of specifying exact messages, you can test for the number of findings:

```python
validate_linter(
    TaskNamingLinter,
    """
    task BadTask1 {
      command { echo "hello" }
    }
    
    task BadTask2 {
      command { echo "world" }
    }
    """,
    expected_count=2
)
```

## Utility Functions

### Creating Test WDL

The `create_test_wdl()` function helps generate WDL code fragments:

```python
from WDL.Lint import create_test_wdl

# Basic task
wdl_code = create_test_wdl()
# Generates:
# version 1.0
# 
# task test_task {
#   command {
#     echo 'hello'
#   }
# }

# Task with inputs and outputs
wdl_code = create_test_wdl(
    task_name="process_data",
    command="echo ~{input_file}",
    inputs={"input_file": "String"},
    outputs={"result": "String"}
)
# Generates:
# version 1.0
# 
# task process_data {
#   input {
#     String input_file
#   }
#   command {
#     echo ~{input_file}
#   }
#   output {
#     String result = stdout()
#   }
# }
```

### Assertion Helpers

The framework provides several assertion helpers for more flexible testing:

```python
from WDL.Lint import assert_lint_contains, assert_lint_count, assert_lint_severity

# Run a linter and get results
results = validate_linter(MyLinter, wdl_code)

# Assert that results contain a specific message
assert_lint_contains(results, "expected message substring")

# Assert that results have a specific count
assert_lint_count(results, 2)

# Assert that results have a specific severity
assert_lint_severity(results, LintSeverity.MAJOR)

# Filter by linter name
assert_lint_contains(results, "message", linter_name="MyLinter")
```

## Advanced Usage

### Testing Different Severities

```python
class SecurityLinter(Linter):
    category = LintCategory.SECURITY
    default_severity = LintSeverity.CRITICAL
    
    def task(self, obj):
        if "rm -rf" in str(obj.command):
            self.add(
                obj,
                "Potentially dangerous rm command detected",
                obj.pos,
                severity=LintSeverity.CRITICAL
            )

results = validate_linter(
    SecurityLinter,
    """
    task cleanup {
      command { rm -rf /tmp/data }
    }
    """,
    expected_lint=["Potentially dangerous rm command detected"]
)

# Verify the severity
assert_lint_severity(results, LintSeverity.CRITICAL)
```

### Testing Multiple Findings

```python
class MultiIssueLinter(Linter):
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        if obj.name.startswith("bad"):
            self.add(obj, f"Task name '{obj.name}' starts with 'bad'", obj.pos)
        if len(obj.name) > 20:
            self.add(obj, f"Task name '{obj.name}' is too long", obj.pos)

validate_linter(
    MultiIssueLinter,
    """
    task bad_very_long_task_name_that_exceeds_limit {
      command { echo "hello" }
    }
    """,
    expected_count=2  # Both issues should be found
)
```

### Testing with Different WDL Versions

```python
validate_linter(
    MyLinter,
    """
    task test_task {
      command { echo "hello" }
    }
    """,
    expected_lint=[],
    version="1.1"  # Use WDL version 1.1
)
```

## Integration with pytest

The testing framework works seamlessly with pytest:

```python
import pytest
from WDL.Lint import validate_linter, Linter, LintSeverity, LintCategory

class TestMyLinter:
    def test_valid_task_names(self):
        """Test that valid task names don't trigger warnings"""
        validate_linter(
            MyLinter,
            """
            task valid_name {
              command { echo "hello" }
            }
            """,
            expected_lint=[]
        )
    
    def test_invalid_task_names(self):
        """Test that invalid task names trigger warnings"""
        validate_linter(
            MyLinter,
            """
            task InvalidName {
              command { echo "hello" }
            }
            """,
            expected_lint=["Task name should be lowercase"]
        )
    
    @pytest.mark.parametrize("task_name,should_warn", [
        ("good_name", False),
        ("BadName", True),
        ("UPPERCASE", True),
        ("snake_case", False),
    ])
    def test_task_naming_patterns(self, task_name, should_warn):
        """Test various task naming patterns"""
        wdl_code = f"""
        task {task_name} {{
          command {{ echo "hello" }}
        }}
        """
        
        if should_warn:
            validate_linter(MyLinter, wdl_code, expected_count=1)
        else:
            validate_linter(MyLinter, wdl_code, expected_lint=[])
```

## Error Handling

The testing framework includes comprehensive error handling:

### Invalid Linter Classes

```python
# This will raise ValueError
validate_linter("NotAClass", "task foo { command { echo 'hello' } }")

# This will also raise ValueError
class NotALinter:
    pass

validate_linter(NotALinter, "task foo { command { echo 'hello' } }")
```

### Assertion Failures

The framework provides clear error messages when assertions fail:

```python
# If expecting lint but getting none:
# AssertionError: Expected 1 lint findings from MyLinter, but got 0: []

# If expecting no lint but getting some:
# AssertionError: Expected no lint findings from MyLinter, but got 1: ['Found issue']

# If message doesn't match:
# AssertionError: Expected lint message to contain 'expected', but got 'actual message'
```

## Best Practices

### 1. Test Both Positive and Negative Cases

Always test both cases where your linter should and shouldn't trigger:

```python
def test_my_linter():
    # Test case that should trigger the linter
    validate_linter(MyLinter, bad_wdl_code, expected_lint=["error message"])
    
    # Test case that should not trigger the linter
    validate_linter(MyLinter, good_wdl_code, expected_lint=[])
```

### 2. Use Descriptive Test Names

```python
def test_linter_flags_uppercase_task_names():
    """Test that linter flags task names with uppercase letters"""
    # ...

def test_linter_allows_lowercase_task_names():
    """Test that linter allows properly formatted lowercase task names"""
    # ...
```

### 3. Test Edge Cases

```python
def test_linter_with_empty_task():
    """Test linter behavior with minimal task definition"""
    validate_linter(
        MyLinter,
        """
        task empty {
          command { }
        }
        """,
        expected_lint=[]
    )
```

### 4. Use Parameterized Tests

```python
@pytest.mark.parametrize("wdl_code,expected_count", [
    ("task good { command { echo 'hi' } }", 0),
    ("task Bad { command { echo 'hi' } }", 1),
    ("task WORSE { command { echo 'hi' } }", 1),
])
def test_various_task_names(wdl_code, expected_count):
    validate_linter(MyLinter, wdl_code, expected_count=expected_count)
```

## API Reference

### validate_linter(linter_class, wdl_code, expected_lint=None, expected_count=None, version="1.0")

Test a linter with a WDL code fragment.

**Parameters:**
- `linter_class`: The linter class to test (must be a subclass of Linter)
- `wdl_code`: WDL code fragment as a string
- `expected_lint`: List of expected lint messages (partial matches)
- `expected_count`: Expected number of lint findings (if expected_lint not provided)
- `version`: WDL version to use (default: "1.0")

**Returns:** List of lint results

**Raises:** AssertionError if results don't match expectations, ValueError for invalid linter class

### create_test_wdl(task_name="test_task", command="echo 'hello'", inputs=None, outputs=None, version="1.0")

Generate test WDL fragments.

**Parameters:**
- `task_name`: Name of the task
- `command`: Command to execute
- `inputs`: Dictionary of input declarations {name: type}
- `outputs`: Dictionary of output declarations {name: type}
- `version`: WDL version to use

**Returns:** WDL code as a string

### assert_lint_contains(lint_results, expected_message, linter_name=None)

Assert that lint results contain a message with the expected substring.

### assert_lint_count(lint_results, expected_count, linter_name=None)

Assert that lint results have the expected count.

### assert_lint_severity(lint_results, expected_severity, linter_name=None)

Assert that lint results have the expected severity.
