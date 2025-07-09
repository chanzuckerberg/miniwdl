# Writing Custom Linters for miniwdl

This tutorial will guide you through creating custom linters for miniwdl's pluggable linting system. By the end of this tutorial, you'll understand how to create, test, and deploy custom linters to enforce your own WDL coding standards.

## Table of Contents

1. [Understanding the Linter System](#understanding-the-linter-system)
2. [Your First Custom Linter](#your-first-custom-linter)
3. [Linter Categories and Severities](#linter-categories-and-severities)
4. [Advanced Linter Patterns](#advanced-linter-patterns)
5. [Testing Your Linters](#testing-your-linters)
6. [Deploying Custom Linters](#deploying-custom-linters)
7. [Best Practices](#best-practices)

## Understanding the Linter System

miniwdl's linting system is built around the concept of **linters** - classes that traverse the WDL Abstract Syntax Tree (AST) and report issues. Each linter:

- Extends the `WDL.Lint.Linter` base class
- Has a category (STYLE, SECURITY, PERFORMANCE, etc.)
- Has a default severity level (MINOR, MODERATE, MAJOR, CRITICAL)
- Implements methods to check different AST node types

### The AST Walker Pattern

Linters use the Walker pattern to traverse the AST. You can override methods for different node types:

- `document(self, obj)` - Called for the entire document
- `workflow(self, obj)` - Called for workflow definitions
- `task(self, obj)` - Called for task definitions
- `call(self, obj)` - Called for task/workflow calls
- `decl(self, obj)` - Called for variable declarations
- `expr(self, obj)` - Called for expressions

## Your First Custom Linter

Let's create a simple linter that enforces task naming conventions.

### Step 1: Create the Linter Class

```python
from WDL.Lint import Linter, LintSeverity, LintCategory

class TaskNamingLinter(Linter):
    """Enforces snake_case naming for tasks"""
    
    # Set the category and default severity
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        """Check task names for proper formatting"""
        task_name = obj.name
        
        # Check if name contains uppercase letters
        if not task_name.islower():
            self.add(
                obj,  # The AST node to attach the warning to
                f"Task name '{task_name}' should be lowercase",
                obj.pos,  # Source position
                severity=LintSeverity.MINOR
            )
        
        # Check if name contains spaces or special characters
        if not task_name.replace('_', '').isalnum():
            self.add(
                obj,
                f"Task name '{task_name}' should only contain letters, numbers, and underscores",
                obj.pos,
                severity=LintSeverity.MINOR
            )
```

### Step 2: Test Your Linter

```python
from WDL.Lint import test_linter

# Test with a bad task name
test_linter(
    TaskNamingLinter,
    """
    task BadTaskName {
      command { echo "hello" }
    }
    """,
    expected_lint=["Task name 'BadTaskName' should be lowercase"]
)

# Test with a good task name
test_linter(
    TaskNamingLinter,
    """
    task good_task_name {
      command { echo "hello" }
    }
    """,
    expected_lint=[]  # Should pass with no warnings
)
```

### Step 3: Use Your Linter

Save your linter to a file (e.g., `my_linters.py`) and use it:

```bash
miniwdl check --additional-linters my_linters.py:TaskNamingLinter workflow.wdl
```

## Linter Categories and Severities

### Categories

Choose the appropriate category for your linter:

- **STYLE**: Code formatting, naming conventions, cosmetic issues
- **SECURITY**: Security vulnerabilities, unsafe practices
- **PERFORMANCE**: Performance issues, inefficient patterns
- **CORRECTNESS**: Logic errors, type mismatches
- **PORTABILITY**: Platform-specific issues, compatibility problems
- **BEST_PRACTICE**: Recommended practices, maintainability
- **OTHER**: Issues that don't fit other categories

### Severities

Set appropriate severity levels:

- **MINOR**: Cosmetic issues, style violations
- **MODERATE**: Code quality issues, minor bugs
- **MAJOR**: Significant issues, potential bugs
- **CRITICAL**: Security vulnerabilities, serious bugs

```python
class SecurityLinter(Linter):
    category = LintCategory.SECURITY
    default_severity = LintSeverity.CRITICAL  # Security issues are critical
    
    def task(self, obj):
        command_str = str(obj.command).lower()
        if 'sudo' in command_str:
            self.add(
                obj,
                "Avoid using sudo in task commands",
                obj.pos,
                severity=LintSeverity.CRITICAL
            )
```

## Advanced Linter Patterns

### Pattern 1: Multi-Node Analysis

Some linters need to analyze multiple nodes or maintain state:

```python
class UnusedInputLinter(Linter):
    category = LintCategory.CORRECTNESS
    default_severity = LintSeverity.MODERATE
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.declared_inputs = set()
        self.used_inputs = set()
    
    def task(self, obj):
        # Reset state for each task
        self.declared_inputs.clear()
        self.used_inputs.clear()
        
        # Collect declared inputs
        for input_decl in obj.inputs or []:
            self.declared_inputs.add(input_decl.name)
        
        # Check command for input usage
        command_str = str(obj.command)
        for input_name in self.declared_inputs:
            if f"~{{{input_name}}}" in command_str:
                self.used_inputs.add(input_name)
        
        # Report unused inputs
        unused = self.declared_inputs - self.used_inputs
        for unused_input in unused:
            self.add(
                obj,
                f"Input '{unused_input}' is declared but never used",
                obj.pos,
                severity=LintSeverity.MODERATE
            )
```

### Pattern 2: Configuration-Based Linting

Create configurable linters:

```python
class TaskLengthLinter(Linter):
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def __init__(self, max_length=50, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_length = max_length
    
    def task(self, obj):
        if len(obj.name) > self.max_length:
            self.add(
                obj,
                f"Task name '{obj.name}' is too long ({len(obj.name)} > {self.max_length})",
                obj.pos
            )
```

### Pattern 3: Context-Aware Linting

Use document context for more sophisticated checks:

```python
class WorkflowStructureLinter(Linter):
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MINOR
    
    def workflow(self, obj):
        # Check if workflow has too many tasks
        if len(obj.body) > 20:
            self.add(
                obj,
                f"Workflow has {len(obj.body)} elements, consider breaking into smaller workflows",
                obj.pos,
                severity=LintSeverity.MODERATE
            )
        
        # Check for missing documentation
        if not obj.meta or 'description' not in obj.meta:
            self.add(
                obj,
                "Workflow should have a description in the meta section",
                obj.pos,
                severity=LintSeverity.MINOR
            )
```

## Testing Your Linters

### Basic Testing

Use the built-in testing framework:

```python
def test_my_linter():
    # Test positive case (should find issues)
    test_linter(
        MyLinter,
        """
        task bad_example {
          command { rm -rf / }
        }
        """,
        expected_lint=["Dangerous command detected"]
    )
    
    # Test negative case (should find no issues)
    test_linter(
        MyLinter,
        """
        task good_example {
          command { echo "hello" }
        }
        """,
        expected_lint=[]
    )
```

### Advanced Testing

Test with different scenarios:

```python
import pytest
from WDL.Lint import test_linter, assert_lint_count, assert_lint_severity

class TestMyLinter:
    def test_multiple_issues(self):
        """Test linter with multiple issues in one file"""
        results = test_linter(
            MyLinter,
            """
            task bad_task1 {
              command { sudo rm file }
            }
            
            task bad_task2 {
              command { dd if=/dev/zero }
            }
            """,
            expected_count=2
        )
        
        # Verify both issues are CRITICAL
        for result in results:
            assert result[4] == LintSeverity.CRITICAL
    
    @pytest.mark.parametrize("command,should_warn", [
        ("echo hello", False),
        ("sudo apt install", True),
        ("rm -rf /tmp", True),
        ("cat file.txt", False),
    ])
    def test_command_patterns(self, command, should_warn):
        """Test various command patterns"""
        wdl_code = f"""
        task test_task {{
          command {{ {command} }}
        }}
        """
        
        if should_warn:
            test_linter(MyLinter, wdl_code, expected_count=1)
        else:
            test_linter(MyLinter, wdl_code, expected_lint=[])
```

## Deploying Custom Linters

### Method 1: File-Based Linters

Save your linters to a Python file:

```python
# my_custom_linters.py
from WDL.Lint import Linter, LintSeverity, LintCategory

class MyLinter1(Linter):
    # ... implementation

class MyLinter2(Linter):
    # ... implementation
```

Use with command line:
```bash
miniwdl check --additional-linters my_custom_linters.py:MyLinter1,my_custom_linters.py:MyLinter2 workflow.wdl
```

### Method 2: Package-Based Linters

Create a Python package with entry points:

```python
# setup.py
from setuptools import setup

setup(
    name="my-wdl-linters",
    version="1.0.0",
    packages=["my_wdl_linters"],
    entry_points={
        "miniwdl.linters": [
            "my_linter1 = my_wdl_linters:MyLinter1",
            "my_linter2 = my_wdl_linters:MyLinter2",
        ]
    }
)
```

After installation, linters are automatically discovered.

### Method 3: Configuration File

Add to your miniwdl configuration:

```ini
[linting]
additional_linters = ["my_custom_linters.py:MyLinter1", "my_custom_linters.py:MyLinter2"]
disabled_linters = ["StringCoercion"]
enabled_categories = ["STYLE", "SECURITY", "PERFORMANCE"]
exit_on_severity = "MAJOR"
```

## Best Practices

### 1. Clear and Actionable Messages

```python
# Good: Specific and actionable
self.add(obj, "Task name 'MyTask' should be lowercase: 'my_task'", obj.pos)

# Bad: Vague and unhelpful
self.add(obj, "Bad task name", obj.pos)
```

### 2. Appropriate Severity Levels

```python
# Style issues: MINOR
if not name.islower():
    self.add(obj, "Use lowercase names", obj.pos, severity=LintSeverity.MINOR)

# Security issues: CRITICAL
if "sudo" in command:
    self.add(obj, "Avoid sudo", obj.pos, severity=LintSeverity.CRITICAL)
```

### 3. Performance Considerations

```python
# Cache expensive operations
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.dangerous_commands = {"rm -rf", "dd if=", "mkfs"}

def task(self, obj):
    command_str = str(obj.command).lower()
    for dangerous_cmd in self.dangerous_commands:
        if dangerous_cmd in command_str:
            # Report issue
            break
```

### 4. Comprehensive Testing

```python
def test_edge_cases():
    # Test empty tasks
    test_linter(MyLinter, "task empty { command { } }", expected_lint=[])
    
    # Test complex commands
    test_linter(MyLinter, complex_wdl, expected_count=3)
    
    # Test with different WDL versions
    test_linter(MyLinter, wdl_code, expected_lint=[], version="1.1")
```

### 5. Documentation

Document your linters well:

```python
class MyLinter(Linter):
    """
    Enforces secure command practices in WDL tasks.
    
    This linter checks for:
    - Use of sudo or su commands
    - Potentially dangerous file operations
    - Hardcoded credentials in commands
    
    Category: SECURITY
    Default Severity: CRITICAL
    
    Examples:
        # Bad
        task unsafe {
          command { sudo rm -rf /data }
        }
        
        # Good
        task safe {
          command { echo "Processing data" }
        }
    """
    
    category = LintCategory.SECURITY
    default_severity = LintSeverity.CRITICAL
```

## Common Linter Examples

Here are some common linter patterns you might want to implement:

### Style Linters
- Task/workflow naming conventions
- Indentation and formatting
- Comment requirements
- Variable naming patterns

### Security Linters
- Dangerous command detection
- Credential scanning
- File permission checks
- Network access validation

### Performance Linters
- Resource allocation checks
- Inefficient command patterns
- Large file handling
- Memory usage optimization

### Best Practice Linters
- Documentation requirements
- Error handling patterns
- Input validation
- Output organization

## Conclusion

You now have the knowledge to create powerful custom linters for miniwdl. Remember to:

1. Start simple and iterate
2. Test thoroughly with various scenarios
3. Use appropriate categories and severities
4. Provide clear, actionable messages
5. Document your linters well

For more advanced examples and patterns, see the `examples/linter_testing_example.py` file in the miniwdl repository.

Happy linting!
