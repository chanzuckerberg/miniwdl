# Common Linter Patterns for miniwdl

This document provides a collection of common linter patterns and best practices for creating effective WDL linters. These patterns can be used as starting points for your own custom linters.

## Table of Contents

1. [Style and Formatting Linters](#style-and-formatting-linters)
2. [Security Linters](#security-linters)
3. [Performance Linters](#performance-linters)
4. [Best Practice Linters](#best-practice-linters)
5. [Correctness Linters](#correctness-linters)
6. [Advanced Patterns](#advanced-patterns)

## Style and Formatting Linters

### Task Naming Convention Linter

Enforces consistent task naming patterns:

```python
from WDL.Lint import Linter, LintSeverity, LintCategory
import re

class TaskNamingLinter(Linter):
    """Enforces snake_case naming for tasks"""
    
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.snake_case_pattern = re.compile(r'^[a-z][a-z0-9_]*$')
    
    def task(self, obj):
        if not self.snake_case_pattern.match(obj.name):
            self.add(
                obj,
                f"Task name '{obj.name}' should use snake_case (lowercase with underscores)",
                obj.pos
            )
```

### Indentation Consistency Linter

Checks for consistent indentation in command blocks:

```python
class IndentationLinter(Linter):
    """Enforces consistent indentation in command blocks"""
    
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        if obj.command:
            command_lines = str(obj.command).split('\n')
            indentations = []
            
            for line in command_lines:
                if line.strip():  # Skip empty lines
                    indent = len(line) - len(line.lstrip())
                    if indent > 0:
                        indentations.append(indent)
            
            # Check if all indentations are consistent
            if len(set(indentations)) > 2:  # Allow for nested indentation
                self.add(
                    obj,
                    "Inconsistent indentation in command block",
                    obj.pos
                )
```

### Documentation Requirement Linter

Ensures tasks and workflows have proper documentation:

```python
class DocumentationLinter(Linter):
    """Requires documentation for tasks and workflows"""
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        if not obj.meta or 'description' not in obj.meta:
            self.add(
                obj,
                f"Task '{obj.name}' should have a description in the meta section",
                obj.pos
            )
    
    def workflow(self, obj):
        if not obj.meta or 'description' not in obj.meta:
            self.add(
                obj,
                f"Workflow '{obj.name}' should have a description in the meta section",
                obj.pos
            )
```

## Security Linters

### Dangerous Command Linter

Detects potentially dangerous commands:

```python
class DangerousCommandLinter(Linter):
    """Detects potentially dangerous commands in tasks"""
    
    category = LintCategory.SECURITY
    default_severity = LintSeverity.MAJOR
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dangerous_patterns = {
            'rm -rf': 'Recursive file deletion can be dangerous',
            'dd if=': 'Direct disk operations can be destructive',
            'mkfs': 'Filesystem creation can destroy data',
            'fdisk': 'Disk partitioning can be destructive',
            'sudo': 'Privilege escalation should be avoided',
            'su ': 'User switching should be avoided',
        }
    
    def task(self, obj):
        command_str = str(obj.command).lower()
        
        for pattern, message in self.dangerous_patterns.items():
            if pattern in command_str:
                severity = LintSeverity.CRITICAL if pattern in ['sudo', 'su '] else LintSeverity.MAJOR
                self.add(
                    obj,
                    f"Potentially dangerous command detected: {pattern}. {message}",
                    obj.pos,
                    severity=severity
                )
```

### Credential Scanner Linter

Scans for hardcoded credentials:

```python
class CredentialScannerLinter(Linter):
    """Scans for hardcoded credentials in commands"""
    
    category = LintCategory.SECURITY
    default_severity = LintSeverity.CRITICAL
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.credential_patterns = [
            r'password\s*=\s*["\']?[^"\'\s]+',
            r'passwd\s*=\s*["\']?[^"\'\s]+',
            r'secret\s*=\s*["\']?[^"\'\s]+',
            r'token\s*=\s*["\']?[^"\'\s]+',
            r'api[_-]?key\s*=\s*["\']?[^"\'\s]+',
            r'-p\s+[^-\s][^\s]*',  # -p password
        ]
        self.compiled_patterns = [re.compile(p, re.IGNORECASE) for p in self.credential_patterns]
    
    def task(self, obj):
        command_str = str(obj.command)
        
        for pattern in self.compiled_patterns:
            if pattern.search(command_str):
                self.add(
                    obj,
                    "Potential hardcoded credential detected in command",
                    obj.pos,
                    severity=LintSeverity.CRITICAL
                )
                break  # Only report once per task
```

### Network Access Linter

Flags tasks that make network requests:

```python
class NetworkAccessLinter(Linter):
    """Flags tasks that make network requests"""
    
    category = LintCategory.SECURITY
    default_severity = LintSeverity.MODERATE
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.network_commands = ['curl', 'wget', 'nc', 'netcat', 'ssh', 'scp', 'rsync']
    
    def task(self, obj):
        command_str = str(obj.command).lower()
        
        for net_cmd in self.network_commands:
            if f' {net_cmd} ' in f' {command_str} ' or command_str.startswith(f'{net_cmd} '):
                self.add(
                    obj,
                    f"Task uses network command '{net_cmd}' - ensure network access is intended",
                    obj.pos,
                    severity=LintSeverity.MODERATE
                )
```

## Performance Linters

### Resource Allocation Linter

Checks for appropriate resource allocation:

```python
class ResourceAllocationLinter(Linter):
    """Checks for appropriate resource allocation"""
    
    category = LintCategory.PERFORMANCE
    default_severity = LintSeverity.MODERATE
    
    def task(self, obj):
        runtime_attrs = obj.runtime or {}
        
        # Check for missing memory specification
        if 'memory' not in runtime_attrs:
            self.add(
                obj,
                f"Task '{obj.name}' should specify memory requirements",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        
        # Check for missing CPU specification
        if 'cpu' not in runtime_attrs:
            self.add(
                obj,
                f"Task '{obj.name}' should specify CPU requirements",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        
        # Check for excessive resource requests
        if 'memory' in runtime_attrs:
            memory_str = str(runtime_attrs['memory']).lower()
            if 'gb' in memory_str:
                try:
                    memory_gb = float(memory_str.replace('gb', '').strip())
                    if memory_gb > 100:
                        self.add(
                            obj,
                            f"Task '{obj.name}' requests {memory_gb}GB memory - verify this is necessary",
                            obj.pos,
                            severity=LintSeverity.MODERATE
                        )
                except ValueError:
                    pass
```

### Inefficient Command Linter

Detects inefficient command patterns:

```python
class InefficientCommandLinter(Linter):
    """Detects inefficient command patterns"""
    
    category = LintCategory.PERFORMANCE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        command_str = str(obj.command)
        
        # Check for inefficient file operations
        if 'cat' in command_str and '|' in command_str:
            if re.search(r'cat\s+[^|]+\|\s*head', command_str):
                self.add(
                    obj,
                    "Consider using 'head' directly instead of 'cat | head'",
                    obj.pos
                )
            elif re.search(r'cat\s+[^|]+\|\s*tail', command_str):
                self.add(
                    obj,
                    "Consider using 'tail' directly instead of 'cat | tail'",
                    obj.pos
                )
        
        # Check for unnecessary use of grep
        if re.search(r'cat\s+[^|]+\|\s*grep', command_str):
            self.add(
                obj,
                "Consider using 'grep' directly on the file instead of 'cat | grep'",
                obj.pos
            )
```

## Best Practice Linters

### Input Validation Linter

Ensures proper input validation:

```python
class InputValidationLinter(Linter):
    """Ensures proper input validation"""
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MODERATE
    
    def task(self, obj):
        if not obj.inputs:
            return
        
        command_str = str(obj.command)
        
        for input_decl in obj.inputs:
            input_name = input_decl.name
            input_type = str(input_decl.type)
            
            # Check if File inputs are validated
            if 'File' in input_type and f"~{{{input_name}}}" in command_str:
                # Look for basic file existence checks
                if f"[ -f ~{{{input_name}}} ]" not in command_str and \
                   f"test -f ~{{{input_name}}}" not in command_str:
                    self.add(
                        obj,
                        f"Consider validating file input '{input_name}' exists before use",
                        obj.pos,
                        severity=LintSeverity.MINOR
                    )
```

### Error Handling Linter

Checks for proper error handling:

```python
class ErrorHandlingLinter(Linter):
    """Checks for proper error handling in commands"""
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MODERATE
    
    def task(self, obj):
        command_str = str(obj.command)
        
        # Check for commands that might fail silently
        risky_commands = ['curl', 'wget', 'scp', 'rsync']
        
        for cmd in risky_commands:
            if cmd in command_str.lower():
                # Check if there's error handling
                if 'set -e' not in command_str and \
                   '|| exit 1' not in command_str and \
                   '&& ' not in command_str:
                    self.add(
                        obj,
                        f"Command using '{cmd}' should include error handling (set -e, ||, or &&)",
                        obj.pos
                    )
                    break
```

### Output Organization Linter

Ensures outputs are properly organized:

```python
class OutputOrganizationLinter(Linter):
    """Ensures outputs are properly organized"""
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        if not obj.outputs:
            self.add(
                obj,
                f"Task '{obj.name}' should define at least one output",
                obj.pos
            )
            return
        
        # Check for descriptive output names
        for output_decl in obj.outputs:
            output_name = output_decl.name
            
            if len(output_name) < 3:
                self.add(
                    obj,
                    f"Output name '{output_name}' should be more descriptive",
                    obj.pos
                )
            
            # Check for generic names
            generic_names = ['out', 'output', 'result', 'file']
            if output_name.lower() in generic_names:
                self.add(
                    obj,
                    f"Output name '{output_name}' is too generic, use a more specific name",
                    obj.pos
                )
```

## Correctness Linters

### Type Consistency Linter

Checks for type consistency issues:

```python
class TypeConsistencyLinter(Linter):
    """Checks for type consistency issues"""
    
    category = LintCategory.CORRECTNESS
    default_severity = LintSeverity.MAJOR
    
    def call(self, obj):
        if not obj.inputs:
            return
        
        # Check for potential type mismatches in call inputs
        for input_name, input_expr in obj.inputs.items():
            if hasattr(input_expr, 'type') and hasattr(obj.callee, 'available_inputs'):
                expected_input = obj.callee.available_inputs.get(input_name)
                if expected_input:
                    expected_type = str(expected_input.type)
                    actual_type = str(input_expr.type)
                    
                    # Simple type mismatch check
                    if 'String' in expected_type and 'Int' in actual_type:
                        self.add(
                            obj,
                            f"Potential type mismatch: passing {actual_type} to {expected_type} parameter '{input_name}'",
                            obj.pos,
                            severity=LintSeverity.MODERATE
                        )
```

### Unused Variable Linter

Detects unused variables:

```python
class UnusedVariableLinter(Linter):
    """Detects unused variables in workflows"""
    
    category = LintCategory.CORRECTNESS
    default_severity = LintSeverity.MINOR
    
    def workflow(self, obj):
        declared_vars = set()
        used_vars = set()
        
        # Collect all declared variables
        for element in obj.body:
            if hasattr(element, 'name'):
                declared_vars.add(element.name)
        
        # Collect all used variables (simplified)
        workflow_str = str(obj)
        for var_name in declared_vars:
            if var_name in workflow_str:
                # Count occurrences (declaration + usage)
                occurrences = workflow_str.count(var_name)
                if occurrences > 1:  # More than just declaration
                    used_vars.add(var_name)
        
        # Report unused variables
        unused_vars = declared_vars - used_vars
        for unused_var in unused_vars:
            self.add(
                obj,
                f"Variable '{unused_var}' is declared but never used",
                obj.pos
            )
```

## Advanced Patterns

### Multi-Pass Linter

Some linters need multiple passes to collect and analyze information:

```python
class DependencyLinter(Linter):
    """Analyzes task dependencies and call patterns"""
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MODERATE
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tasks = {}
        self.calls = []
    
    def task(self, obj):
        """First pass: collect all tasks"""
        self.tasks[obj.name] = obj
    
    def call(self, obj):
        """Second pass: collect all calls"""
        self.calls.append(obj)
    
    def workflow(self, obj):
        """Third pass: analyze dependencies"""
        # Analyze call patterns after collecting all information
        task_call_count = {}
        
        for call in self.calls:
            task_name = call.task
            task_call_count[task_name] = task_call_count.get(task_name, 0) + 1
        
        # Report tasks that are never called
        for task_name, task_obj in self.tasks.items():
            if task_name not in task_call_count:
                self.add(
                    task_obj,
                    f"Task '{task_name}' is defined but never called",
                    task_obj.pos
                )
```

### Configurable Linter

Create linters that can be configured:

```python
class ConfigurableNamingLinter(Linter):
    """Configurable naming convention linter"""
    
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def __init__(self, naming_style='snake_case', max_length=50, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.naming_style = naming_style
        self.max_length = max_length
        
        if naming_style == 'snake_case':
            self.pattern = re.compile(r'^[a-z][a-z0-9_]*$')
        elif naming_style == 'camelCase':
            self.pattern = re.compile(r'^[a-z][a-zA-Z0-9]*$')
        elif naming_style == 'PascalCase':
            self.pattern = re.compile(r'^[A-Z][a-zA-Z0-9]*$')
        else:
            raise ValueError(f"Unsupported naming style: {naming_style}")
    
    def task(self, obj):
        name = obj.name
        
        if not self.pattern.match(name):
            self.add(
                obj,
                f"Task name '{name}' should follow {self.naming_style} convention",
                obj.pos
            )
        
        if len(name) > self.max_length:
            self.add(
                obj,
                f"Task name '{name}' exceeds maximum length of {self.max_length}",
                obj.pos
            )
```

### Context-Aware Linter

Use document context for sophisticated analysis:

```python
class WorkflowComplexityLinter(Linter):
    """Analyzes workflow complexity"""
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MODERATE
    
    def workflow(self, obj):
        complexity_score = 0
        
        # Count different types of complexity
        for element in obj.body:
            if hasattr(element, 'scatter'):
                complexity_score += 2  # Scatter adds complexity
            elif hasattr(element, 'condition'):
                complexity_score += 2  # Conditionals add complexity
            else:
                complexity_score += 1  # Regular calls
        
        # Report high complexity
        if complexity_score > 15:
            self.add(
                obj,
                f"Workflow complexity score is {complexity_score} (>15). Consider breaking into smaller workflows.",
                obj.pos,
                severity=LintSeverity.MODERATE
            )
        elif complexity_score > 25:
            self.add(
                obj,
                f"Workflow complexity score is {complexity_score} (>25). This workflow is very complex.",
                obj.pos,
                severity=LintSeverity.MAJOR
            )
```

## Usage Examples

### Using Multiple Linters

```python
# my_linters.py
from WDL.Lint import Linter, LintSeverity, LintCategory

# Include multiple linter classes
class StyleLinter(Linter):
    # ... implementation

class SecurityLinter(Linter):
    # ... implementation

class PerformanceLinter(Linter):
    # ... implementation
```

```bash
# Use all linters from the file
miniwdl check --additional-linters my_linters.py:StyleLinter,my_linters.py:SecurityLinter,my_linters.py:PerformanceLinter workflow.wdl
```

### Configuration-Based Usage

```ini
# .miniwdl.cfg
[linting]
additional_linters = [
    "my_linters.py:StyleLinter",
    "my_linters.py:SecurityLinter"
]
disabled_linters = ["StringCoercion"]
enabled_categories = ["STYLE", "SECURITY", "PERFORMANCE"]
exit_on_severity = "MAJOR"
```

These patterns provide a solid foundation for creating effective WDL linters. Combine and modify them to suit your specific needs and coding standards.
