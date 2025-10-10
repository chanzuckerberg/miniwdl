# Linter Configuration Guide

This guide covers all the ways to configure miniwdl's pluggable linting system, from command-line options to configuration files and environment variables.

## Table of Contents

1. [Configuration Methods](#configuration-methods)
2. [Command Line Options](#command-line-options)
3. [Configuration Files](#configuration-files)
4. [Environment Variables](#environment-variables)
5. [Priority Order](#priority-order)
6. [Complete Examples](#complete-examples)

## Configuration Methods

miniwdl's linting system can be configured through three methods:

1. **Command Line Arguments** - Highest priority, overrides all other settings
2. **Environment Variables** - Medium priority, overrides configuration files
3. **Configuration Files** - Lowest priority, provides defaults

## Command Line Options

### Basic Linting Options

```bash
# Basic linting (uses built-in linters only)
miniwdl check workflow.wdl

# Strict mode (exit with error on any lint findings)
miniwdl check --strict workflow.wdl

# Show all lint findings (including suppressed ones)
miniwdl check --no-suppress workflow.wdl
```

### Adding Custom Linters

```bash
# Add linters from a Python file
miniwdl check --additional-linters my_linters.py:MyLinter workflow.wdl

# Add multiple linters
miniwdl check --additional-linters my_linters.py:Linter1,my_linters.py:Linter2 workflow.wdl

# Mix file and module linters
miniwdl check --additional-linters my_linters.py:FileLinter,my_module:ModuleLinter workflow.wdl
```

### Disabling Linters

```bash
# Disable specific built-in linters
miniwdl check --disable-linters StringCoercion,FileCoercion workflow.wdl

# Disable multiple linters
miniwdl check --disable-linters UnnecessaryQuantifier,MixedIndentation workflow.wdl
```

### Category-Based Filtering

```bash
# Enable only specific categories
miniwdl check --enable-lint-categories STYLE,SECURITY workflow.wdl

# Disable specific categories
miniwdl check --disable-lint-categories PERFORMANCE,PORTABILITY workflow.wdl

# Combine category filtering with custom linters
miniwdl check --additional-linters my_linters.py:MyLinter --enable-lint-categories STYLE,SECURITY workflow.wdl
```

### Exit Code Control

```bash
# Exit with error on findings of MAJOR severity or higher
miniwdl check --exit-on-lint-severity MAJOR workflow.wdl

# Exit with error on any CRITICAL findings
miniwdl check --exit-on-lint-severity CRITICAL workflow.wdl

# Combine with strict mode (strict takes precedence)
miniwdl check --strict --exit-on-lint-severity MAJOR workflow.wdl
```

### Listing Available Linters

```bash
# List all available linters with their categories and severities
miniwdl check --list-linters
```

## Configuration Files

### Basic Configuration File

Create a `.miniwdl.cfg` file in your project directory or home directory:

```ini
[linting]
# Add custom linters
additional_linters = [
    "my_linters.py:TaskNamingLinter",
    "my_linters.py:SecurityLinter",
    "security_module:CredentialScanner"
]

# Disable specific built-in linters
disabled_linters = [
    "StringCoercion",
    "FileCoercion",
    "UnnecessaryQuantifier"
]

# Enable only specific categories
enabled_categories = [
    "STYLE",
    "SECURITY", 
    "PERFORMANCE"
]

# Disable specific categories
disabled_categories = [
    "PORTABILITY",
    "OTHER"
]

# Set exit behavior
exit_on_severity = "MAJOR"
```

### Advanced Configuration

```ini
[linting]
# Comprehensive linter configuration
additional_linters = [
    # File-based linters
    "/path/to/custom_linters.py:TaskNamingLinter",
    "/path/to/custom_linters.py:SecurityLinter",
    "/path/to/custom_linters.py:PerformanceLinter",
    
    # Module-based linters (from installed packages)
    "company_wdl_linters:CompanyStyleLinter",
    "security_linters:CredentialScanner",
    "performance_linters:ResourceOptimizer"
]

# Fine-grained linter control
disabled_linters = [
    # Disable specific built-in linters that conflict with custom ones
    "StringCoercion",      # We have a custom string handling linter
    "FileCoercion",        # We have a custom file handling linter
    "MixedIndentation"     # We use a different indentation standard
]

# Category-based filtering
enabled_categories = [
    "STYLE",           # Code formatting and naming
    "SECURITY",        # Security vulnerabilities
    "PERFORMANCE",     # Performance issues
    "CORRECTNESS",     # Logic errors
    "BEST_PRACTICE"    # Recommended practices
]

# Categories to disable
disabled_categories = [
    "PORTABILITY",     # We only target one platform
    "OTHER"           # Catch-all category we don't need
]

# Exit behavior
exit_on_severity = "MAJOR"  # Exit on MAJOR or CRITICAL findings

# Additional configuration (if supported by custom linters)
[linting.custom]
max_task_name_length = 50
require_task_documentation = true
allowed_commands = ["echo", "cat", "grep", "awk", "sed"]
```

### Project-Specific Configuration

For team projects, create a `.miniwdl.cfg` in the project root:

```ini
# Project-specific linting rules
[linting]
additional_linters = [
    "project_linters/style.py:ProjectStyleLinter",
    "project_linters/security.py:ProjectSecurityLinter"
]

disabled_linters = [
    "UnnecessaryQuantifier"  # We allow optional inputs with defaults
]

enabled_categories = [
    "STYLE",
    "SECURITY",
    "CORRECTNESS"
]

exit_on_severity = "MAJOR"

# Project-specific settings
[linting.project]
enforce_team_naming = true
require_author_metadata = true
max_workflow_complexity = 20
```

### User-Specific Configuration

Create a global configuration in your home directory (`~/.miniwdl.cfg`):

```ini
# Personal linting preferences
[linting]
additional_linters = [
    "~/my_linters/personal_style.py:PersonalStyleLinter"
]

disabled_linters = [
    "MixedIndentation"  # I prefer tabs
]

enabled_categories = [
    "STYLE",
    "SECURITY",
    "PERFORMANCE",
    "CORRECTNESS",
    "BEST_PRACTICE"
]

exit_on_severity = "MODERATE"  # I want to catch more issues

# Personal preferences
[linting.personal]
preferred_naming = "snake_case"
max_line_length = 120
require_comments = true
```

## Environment Variables

### Basic Environment Variables

```bash
# Add custom linters
export MINIWDL_ADDITIONAL_LINTERS="my_linters.py:Linter1,my_linters.py:Linter2"

# Disable specific linters
export MINIWDL_DISABLED_LINTERS="StringCoercion,FileCoercion"

# Enable specific categories
export MINIWDL_ENABLED_LINT_CATEGORIES="STYLE,SECURITY,PERFORMANCE"

# Disable specific categories
export MINIWDL_DISABLED_LINT_CATEGORIES="PORTABILITY,OTHER"

# Set exit behavior
export MINIWDL_EXIT_ON_LINT_SEVERITY="MAJOR"
```

### CI/CD Environment

For continuous integration environments:

```bash
#!/bin/bash
# CI linting configuration

# Use strict linting in CI
export MINIWDL_ADDITIONAL_LINTERS="ci_linters/strict.py:StrictLinter,ci_linters/security.py:SecurityLinter"
export MINIWDL_ENABLED_LINT_CATEGORIES="STYLE,SECURITY,CORRECTNESS"
export MINIWDL_EXIT_ON_LINT_SEVERITY="MODERATE"

# Run linting
miniwdl check --strict workflows/*.wdl
```

### Development Environment

For development environments:

```bash
#!/bin/bash
# Development linting configuration

# Use more lenient linting during development
export MINIWDL_ADDITIONAL_LINTERS="dev_linters/style.py:DevStyleLinter"
export MINIWDL_DISABLED_LINTERS="UnnecessaryQuantifier,MixedIndentation"
export MINIWDL_ENABLED_LINT_CATEGORIES="STYLE,SECURITY"
export MINIWDL_EXIT_ON_LINT_SEVERITY="MAJOR"

# Run linting
miniwdl check workflows/my_workflow.wdl
```

## Priority Order

Configuration options are applied in this priority order (highest to lowest):

1. **Command Line Arguments** - Always take precedence
2. **Environment Variables** - Override configuration files
3. **Configuration Files** - Provide default values

### Example Priority Resolution

Given this configuration file:
```ini
[linting]
exit_on_severity = "MINOR"
enabled_categories = ["STYLE", "SECURITY"]
```

And this environment variable:
```bash
export MINIWDL_EXIT_ON_LINT_SEVERITY="MAJOR"
```

And this command:
```bash
miniwdl check --enable-lint-categories PERFORMANCE workflow.wdl
```

The final configuration will be:
- `exit_on_severity = "MAJOR"` (from environment variable)
- `enabled_categories = ["PERFORMANCE"]` (from command line)

## Complete Examples

### Example 1: Team Development Setup

**Project `.miniwdl.cfg`:**
```ini
[linting]
additional_linters = [
    "team_linters/naming.py:TeamNamingLinter",
    "team_linters/security.py:TeamSecurityLinter",
    "team_linters/performance.py:TeamPerformanceLinter"
]

disabled_linters = [
    "StringCoercion"  # Team uses custom string handling
]

enabled_categories = [
    "STYLE",
    "SECURITY", 
    "PERFORMANCE",
    "CORRECTNESS"
]

exit_on_severity = "MAJOR"
```

**Developer's personal `~/.miniwdl.cfg`:**
```ini
[linting]
# Personal additions to team config
additional_linters = [
    "~/personal_linters/debug.py:DebugLinter"
]

# More lenient during development
exit_on_severity = "CRITICAL"
```

**CI/CD script:**
```bash
#!/bin/bash
# Override for strict CI checking
export MINIWDL_EXIT_ON_LINT_SEVERITY="MODERATE"
export MINIWDL_ADDITIONAL_LINTERS="ci_linters/strict.py:CIStrictLinter"

miniwdl check --strict workflows/*.wdl
```

### Example 2: Security-Focused Configuration

```ini
[linting]
# Security-focused linting
additional_linters = [
    "security_linters/credentials.py:CredentialScanner",
    "security_linters/commands.py:DangerousCommandLinter",
    "security_linters/network.py:NetworkAccessLinter",
    "security_linters/permissions.py:PermissionLinter"
]

# Disable non-security linters to focus on security
enabled_categories = [
    "SECURITY",
    "CORRECTNESS"  # Include correctness as it affects security
]

disabled_categories = [
    "STYLE",        # Don't care about style for security scan
    "PERFORMANCE",  # Don't care about performance for security scan
    "PORTABILITY",
    "BEST_PRACTICE",
    "OTHER"
]

# Exit on any security finding
exit_on_severity = "MINOR"
```

### Example 3: Performance-Focused Configuration

```ini
[linting]
# Performance-focused linting
additional_linters = [
    "perf_linters/resources.py:ResourceAllocationLinter",
    "perf_linters/commands.py:InefficientCommandLinter",
    "perf_linters/io.py:IOOptimizationLinter",
    "perf_linters/memory.py:MemoryUsageLinter"
]

# Focus on performance and correctness
enabled_categories = [
    "PERFORMANCE",
    "CORRECTNESS"
]

# Disable style-related linters
disabled_linters = [
    "MixedIndentation",
    "UnnecessaryQuantifier"
]

exit_on_severity = "MODERATE"
```

### Example 4: Gradual Adoption

For teams adopting linting gradually:

```ini
[linting]
# Start with basic style and critical security issues
enabled_categories = [
    "STYLE",
    "SECURITY"
]

# Only exit on critical issues initially
exit_on_severity = "CRITICAL"

# Disable linters that would generate too many warnings initially
disabled_linters = [
    "UnnecessaryQuantifier",
    "MixedIndentation",
    "StringCoercion"
]

# Add one custom linter to start
additional_linters = [
    "basic_linters/naming.py:BasicNamingLinter"
]
```

## Troubleshooting Configuration

### Debugging Configuration Issues

Use the `--list-linters` option to see what linters are actually loaded:

```bash
miniwdl check --list-linters
```

This will show:
- All available linters
- Their categories and severities
- Which ones are enabled/disabled
- Any loading errors

### Common Configuration Problems

1. **Linter not found:**
   ```
   Error: Failed to load linter my_linters.py:MyLinter
   ```
   - Check file path is correct
   - Ensure class name matches exactly
   - Verify the Python file is valid

2. **Category filtering not working:**
   - Check category names are spelled correctly
   - Use exact case: "STYLE", not "style"
   - Remember that `enabled_categories` overrides `disabled_categories`

3. **Environment variables not working:**
   - Ensure variable names are exact: `MINIWDL_ADDITIONAL_LINTERS`
   - Check for typos in variable names
   - Remember that command line options override environment variables

4. **Configuration file not loaded:**
   - Check file is named `.miniwdl.cfg`
   - Ensure it's in the current directory or home directory
   - Verify INI file syntax is correct

This configuration system provides flexible control over miniwdl's linting behavior, allowing you to customize it for different environments, teams, and use cases.
