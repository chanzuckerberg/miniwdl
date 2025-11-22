"""
Example style linters for miniwdl

This module contains example linters that enforce coding style and formatting standards.
These linters demonstrate best practices for creating style-focused linters.
"""

import re
from WDL.Lint import Linter, LintSeverity, LintCategory


class TaskNamingLinter(Linter):
    """
    Enforces consistent task naming conventions.
    
    Rules:
    - Task names should use snake_case (lowercase with underscores)
    - Task names should be descriptive (minimum 3 characters)
    - Task names should not be too long (maximum 50 characters)
    - Task names should not use generic names like 'task', 'run', 'execute'
    
    Examples:
        # Good
        task process_samples { ... }
        task align_reads { ... }
        task generate_report { ... }
        
        # Bad
        task ProcessSamples { ... }  # camelCase
        task ALIGN_READS { ... }     # UPPERCASE
        task t { ... }               # too short
        task task { ... }            # generic name
    """
    
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.snake_case_pattern = re.compile(r'^[a-z][a-z0-9_]*$')
        self.generic_names = {
            'task', 'run', 'execute', 'process', 'do', 'main', 'work', 'job'
        }
    
    def task(self, obj):
        name = obj.name
        
        # Check for snake_case
        if not self.snake_case_pattern.match(name):
            self.add(
                obj,
                f"Task name '{name}' should use snake_case (lowercase with underscores)",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        
        # Check minimum length
        if len(name) < 3:
            self.add(
                obj,
                f"Task name '{name}' is too short, use a more descriptive name",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        
        # Check maximum length
        if len(name) > 50:
            self.add(
                obj,
                f"Task name '{name}' is too long ({len(name)} characters, max 50)",
                obj.pos,
                severity=LintSeverity.MODERATE
            )
        
        # Check for generic names
        if name.lower() in self.generic_names:
            self.add(
                obj,
                f"Task name '{name}' is too generic, use a more specific name",
                obj.pos,
                severity=LintSeverity.MINOR
            )


class WorkflowNamingLinter(Linter):
    """
    Enforces consistent workflow naming conventions.
    
    Rules:
    - Workflow names should use snake_case
    - Workflow names should be descriptive
    - Workflow names should not conflict with task names
    """
    
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.snake_case_pattern = re.compile(r'^[a-z][a-z0-9_]*$')
        self.task_names = set()
    
    def task(self, obj):
        """Collect task names to check for conflicts"""
        self.task_names.add(obj.name)
    
    def workflow(self, obj):
        name = obj.name
        
        # Check for snake_case
        if not self.snake_case_pattern.match(name):
            self.add(
                obj,
                f"Workflow name '{name}' should use snake_case (lowercase with underscores)",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        
        # Check for conflicts with task names
        if name in self.task_names:
            self.add(
                obj,
                f"Workflow name '{name}' conflicts with a task name",
                obj.pos,
                severity=LintSeverity.MODERATE
            )


class DocumentationLinter(Linter):
    """
    Enforces documentation requirements for tasks and workflows.
    
    Rules:
    - Tasks and workflows should have descriptions in meta sections
    - Descriptions should be meaningful (not just the name)
    - Complex tasks should have parameter documentation
    """
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        # Check for meta section with description
        if not obj.meta or 'description' not in obj.meta:
            self.add(
                obj,
                f"Task '{obj.name}' should have a description in the meta section",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        elif obj.meta and 'description' in obj.meta:
            description = str(obj.meta['description']).strip('"\'')
            
            # Check for meaningful description
            if description.lower() == obj.name.lower().replace('_', ' '):
                self.add(
                    obj,
                    f"Task '{obj.name}' description should be more detailed than just the name",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
            
            # Check minimum description length
            if len(description) < 10:
                self.add(
                    obj,
                    f"Task '{obj.name}' description is too brief, provide more detail",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
    
    def workflow(self, obj):
        # Check for meta section with description
        if not obj.meta or 'description' not in obj.meta:
            self.add(
                obj,
                f"Workflow '{obj.name}' should have a description in the meta section",
                obj.pos,
                severity=LintSeverity.MINOR
            )


class IndentationLinter(Linter):
    """
    Enforces consistent indentation in command blocks.
    
    Rules:
    - Command blocks should use consistent indentation
    - Prefer 2 or 4 spaces for indentation
    - Avoid mixing tabs and spaces
    """
    
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        lines = command_str.split('\n')
        
        # Check for mixed indentation
        has_tabs = any('\t' in line for line in lines)
        has_spaces = any(line.startswith(' ') for line in lines if line.strip())
        
        if has_tabs and has_spaces:
            self.add(
                obj,
                f"Task '{obj.name}' command block mixes tabs and spaces for indentation",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        
        # Check for consistent indentation levels
        indentations = []
        for line in lines:
            if line.strip():  # Skip empty lines
                indent = len(line) - len(line.lstrip())
                if indent > 0:
                    indentations.append(indent)
        
        if indentations:
            # Check if indentations follow a consistent pattern
            unique_indents = sorted(set(indentations))
            if len(unique_indents) > 1:
                # Check if they're multiples of a base indentation
                base_indent = unique_indents[0]
                if not all(indent % base_indent == 0 for indent in unique_indents):
                    self.add(
                        obj,
                        f"Task '{obj.name}' command block has inconsistent indentation",
                        obj.pos,
                        severity=LintSeverity.MINOR
                    )


class VariableNamingLinter(Linter):
    """
    Enforces consistent variable naming conventions.
    
    Rules:
    - Variable names should use snake_case
    - Variable names should be descriptive
    - Avoid single-letter variable names except for common cases (i, j, k for indices)
    """
    
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.snake_case_pattern = re.compile(r'^[a-z][a-z0-9_]*$')
        self.allowed_single_letters = {'i', 'j', 'k', 'n', 'x', 'y', 'z'}
    
    def decl(self, obj):
        name = obj.name
        
        # Skip if this is a task or workflow name (handled by other linters)
        if hasattr(obj, 'command') or hasattr(obj, 'body'):
            return
        
        # Check for snake_case
        if not self.snake_case_pattern.match(name):
            self.add(
                obj,
                f"Variable name '{name}' should use snake_case (lowercase with underscores)",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        
        # Check for single-letter names
        if len(name) == 1 and name not in self.allowed_single_letters:
            self.add(
                obj,
                f"Variable name '{name}' is too short, use a more descriptive name",
                obj.pos,
                severity=LintSeverity.MINOR
            )
