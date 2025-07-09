"""
Example best practices linters for miniwdl

This module contains example linters that enforce best practices for workflow structure,
maintainability, and code quality. These linters demonstrate patterns for creating
best-practice-focused linters.
"""

import re
from WDL.Lint import Linter, LintSeverity, LintCategory


class WorkflowStructureLinter(Linter):
    """
    Enforces good workflow structure and organization.
    
    This linter checks for:
    - Workflow complexity (too many tasks)
    - Proper workflow organization
    - Appropriate use of scatter/conditional blocks
    - Workflow input/output organization
    
    Examples:
        # Complex workflow that should be broken down
        workflow huge_workflow {
            # 50+ task calls...
        }
        
        # Better - modular approach
        workflow main_workflow {
            call preprocessing_workflow { ... }
            call analysis_workflow { ... }
            call postprocessing_workflow { ... }
        }
    """
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MODERATE
    
    def workflow(self, obj):
        # Count different types of workflow elements
        task_calls = 0
        scatter_blocks = 0
        conditional_blocks = 0
        
        for element in obj.body:
            if hasattr(element, 'task'):  # Task call
                task_calls += 1
            elif hasattr(element, 'scatter'):  # Scatter block
                scatter_blocks += 1
            elif hasattr(element, 'condition'):  # Conditional block
                conditional_blocks += 1
        
        # Check for overly complex workflows
        total_complexity = task_calls + (scatter_blocks * 2) + (conditional_blocks * 2)
        
        if total_complexity > 20:
            self.add(
                obj,
                f"Workflow '{obj.name}' is complex (complexity score: {total_complexity}). "
                "Consider breaking into smaller, modular workflows.",
                obj.pos,
                severity=LintSeverity.MODERATE
            )
        elif total_complexity > 35:
            self.add(
                obj,
                f"Workflow '{obj.name}' is very complex (complexity score: {total_complexity}). "
                "This workflow should be refactored into smaller components.",
                obj.pos,
                severity=LintSeverity.MAJOR
            )
        
        # Check for workflows with too many direct task calls
        if task_calls > 15:
            self.add(
                obj,
                f"Workflow '{obj.name}' has {task_calls} direct task calls. "
                "Consider grouping related tasks into sub-workflows.",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        
        # Check for missing workflow documentation
        if not obj.meta or 'description' not in obj.meta:
            self.add(
                obj,
                f"Workflow '{obj.name}' should have a description in the meta section",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        
        # Check for missing version information
        if not obj.meta or 'version' not in obj.meta:
            self.add(
                obj,
                f"Workflow '{obj.name}' should include version information in the meta section",
                obj.pos,
                severity=LintSeverity.MINOR
            )


class ErrorHandlingLinter(Linter):
    """
    Checks for proper error handling patterns in tasks.
    
    This linter identifies:
    - Missing error handling in commands
    - Commands that might fail silently
    - Lack of input validation
    - Missing exit status checks
    
    Examples:
        # Poor error handling
        command { 
            wget http://example.com/file
            process_file file
        }
        
        # Better error handling
        command {
            set -euo pipefail
            wget http://example.com/file || {
                echo "Failed to download file" >&2
                exit 1
            }
            if [ ! -f file ]; then
                echo "Downloaded file not found" >&2
                exit 1
            fi
            process_file file
        }
    """
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MODERATE
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Commands that commonly fail and should have error handling
        self.risky_commands = [
            'wget', 'curl', 'scp', 'rsync', 'ssh',
            'git', 'svn', 'docker', 'singularity',
            'pip', 'conda', 'apt-get', 'yum',
            'make', 'cmake', 'gcc', 'javac'
        ]
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        
        # Check for set -e or equivalent error handling
        has_error_handling = any(pattern in command_str for pattern in [
            'set -e', 'set -o errexit', 'set -euo pipefail'
        ])
        
        # Check for risky commands
        has_risky_commands = any(
            re.search(rf'\b{cmd}\b', command_str, re.IGNORECASE)
            for cmd in self.risky_commands
        )
        
        if has_risky_commands and not has_error_handling:
            self.add(
                obj,
                f"Task '{obj.name}' uses commands that may fail but lacks error handling (consider 'set -e' or explicit error checks)",
                obj.pos,
                severity=LintSeverity.MODERATE
            )
        
        # Check for commands with output redirection that might hide errors
        if re.search(r'2>/dev/null', command_str):
            self.add(
                obj,
                f"Task '{obj.name}' redirects stderr to /dev/null, which may hide important error messages",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        
        # Check for file operations without validation
        file_operations = ['cp', 'mv', 'rm', 'chmod', 'chown']
        for op in file_operations:
            if re.search(rf'\b{op}\b.*~\{{[^}}]+\}}', command_str, re.IGNORECASE):
                if not re.search(r'\[\s*-[ef]\s+.*\]', command_str):
                    self.add(
                        obj,
                        f"Task '{obj.name}' performs file operations on WDL variables without validation",
                        obj.pos,
                        severity=LintSeverity.MINOR
                    )
                    break


class OutputOrganizationLinter(Linter):
    """
    Ensures outputs are properly organized and documented.
    
    This linter checks for:
    - Descriptive output names
    - Proper output types
    - Missing outputs for tasks that generate files
    - Consistent output naming patterns
    
    Examples:
        # Poor output organization
        task analyze {
            command { ... }
            output {
                File out = "result"
                String s = stdout()
            }
        }
        
        # Better output organization
        task analyze {
            command { ... }
            output {
                File analysis_report = "analysis_report.txt"
                File summary_stats = "summary_statistics.json"
                String analysis_log = stdout()
            }
        }
    """
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MINOR
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Generic output names that should be more specific
        self.generic_names = {
            'out', 'output', 'result', 'file', 'data', 'temp', 'tmp'
        }
    
    def task(self, obj):
        if not obj.outputs:
            # Check if task likely produces outputs but doesn't declare them
            command_str = str(obj.command).lower()
            output_indicators = [
                r'>\s*[^&]',  # Output redirection
                r'tee\s+',    # tee command
                r'echo.*>\s*',  # echo to file
                r'printf.*>\s*',  # printf to file
                r'cat.*>\s*',   # cat to file
            ]
            
            if any(re.search(pattern, command_str) for pattern in output_indicators):
                self.add(
                    obj,
                    f"Task '{obj.name}' appears to generate output files but doesn't declare any outputs",
                    obj.pos,
                    severity=LintSeverity.MODERATE
                )
            return
        
        # Check each output
        for output_decl in obj.outputs:
            output_name = output_decl.name
            
            # Check for generic names
            if output_name.lower() in self.generic_names:
                self.add(
                    obj,
                    f"Output name '{output_name}' is too generic, use a more descriptive name",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
            
            # Check for very short names
            if len(output_name) < 3:
                self.add(
                    obj,
                    f"Output name '{output_name}' is too short, use a more descriptive name",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
            
            # Check for inconsistent naming (mixing styles)
            if '_' in output_name and any(c.isupper() for c in output_name):
                self.add(
                    obj,
                    f"Output name '{output_name}' mixes naming conventions, use consistent snake_case",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )


class InputValidationLinter(Linter):
    """
    Ensures proper input validation and documentation.
    
    This linter checks for:
    - Documented input parameters
    - Appropriate input types
    - Missing optional input defaults
    - Input naming consistency
    
    Examples:
        # Poor input handling
        task process {
            input {
                String s
                File f
                Int i
            }
            # ...
        }
        
        # Better input handling
        task process {
            input {
                String sample_name
                File input_data
                Int? max_iterations = 100
            }
            # ...
        }
    """
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        if not obj.inputs:
            return
        
        for input_decl in obj.inputs:
            input_name = input_decl.name
            input_type = str(input_decl.type)
            
            # Check for single-letter input names
            if len(input_name) == 1:
                self.add(
                    obj,
                    f"Input parameter '{input_name}' has a single-letter name, use a more descriptive name",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
            
            # Check for generic input names
            generic_names = {'input', 'file', 'data', 'string', 'int', 'bool'}
            if input_name.lower() in generic_names:
                self.add(
                    obj,
                    f"Input parameter '{input_name}' has a generic name, use a more specific name",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
            
            # Check for optional inputs without defaults
            if '?' in input_type and not hasattr(input_decl, 'expr'):
                self.add(
                    obj,
                    f"Optional input parameter '{input_name}' should have a default value",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )


class VersioningLinter(Linter):
    """
    Enforces proper versioning and metadata practices.
    
    This linter checks for:
    - WDL version declarations
    - Task/workflow version metadata
    - Author information
    - Proper import versioning
    
    Examples:
        # Missing version info
        task analyze { ... }
        
        # Better with version info
        task analyze {
            meta {
                version: "1.2.0"
                author: "Data Team"
                description: "Performs statistical analysis"
            }
            # ...
        }
    """
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MINOR
    
    def document(self, obj):
        # Check for WDL version declaration
        if not hasattr(obj, 'wdl_version') or not obj.wdl_version:
            self.add(
                obj,
                "Document should declare WDL version at the beginning",
                obj.pos,
                severity=LintSeverity.MODERATE
            )
    
    def task(self, obj):
        if obj.meta:
            # Check for version information
            if 'version' not in obj.meta:
                self.add(
                    obj,
                    f"Task '{obj.name}' should include version information in meta section",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
            
            # Check for author information
            if 'author' not in obj.meta and 'maintainer' not in obj.meta:
                self.add(
                    obj,
                    f"Task '{obj.name}' should include author or maintainer information in meta section",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
    
    def workflow(self, obj):
        if obj.meta:
            # Check for version information
            if 'version' not in obj.meta:
                self.add(
                    obj,
                    f"Workflow '{obj.name}' should include version information in meta section",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )


class ModularityLinter(Linter):
    """
    Promotes modular design and reusability.
    
    This linter checks for:
    - Tasks that are too complex and should be split
    - Repeated code patterns
    - Opportunities for parameterization
    - Proper separation of concerns
    
    Examples:
        # Monolithic task
        task do_everything {
            command {
                # 100+ lines of complex operations
            }
        }
        
        # Better modular approach
        task preprocess { ... }
        task analyze { ... }
        task postprocess { ... }
    """
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MODERATE
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        command_lines = [line.strip() for line in command_str.split('\n') if line.strip()]
        
        # Check for overly long tasks
        if len(command_lines) > 50:
            self.add(
                obj,
                f"Task '{obj.name}' has {len(command_lines)} command lines. "
                "Consider breaking into smaller, focused tasks.",
                obj.pos,
                severity=LintSeverity.MODERATE
            )
        elif len(command_lines) > 100:
            self.add(
                obj,
                f"Task '{obj.name}' has {len(command_lines)} command lines. "
                "This task is too complex and should be refactored.",
                obj.pos,
                severity=LintSeverity.MAJOR
            )
        
        # Check for multiple distinct operations in one task
        operation_indicators = [
            ('download', ['wget', 'curl', 'scp', 'rsync']),
            ('process', ['awk', 'sed', 'grep', 'cut', 'sort']),
            ('analyze', ['python', 'R', 'java', 'perl']),
            ('compress', ['gzip', 'bzip2', 'tar', 'zip']),
            ('validate', ['md5sum', 'sha256sum', 'diff', 'cmp']),
        ]
        
        found_operations = []
        for op_name, commands in operation_indicators:
            if any(re.search(rf'\b{cmd}\b', command_str, re.IGNORECASE) for cmd in commands):
                found_operations.append(op_name)
        
        if len(found_operations) > 2:
            self.add(
                obj,
                f"Task '{obj.name}' performs multiple distinct operations ({', '.join(found_operations)}). "
                "Consider splitting into separate tasks for better modularity.",
                obj.pos,
                severity=LintSeverity.MINOR
            )


class ConsistencyLinter(Linter):
    """
    Enforces consistency across the workflow.
    
    This linter checks for:
    - Consistent naming patterns
    - Consistent parameter patterns
    - Consistent output patterns
    - Consistent documentation styles
    """
    
    category = LintCategory.BEST_PRACTICE
    default_severity = LintSeverity.MINOR
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_names = []
        self.input_patterns = []
        self.output_patterns = []
    
    def task(self, obj):
        self.task_names.append(obj.name)
        
        # Collect input patterns
        if obj.inputs:
            for input_decl in obj.inputs:
                self.input_patterns.append(input_decl.name)
        
        # Collect output patterns
        if obj.outputs:
            for output_decl in obj.outputs:
                self.output_patterns.append(output_decl.name)
    
    def document(self, obj):
        # After processing all tasks, check for consistency
        if len(self.task_names) > 1:
            # Check naming pattern consistency
            snake_case_count = sum(1 for name in self.task_names if '_' in name and name.islower())
            camel_case_count = sum(1 for name in self.task_names if any(c.isupper() for c in name))
            
            if snake_case_count > 0 and camel_case_count > 0:
                self.add(
                    obj,
                    "Inconsistent task naming patterns detected - use either snake_case or camelCase consistently",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
