"""
Example Linters Package for miniwdl

This package contains example linters that demonstrate how to create custom linters
for different categories: style, security, performance, and best practices.

Usage:
    # Use individual linters
    miniwdl check --additional-linters examples/example_linters/style_linters.py:TaskNamingLinter workflow.wdl
    
    # Use multiple linters from different modules
    miniwdl check --additional-linters \
        examples/example_linters/style_linters.py:TaskNamingLinter,\
        examples/example_linters/security_linters.py:DangerousCommandLinter,\
        examples/example_linters/performance_linters.py:ResourceAllocationLinter \
        workflow.wdl

Available Linters:

Style Linters (style_linters.py):
    - TaskNamingLinter: Enforces snake_case task naming
    - WorkflowNamingLinter: Enforces workflow naming conventions
    - DocumentationLinter: Requires task/workflow documentation
    - IndentationLinter: Checks command block indentation
    - VariableNamingLinter: Enforces variable naming conventions

Security Linters (security_linters.py):
    - DangerousCommandLinter: Detects dangerous commands
    - CredentialScannerLinter: Scans for hardcoded credentials
    - NetworkAccessLinter: Flags network access and insecure protocols
    - FilePermissionLinter: Checks for insecure file permissions
    - InputValidationLinter: Ensures proper input validation

Performance Linters (performance_linters.py):
    - ResourceAllocationLinter: Checks resource specifications
    - InefficientCommandLinter: Detects inefficient command patterns
    - LargeFileHandlingLinter: Optimizes large file operations
    - ParallelizationLinter: Identifies parallelization opportunities
    - IOOptimizationLinter: Optimizes I/O operations
    - MemoryUsageLinter: Analyzes memory usage patterns

Best Practices Linters (best_practices_linters.py):
    - WorkflowStructureLinter: Enforces good workflow structure
    - ErrorHandlingLinter: Checks for proper error handling
    - OutputOrganizationLinter: Ensures well-organized outputs
    - InputValidationLinter: Validates input parameters
    - VersioningLinter: Enforces versioning and metadata
    - ModularityLinter: Promotes modular design
    - ConsistencyLinter: Enforces consistency across workflows
"""

# Import all linters for easy access
from .style_linters import (
    TaskNamingLinter,
    WorkflowNamingLinter,
    DocumentationLinter,
    IndentationLinter,
    VariableNamingLinter
)

from .security_linters import (
    DangerousCommandLinter,
    CredentialScannerLinter,
    NetworkAccessLinter,
    FilePermissionLinter,
    InputValidationLinter as SecurityInputValidationLinter
)

from .performance_linters import (
    ResourceAllocationLinter,
    InefficientCommandLinter,
    LargeFileHandlingLinter,
    ParallelizationLinter,
    IOOptimizationLinter,
    MemoryUsageLinter
)

from .best_practices_linters import (
    WorkflowStructureLinter,
    ErrorHandlingLinter,
    OutputOrganizationLinter,
    InputValidationLinter as BestPracticesInputValidationLinter,
    VersioningLinter,
    ModularityLinter,
    ConsistencyLinter
)

# Define linter collections for easy use
STYLE_LINTERS = [
    TaskNamingLinter,
    WorkflowNamingLinter,
    DocumentationLinter,
    IndentationLinter,
    VariableNamingLinter
]

SECURITY_LINTERS = [
    DangerousCommandLinter,
    CredentialScannerLinter,
    NetworkAccessLinter,
    FilePermissionLinter,
    SecurityInputValidationLinter
]

PERFORMANCE_LINTERS = [
    ResourceAllocationLinter,
    InefficientCommandLinter,
    LargeFileHandlingLinter,
    ParallelizationLinter,
    IOOptimizationLinter,
    MemoryUsageLinter
]

BEST_PRACTICES_LINTERS = [
    WorkflowStructureLinter,
    ErrorHandlingLinter,
    OutputOrganizationLinter,
    BestPracticesInputValidationLinter,
    VersioningLinter,
    ModularityLinter,
    ConsistencyLinter
]

ALL_LINTERS = (
    STYLE_LINTERS +
    SECURITY_LINTERS +
    PERFORMANCE_LINTERS +
    BEST_PRACTICES_LINTERS
)

__all__ = [
    # Style linters
    'TaskNamingLinter',
    'WorkflowNamingLinter',
    'DocumentationLinter',
    'IndentationLinter',
    'VariableNamingLinter',
    
    # Security linters
    'DangerousCommandLinter',
    'CredentialScannerLinter',
    'NetworkAccessLinter',
    'FilePermissionLinter',
    'SecurityInputValidationLinter',
    
    # Performance linters
    'ResourceAllocationLinter',
    'InefficientCommandLinter',
    'LargeFileHandlingLinter',
    'ParallelizationLinter',
    'IOOptimizationLinter',
    'MemoryUsageLinter',
    
    # Best practices linters
    'WorkflowStructureLinter',
    'ErrorHandlingLinter',
    'OutputOrganizationLinter',
    'BestPracticesInputValidationLinter',
    'VersioningLinter',
    'ModularityLinter',
    'ConsistencyLinter',
    
    # Collections
    'STYLE_LINTERS',
    'SECURITY_LINTERS',
    'PERFORMANCE_LINTERS',
    'BEST_PRACTICES_LINTERS',
    'ALL_LINTERS'
]
