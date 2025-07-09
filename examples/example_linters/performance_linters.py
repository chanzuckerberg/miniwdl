"""
Example performance linters for miniwdl

This module contains example linters that detect performance issues and inefficient patterns.
These linters demonstrate best practices for creating performance-focused linters.
"""

import re
from WDL.Lint import Linter, LintSeverity, LintCategory


class ResourceAllocationLinter(Linter):
    """
    Checks for appropriate resource allocation in task runtime sections.
    
    This linter identifies:
    - Missing resource specifications
    - Excessive resource requests
    - Inefficient resource combinations
    - Missing disk space specifications for large operations
    
    Examples:
        # Missing resources
        task process_data {
            command { ... }
            # Missing runtime section
        }
        
        # Good resource specification
        task process_data {
            command { ... }
            runtime {
                memory: "4 GB"
                cpu: 2
                disk: "10 GB"
            }
        }
    """
    
    category = LintCategory.PERFORMANCE
    default_severity = LintSeverity.MODERATE
    
    def task(self, obj):
        runtime_attrs = obj.runtime or {}
        
        # Check for missing memory specification
        if 'memory' not in runtime_attrs:
            self.add(
                obj,
                f"Task '{obj.name}' should specify memory requirements for optimal resource allocation",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        else:
            # Check for excessive memory requests
            memory_str = str(runtime_attrs['memory']).lower()
            if self._parse_memory_gb(memory_str) > 100:
                self.add(
                    obj,
                    f"Task '{obj.name}' requests excessive memory ({memory_str}) - verify this is necessary",
                    obj.pos,
                    severity=LintSeverity.MODERATE
                )
        
        # Check for missing CPU specification
        if 'cpu' not in runtime_attrs:
            self.add(
                obj,
                f"Task '{obj.name}' should specify CPU requirements for optimal scheduling",
                obj.pos,
                severity=LintSeverity.MINOR
            )
        else:
            # Check for excessive CPU requests
            try:
                cpu_count = int(str(runtime_attrs['cpu']))
                if cpu_count > 32:
                    self.add(
                        obj,
                        f"Task '{obj.name}' requests {cpu_count} CPUs - verify this level of parallelization is effective",
                        obj.pos,
                        severity=LintSeverity.MODERATE
                    )
            except (ValueError, TypeError):
                pass
        
        # Check for missing disk specification for I/O intensive tasks
        if 'disk' not in runtime_attrs:
            command_str = str(obj.command).lower()
            io_intensive_patterns = [
                r'\bsort\b', r'\buniq\b', r'\bawk\b', r'\bsed\b',
                r'\bgrep\b', r'\bcut\b', r'\bjoin\b', r'\bcomm\b',
                r'\.gz\b', r'\.bz2\b', r'\.xz\b',  # Compression
                r'\btar\b', r'\bzip\b', r'\bunzip\b'
            ]
            
            if any(re.search(pattern, command_str) for pattern in io_intensive_patterns):
                self.add(
                    obj,
                    f"Task '{obj.name}' appears to be I/O intensive but doesn't specify disk requirements",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
    
    def _parse_memory_gb(self, memory_str):
        """Parse memory string and return value in GB"""
        try:
            memory_str = memory_str.replace('"', '').replace("'", '').strip()
            if 'gb' in memory_str:
                return float(memory_str.replace('gb', '').strip())
            elif 'mb' in memory_str:
                return float(memory_str.replace('mb', '').strip()) / 1024
            elif 'tb' in memory_str:
                return float(memory_str.replace('tb', '').strip()) * 1024
            else:
                # Assume GB if no unit
                return float(memory_str)
        except (ValueError, AttributeError):
            return 0


class InefficientCommandLinter(Linter):
    """
    Detects inefficient command patterns that could be optimized.
    
    This linter identifies:
    - Unnecessary use of cat with pipes
    - Inefficient file processing patterns
    - Redundant operations
    - Suboptimal tool usage
    
    Examples:
        # Inefficient
        command { cat file.txt | head -10 }
        command { cat file.txt | grep pattern }
        command { cat file.txt | wc -l }
        
        # Efficient
        command { head -10 file.txt }
        command { grep pattern file.txt }
        command { wc -l file.txt }
    """
    
    category = LintCategory.PERFORMANCE
    default_severity = LintSeverity.MINOR
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Define inefficient patterns and their optimizations
        self.inefficient_patterns = [
            {
                'pattern': r'\bcat\s+([^\s|]+)\s*\|\s*head\b',
                'message': "Use 'head file' instead of 'cat file | head' for better performance",
                'severity': LintSeverity.MINOR
            },
            {
                'pattern': r'\bcat\s+([^\s|]+)\s*\|\s*tail\b',
                'message': "Use 'tail file' instead of 'cat file | tail' for better performance",
                'severity': LintSeverity.MINOR
            },
            {
                'pattern': r'\bcat\s+([^\s|]+)\s*\|\s*grep\b',
                'message': "Use 'grep pattern file' instead of 'cat file | grep pattern' for better performance",
                'severity': LintSeverity.MINOR
            },
            {
                'pattern': r'\bcat\s+([^\s|]+)\s*\|\s*wc\b',
                'message': "Use 'wc file' instead of 'cat file | wc' for better performance",
                'severity': LintSeverity.MINOR
            },
            {
                'pattern': r'\bcat\s+([^\s|]+)\s*\|\s*sort\b',
                'message': "Use 'sort file' instead of 'cat file | sort' for better performance",
                'severity': LintSeverity.MINOR
            },
            {
                'pattern': r'\bls\s+[^|]*\|\s*grep\b',
                'message': "Use shell globbing or 'find' instead of 'ls | grep' for better performance",
                'severity': LintSeverity.MINOR
            },
            {
                'pattern': r'\bfind\s+[^|]*\|\s*xargs\s+rm\b',
                'message': "Use 'find ... -delete' instead of 'find ... | xargs rm' for better performance",
                'severity': LintSeverity.MINOR
            },
        ]
        
        # Compile patterns
        self.compiled_patterns = [
            {
                'pattern': re.compile(item['pattern'], re.IGNORECASE),
                'message': item['message'],
                'severity': item['severity']
            }
            for item in self.inefficient_patterns
        ]
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        
        for item in self.compiled_patterns:
            if item['pattern'].search(command_str):
                self.add(
                    obj,
                    item['message'],
                    obj.pos,
                    severity=item['severity']
                )


class LargeFileHandlingLinter(Linter):
    """
    Checks for efficient handling of large files.
    
    This linter identifies:
    - Operations that load entire files into memory
    - Missing streaming operations for large data
    - Inefficient sorting of large files
    - Missing compression for intermediate files
    
    Examples:
        # Potentially inefficient for large files
        command { sort huge_file.txt }
        
        # Better for large files
        command { sort -T /tmp huge_file.txt }
        runtime { disk: "50 GB" }
    """
    
    category = LintCategory.PERFORMANCE
    default_severity = LintSeverity.MODERATE
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        runtime_attrs = obj.runtime or {}
        
        # Check for sort without temp directory specification
        if re.search(r'\bsort\b(?!.*-T)', command_str, re.IGNORECASE):
            if 'disk' not in runtime_attrs:
                self.add(
                    obj,
                    "Large file sorting should specify temp directory (-T) and disk space requirements",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
        
        # Check for operations that might benefit from compression
        compression_candidates = [
            r'\btar\s+.*cf\b',  # tar create without compression
            r'\bcp\s+.*\.txt\b',  # copying text files
        ]
        
        for pattern in compression_candidates:
            if re.search(pattern, command_str, re.IGNORECASE):
                self.add(
                    obj,
                    "Consider using compression for large intermediate files to save disk space",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
                break
        
        # Check for memory-intensive operations without memory specification
        memory_intensive_patterns = [
            r'\bawk\b.*BEGIN',  # AWK with BEGIN (might load data)
            r'\bpython\b.*pandas',  # Python with pandas
            r'\bR\b.*read\.table',  # R reading tables
        ]
        
        if 'memory' not in runtime_attrs:
            for pattern in memory_intensive_patterns:
                if re.search(pattern, command_str, re.IGNORECASE):
                    self.add(
                        obj,
                        "Memory-intensive operations should specify memory requirements",
                        obj.pos,
                        severity=LintSeverity.MODERATE
                    )
                    break


class ParallelizationLinter(Linter):
    """
    Identifies opportunities for parallelization and checks for proper parallel usage.
    
    This linter identifies:
    - Tools that support parallelization but don't use it
    - Inefficient parallel patterns
    - Missing CPU specifications for parallel tasks
    
    Examples:
        # Could be parallelized
        command { gzip file.txt }
        
        # Better
        command { pigz file.txt }
        runtime { cpu: 4 }
    """
    
    category = LintCategory.PERFORMANCE
    default_severity = LintSeverity.MINOR
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Tools that have parallel alternatives
        self.parallelizable_tools = {
            'gzip': 'pigz',
            'bzip2': 'pbzip2',
            'xz': 'pxz',
            'grep': 'grep with -P or parallel grep',
            'sort': 'sort with --parallel',
        }
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        runtime_attrs = obj.runtime or {}
        
        # Check for parallelizable tools
        for tool, alternative in self.parallelizable_tools.items():
            if re.search(rf'\b{tool}\b', command_str, re.IGNORECASE):
                # Check if CPU count suggests parallelization would help
                cpu_count = 1
                if 'cpu' in runtime_attrs:
                    try:
                        cpu_count = int(str(runtime_attrs['cpu']))
                    except (ValueError, TypeError):
                        pass
                
                if cpu_count > 1:
                    self.add(
                        obj,
                        f"Consider using {alternative} instead of {tool} for better parallelization with {cpu_count} CPUs",
                        obj.pos,
                        severity=LintSeverity.MINOR
                    )
        
        # Check for parallel tools without CPU specification
        parallel_tools = ['pigz', 'pbzip2', 'pxz', 'parallel']
        for tool in parallel_tools:
            if re.search(rf'\b{tool}\b', command_str, re.IGNORECASE):
                if 'cpu' not in runtime_attrs:
                    self.add(
                        obj,
                        f"Task uses {tool} but doesn't specify CPU count - consider adding cpu runtime attribute",
                        obj.pos,
                        severity=LintSeverity.MINOR
                    )


class IOOptimizationLinter(Linter):
    """
    Checks for I/O optimization opportunities.
    
    This linter identifies:
    - Multiple reads of the same file
    - Inefficient file access patterns
    - Missing buffering for sequential operations
    - Temporary file usage patterns
    
    Examples:
        # Inefficient - multiple reads
        command {
            head file.txt
            tail file.txt
            wc -l file.txt
        }
        
        # Better - single pass
        command {
            awk 'NR<=10{print "HEAD:"$0} END{print "LINES:"NR; for(i=NR-9;i<=NR;i++) print "TAIL:"a[i%10]} {a[NR%10]=$0}' file.txt
        }
    """
    
    category = LintCategory.PERFORMANCE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        
        # Look for multiple operations on the same file
        file_operations = {}
        
        # Simple pattern to find file operations
        operations = ['cat', 'head', 'tail', 'grep', 'awk', 'sed', 'cut', 'sort', 'uniq', 'wc']
        
        for op in operations:
            matches = re.finditer(rf'\b{op}\s+([^\s;|&]+)', command_str, re.IGNORECASE)
            for match in matches:
                filename = match.group(1)
                if filename not in file_operations:
                    file_operations[filename] = []
                file_operations[filename].append(op)
        
        # Check for files accessed multiple times
        for filename, ops in file_operations.items():
            if len(ops) > 2 and not filename.startswith('~{'):  # Skip WDL variables
                self.add(
                    obj,
                    f"File '{filename}' is accessed {len(ops)} times - consider combining operations for better I/O performance",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
        
        # Check for inefficient temporary file patterns
        if re.search(r'>\s*/tmp/.*\s*&&.*<\s*/tmp/', command_str):
            self.add(
                obj,
                "Consider using pipes instead of temporary files for better performance",
                obj.pos,
                severity=LintSeverity.MINOR
            )


class MemoryUsageLinter(Linter):
    """
    Analyzes memory usage patterns and suggests optimizations.
    
    This linter identifies:
    - Operations that might consume excessive memory
    - Missing memory limits for memory-intensive tasks
    - Inefficient data structures or algorithms
    
    Examples:
        # Memory-intensive without specification
        command { sort -k1,1 huge_file.txt }
        
        # Better
        command { sort -k1,1 huge_file.txt }
        runtime {
            memory: "8 GB"
            disk: "20 GB"
        }
    """
    
    category = LintCategory.PERFORMANCE
    default_severity = LintSeverity.MODERATE
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        runtime_attrs = obj.runtime or {}
        
        # Memory-intensive operations
        memory_intensive_patterns = [
            (r'\bsort\b(?!.*-T)', "Sort without temp directory can use excessive memory"),
            (r'\buniq\b(?!.*sort)', "Uniq without prior sort might load entire file"),
            (r'\bawk\b.*\{.*\[.*\].*\}', "AWK with arrays can consume significant memory"),
            (r'\bpython\b.*pandas', "Python pandas operations can be memory-intensive"),
            (r'\bR\b.*read\.', "R data loading can consume significant memory"),
            (r'\bjava\b(?!.*-Xmx)', "Java without memory limits can consume excessive memory"),
        ]
        
        for pattern, message in memory_intensive_patterns:
            if re.search(pattern, command_str, re.IGNORECASE):
                if 'memory' not in runtime_attrs:
                    self.add(
                        obj,
                        f"{message} - consider specifying memory requirements",
                        obj.pos,
                        severity=LintSeverity.MODERATE
                    )
                break
