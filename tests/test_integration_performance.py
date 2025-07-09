#!/usr/bin/env python3
"""
Performance benchmark tests for the pluggable linting system

This module contains performance tests to ensure the linting system
scales well and performs efficiently.
"""

import unittest
import tempfile
import os
import sys
import time
import psutil
from pathlib import Path

# Add examples to path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'examples'))

from WDL import load, Lint


class TestLintingPerformance(unittest.TestCase):
    """Test linting performance with various scenarios"""
    
    def setUp(self):
        """Set up performance testing"""
        self.performance_results = {}
    
    def tearDown(self):
        """Report performance results"""
        if self.performance_results:
            print(f"\nPerformance results for {self._testMethodName}:")
            for metric, value in self.performance_results.items():
                print(f"  {metric}: {value}")
    
    def measure_performance(self, func, *args, **kwargs):
        """Measure execution time and memory usage of a function"""
        process = psutil.Process()
        
        # Measure initial memory
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Measure execution time
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        # Measure final memory
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        execution_time = end_time - start_time
        memory_delta = final_memory - initial_memory
        
        return result, execution_time, memory_delta
    
    def test_baseline_linting_performance(self):
        """Test baseline performance with built-in linters only"""
        
        # Create a moderately complex WDL file
        wdl_content = self._create_complex_wdl(10)  # 10 tasks
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
            tmp_file.write(wdl_content)
            tmp_file.flush()
            
            try:
                def lint_workflow():
                    doc = load(tmp_file.name, path=[])
                    Lint.lint(doc)
                    return Lint.collect(doc)
                
                results, exec_time, memory_delta = self.measure_performance(lint_workflow)
                
                self.performance_results.update({
                    'execution_time_seconds': f"{exec_time:.3f}",
                    'memory_delta_mb': f"{memory_delta:.2f}",
                    'lint_findings': len(results)
                })
                
                # Performance assertions
                self.assertLess(exec_time, 5.0, "Baseline linting should complete in under 5 seconds")
                self.assertLess(memory_delta, 100, "Memory usage should not increase by more than 100MB")
                
            finally:
                os.unlink(tmp_file.name)
    
    def test_custom_linters_performance_impact(self):
        """Test performance impact of adding custom linters"""
        
        # Import example linters
        from example_linters import ALL_LINTERS
        
        wdl_content = self._create_complex_wdl(10)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
            tmp_file.write(wdl_content)
            tmp_file.flush()
            
            try:
                # Measure baseline performance
                def baseline_lint():
                    doc = load(tmp_file.name, path=[])
                    Lint.lint(doc)
                    return Lint.collect(doc)
                
                baseline_results, baseline_time, baseline_memory = self.measure_performance(baseline_lint)
                
                # Measure performance with custom linters
                original_linters = Lint._all_linters.copy()
                
                try:
                    Lint._all_linters.extend(ALL_LINTERS)
                    
                    def custom_lint():
                        doc = load(tmp_file.name, path=[])
                        Lint.lint(doc)
                        return Lint.collect(doc)
                    
                    custom_results, custom_time, custom_memory = self.measure_performance(custom_lint)
                    
                    # Calculate overhead
                    time_overhead = custom_time - baseline_time
                    memory_overhead = custom_memory - baseline_memory
                    
                    self.performance_results.update({
                        'baseline_time_seconds': f"{baseline_time:.3f}",
                        'custom_linters_time_seconds': f"{custom_time:.3f}",
                        'time_overhead_seconds': f"{time_overhead:.3f}",
                        'time_overhead_percent': f"{(time_overhead / baseline_time * 100):.1f}%",
                        'memory_overhead_mb': f"{memory_overhead:.2f}",
                        'baseline_findings': len(baseline_results),
                        'custom_findings': len(custom_results)
                    })
                    
                    # Performance assertions
                    self.assertLess(time_overhead, baseline_time * 2, 
                                  "Custom linters should not more than double execution time")
                    self.assertLess(custom_time, 10.0, 
                                  "Linting with custom linters should complete in under 10 seconds")
                    
                finally:
                    Lint._all_linters[:] = original_linters
                    
            finally:
                os.unlink(tmp_file.name)
    
    def test_scaling_with_workflow_size(self):
        """Test how linting performance scales with workflow size"""
        
        workflow_sizes = [5, 10, 20, 50]
        scaling_results = {}
        
        for size in workflow_sizes:
            wdl_content = self._create_complex_wdl(size)
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
                tmp_file.write(wdl_content)
                tmp_file.flush()
                
                try:
                    def lint_workflow():
                        doc = load(tmp_file.name, path=[])
                        Lint.lint(doc)
                        return Lint.collect(doc)
                    
                    results, exec_time, memory_delta = self.measure_performance(lint_workflow)
                    
                    scaling_results[size] = {
                        'time': exec_time,
                        'memory': memory_delta,
                        'findings': len(results)
                    }
                    
                finally:
                    os.unlink(tmp_file.name)
        
        # Analyze scaling
        times = [scaling_results[size]['time'] for size in workflow_sizes]
        
        # Check that scaling is reasonable (not exponential)
        largest_time = times[-1]
        smallest_time = times[0]
        size_ratio = workflow_sizes[-1] / workflow_sizes[0]  # 50/5 = 10x
        time_ratio = largest_time / smallest_time
        
        self.performance_results.update({
            f'time_for_{size}_tasks': f"{scaling_results[size]['time']:.3f}s"
            for size in workflow_sizes
        })
        self.performance_results.update({
            'size_ratio': f"{size_ratio}x",
            'time_ratio': f"{time_ratio:.1f}x",
            'scaling_efficiency': f"{size_ratio / time_ratio:.2f}"
        })
        
        # Time should not scale worse than quadratically
        self.assertLess(time_ratio, size_ratio ** 2, 
                       "Linting time should not scale worse than quadratically with workflow size")
    
    def test_memory_usage_stability(self):
        """Test that memory usage remains stable across multiple linting operations"""
        
        wdl_content = self._create_complex_wdl(10)
        
        memory_measurements = []
        process = psutil.Process()
        
        # Perform multiple linting operations
        for i in range(10):
            with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
                tmp_file.write(wdl_content)
                tmp_file.flush()
                
                try:
                    # Measure memory before
                    memory_before = process.memory_info().rss / 1024 / 1024
                    
                    # Perform linting
                    doc = load(tmp_file.name, path=[])
                    Lint.lint(doc)
                    Lint.collect(doc)
                    
                    # Measure memory after
                    memory_after = process.memory_info().rss / 1024 / 1024
                    memory_measurements.append(memory_after - memory_before)
                    
                finally:
                    os.unlink(tmp_file.name)
        
        # Analyze memory stability
        avg_memory_delta = sum(memory_measurements) / len(memory_measurements)
        max_memory_delta = max(memory_measurements)
        min_memory_delta = min(memory_measurements)
        
        self.performance_results.update({
            'avg_memory_delta_mb': f"{avg_memory_delta:.2f}",
            'max_memory_delta_mb': f"{max_memory_delta:.2f}",
            'min_memory_delta_mb': f"{min_memory_delta:.2f}",
            'memory_variance': f"{max_memory_delta - min_memory_delta:.2f}"
        })
        
        # Memory usage should be stable (not growing significantly)
        self.assertLess(max_memory_delta, 50, "Memory delta should not exceed 50MB per operation")
        self.assertLess(max_memory_delta - min_memory_delta, 20, 
                       "Memory usage variance should be less than 20MB")
    
    def test_linter_discovery_performance(self):
        """Test performance of linter discovery mechanisms"""
        
        def discover_builtin_linters():
            from WDL.LintPlugins.plugins import discover_linters
            return discover_linters()
        
        def discover_with_additional_linters():
            from WDL.LintPlugins.plugins import discover_linters
            additional_specs = [
                "example_linters.style_linters:TaskNamingLinter",
                "example_linters.security_linters:DangerousCommandLinter",
                "example_linters.performance_linters:ResourceAllocationLinter"
            ]
            return discover_linters(additional_linters=additional_specs)
        
        # Test built-in linter discovery
        builtin_linters, builtin_time, builtin_memory = self.measure_performance(discover_builtin_linters)
        
        # Test discovery with additional linters
        all_linters, all_time, all_memory = self.measure_performance(discover_with_additional_linters)
        
        self.performance_results.update({
            'builtin_discovery_time_ms': f"{builtin_time * 1000:.1f}",
            'additional_discovery_time_ms': f"{all_time * 1000:.1f}",
            'builtin_linters_count': len(builtin_linters),
            'total_linters_count': len(all_linters),
            'discovery_overhead_ms': f"{(all_time - builtin_time) * 1000:.1f}"
        })
        
        # Discovery should be fast
        self.assertLess(builtin_time, 1.0, "Built-in linter discovery should take less than 1 second")
        self.assertLess(all_time, 2.0, "Discovery with additional linters should take less than 2 seconds")
    
    def _create_complex_wdl(self, num_tasks):
        """Create a complex WDL workflow for performance testing"""
        
        lines = [
            "version 1.0",
            "",
            "workflow performance_test_workflow {",
            "  input {",
            "    Array[String] samples",
            "    String reference_genome",
            "    Int quality_threshold = 30",
            "    Boolean run_analysis = true",
            "  }",
            "",
        ]
        
        # Add task calls with various patterns
        for i in range(num_tasks):
            if i % 3 == 0:
                # Scatter block
                lines.extend([
                    f"  scatter (sample in samples) {{",
                    f"    call task_{i} {{ input: sample_name = sample, threshold = quality_threshold }}",
                    f"  }}",
                    "",
                ])
            elif i % 3 == 1:
                # Conditional block
                lines.extend([
                    f"  if (run_analysis) {{",
                    f"    call task_{i} {{ input: reference = reference_genome }}",
                    f"  }}",
                    "",
                ])
            else:
                # Regular call
                lines.extend([
                    f"  call task_{i}",
                    "",
                ])
        
        # Add outputs
        lines.extend([
            "  output {",
            f"    Array[File] results = task_0.output_files",
            "  }",
            "}",
            "",
        ])
        
        # Add task definitions
        for i in range(num_tasks):
            lines.extend([
                f"task task_{i} {{",
                "  input {",
                "    String? sample_name",
                "    String? reference",
                "    Int? threshold",
                "  }",
                "",
                "  command <<<",
                f"    echo 'Running task {i}'",
                "    if [ -n '~{sample_name}' ]; then",
                "      echo 'Processing sample: ~{sample_name}'",
                "    fi",
                "    if [ -n '~{reference}' ]; then",
                "      echo 'Using reference: ~{reference}'",
                "    fi",
                "    if [ -n '~{threshold}' ]; then",
                "      echo 'Quality threshold: ~{threshold}'",
                "    fi",
                f"    echo 'Task {i} completed' > output_{i}.txt",
                "  >>>",
                "",
                "  output {",
                f"    Array[File] output_files = [\"output_{i}.txt\"]",
                "  }",
                "",
                "  runtime {",
                "    memory: \"4 GB\"",
                "    cpu: 2",
                "    disk: \"10 GB\"",
                "  }",
                "}",
                "",
            ])
        
        return "\n".join(lines)


class TestConcurrentPerformance(unittest.TestCase):
    """Test performance under concurrent usage"""
    
    def test_concurrent_linting_performance(self):
        """Test performance when linting multiple files concurrently"""
        
        import threading
        import queue
        import concurrent.futures
        
        # Create test WDL content
        wdl_content = """
version 1.0

task concurrent_test {
  input {
    String sample_name
  }
  
  command {
    echo "Processing ~{sample_name}"
    sleep 1
  }
  
  output {
    String result = stdout()
  }
  
  runtime {
    memory: "2 GB"
    cpu: 1
  }
}
"""
        
        def lint_single_file():
            """Lint a single file and return timing info"""
            start_time = time.time()
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
                tmp_file.write(wdl_content)
                tmp_file.flush()
                
                try:
                    doc = load(tmp_file.name, path=[])
                    Lint.lint(doc)
                    results = Lint.collect(doc)
                    
                    end_time = time.time()
                    return end_time - start_time, len(results)
                    
                finally:
                    os.unlink(tmp_file.name)
        
        # Test sequential execution
        sequential_start = time.time()
        sequential_results = []
        for _ in range(5):
            exec_time, findings = lint_single_file()
            sequential_results.append((exec_time, findings))
        sequential_total = time.time() - sequential_start
        
        # Test concurrent execution
        concurrent_start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(lint_single_file) for _ in range(5)]
            concurrent_results = [future.result() for future in concurrent.futures.as_completed(futures)]
        concurrent_total = time.time() - concurrent_start
        
        # Analyze results
        sequential_avg = sum(r[0] for r in sequential_results) / len(sequential_results)
        concurrent_avg = sum(r[0] for r in concurrent_results) / len(concurrent_results)
        
        print(f"\nConcurrent Performance Results:")
        print(f"  Sequential total time: {sequential_total:.3f}s")
        print(f"  Concurrent total time: {concurrent_total:.3f}s")
        print(f"  Sequential avg per file: {sequential_avg:.3f}s")
        print(f"  Concurrent avg per file: {concurrent_avg:.3f}s")
        print(f"  Speedup: {sequential_total / concurrent_total:.2f}x")
        
        # Concurrent execution should be faster than sequential
        self.assertLess(concurrent_total, sequential_total * 0.8, 
                       "Concurrent execution should be at least 20% faster")


if __name__ == "__main__":
    # Run with verbose output to see performance results
    unittest.main(verbosity=2)
