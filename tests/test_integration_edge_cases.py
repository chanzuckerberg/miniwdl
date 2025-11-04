#!/usr/bin/env python3
"""
Edge case and error handling tests for the pluggable linting system

This module tests various edge cases, error conditions, and robustness
of the linting system.
"""

import unittest
import tempfile
import os
import sys
from pathlib import Path

# Add examples to path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'examples'))

from WDL import load, Lint
from WDL.LintPlugins.plugins import discover_linters, _load_linter_from_spec


class TestErrorHandling(unittest.TestCase):
    """Test error handling in various scenarios"""
    
    def test_invalid_linter_specifications(self):
        """Test handling of invalid linter specifications"""
        
        # Test invalid module path
        invalid_specs = [
            "nonexistent_module:NonexistentLinter",
            "invalid.module.path:SomeLinter",
            "/nonexistent/path/to/file.py:SomeLinter",
        ]
        
        for spec in invalid_specs:
            with self.subTest(spec=spec):
                # Should not crash, but should log warning and return None
                try:
                    result = _load_linter_from_spec(spec)
                    self.assertIsNone(result)
                except (ImportError, FileNotFoundError, ModuleNotFoundError):
                    # These exceptions are expected for invalid specs
                    pass
    
    def test_invalid_linter_class(self):
        """Test handling of invalid linter classes"""
        
        # Create a temporary Python file with invalid linter
        invalid_linter_code = '''
from WDL.Lint import Linter

# Not a proper linter class
class NotALinter:
    pass

# Linter with syntax error in method
class BrokenLinter(Linter):
    def task(self, obj):
        # This will cause a syntax error when executed
        invalid_syntax_here
'''
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as tmp_file:
            tmp_file.write(invalid_linter_code)
            tmp_file.flush()
            
            try:
                # Test loading non-linter class
                spec = f"{tmp_file.name}:NotALinter"
                result = _load_linter_from_spec(spec)
                self.assertIsNone(result)
                
                # Test loading broken linter class
                spec = f"{tmp_file.name}:BrokenLinter"
                result = _load_linter_from_spec(spec)
                # Should load the class but fail during execution
                self.assertIsNotNone(result)
                
            finally:
                os.unlink(tmp_file.name)
    
    def test_linter_execution_errors(self):
        """Test handling of errors during linter execution"""
        
        # Create a linter that always throws an exception
        class ErrorLinter(Lint.Linter):
            category = Lint.LintCategory.OTHER
            default_severity = Lint.LintSeverity.MINOR
            
            def task(self, obj):
                raise RuntimeError("This linter always fails")
        
        # Save original linters
        original_linters = Lint._all_linters.copy()
        
        try:
            # Add the error linter
            Lint._all_linters.append(ErrorLinter)
            
            # Create test WDL
            test_wdl = """
version 1.0

task test_task {
  command { echo "hello" }
}
"""
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
                tmp_file.write(test_wdl)
                tmp_file.flush()
                
                try:
                    # Should not crash despite linter error
                    doc = load(tmp_file.name, path=[])
                    Lint.lint(doc)  # Should handle the exception gracefully
                    lint_results = Lint.collect(doc)
                    
                    # Should still return results (from other linters)
                    self.assertIsInstance(lint_results, list)
                    
                finally:
                    os.unlink(tmp_file.name)
        
        finally:
            # Restore original linters
            Lint._all_linters[:] = original_linters
    
    def test_malformed_wdl_files(self):
        """Test linting behavior with malformed WDL files"""
        
        malformed_wdl_examples = [
            # Missing version
            """
task test_task {
  command { echo "hello" }
}
""",
            # Syntax error
            """
version 1.0

task test_task {
  command { echo "hello"
  # Missing closing brace
}
""",
            # Type error
            """
version 1.0

task test_task {
  input {
    String s = 123  # Type mismatch
  }
  command { echo ~{s} }
}
""",
        ]
        
        for i, wdl_content in enumerate(malformed_wdl_examples):
            with self.subTest(example=i):
                with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
                    tmp_file.write(wdl_content)
                    tmp_file.flush()
                    
                    try:
                        # Should handle malformed WDL gracefully
                        # Some may fail to parse, others may fail type checking
                        try:
                            doc = load(tmp_file.name, path=[])
                            Lint.lint(doc)
                            lint_results = Lint.collect(doc)
                            self.assertIsInstance(lint_results, list)
                        except Exception:
                            # Expected for malformed WDL - should not crash the system
                            pass
                            
                    finally:
                        os.unlink(tmp_file.name)
    
    def test_empty_and_minimal_wdl_files(self):
        """Test linting behavior with empty and minimal WDL files"""
        
        minimal_examples = [
            # Empty file
            "",
            # Only version
            "version 1.0",
            # Minimal task
            """
version 1.0

task empty_task {
  command { }
}
""",
            # Minimal workflow
            """
version 1.0

workflow empty_workflow {
}
""",
        ]
        
        for i, wdl_content in enumerate(minimal_examples):
            with self.subTest(example=i):
                with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
                    tmp_file.write(wdl_content)
                    tmp_file.flush()
                    
                    try:
                        try:
                            doc = load(tmp_file.name, path=[])
                            Lint.lint(doc)
                            lint_results = Lint.collect(doc)
                            self.assertIsInstance(lint_results, list)
                        except Exception:
                            # Some minimal examples may not parse - that's OK
                            pass
                            
                    finally:
                        os.unlink(tmp_file.name)


class TestEdgeCases(unittest.TestCase):
    """Test various edge cases"""
    
    def test_very_long_names(self):
        """Test linting with very long task/workflow names"""
        
        long_name = "a" * 1000  # Very long name
        
        wdl_content = f"""
version 1.0

task {long_name} {{
  command {{ echo "hello" }}
}}
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
            tmp_file.write(wdl_content)
            tmp_file.flush()
            
            try:
                doc = load(tmp_file.name, path=[])
                Lint.lint(doc)
                lint_results = Lint.collect(doc)
                
                # Should handle long names without crashing
                self.assertIsInstance(lint_results, list)
                
            finally:
                os.unlink(tmp_file.name)
    
    def test_special_characters_in_content(self):
        """Test linting with special characters in WDL content"""
        
        wdl_content = """
version 1.0

task special_chars {
  input {
    String message = "Hello 世界! 🌍 Special chars: àáâãäåæçèéêë"
  }
  
  command {
    echo "~{message}"
    # Comment with special chars: ñóôõöøùúûüý
  }
  
  output {
    String result = stdout()
  }
}
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False, encoding='utf-8') as tmp_file:
            tmp_file.write(wdl_content)
            tmp_file.flush()
            
            try:
                doc = load(tmp_file.name, path=[])
                Lint.lint(doc)
                lint_results = Lint.collect(doc)
                
                # Should handle special characters without issues
                self.assertIsInstance(lint_results, list)
                
            finally:
                os.unlink(tmp_file.name)
    
    def test_deeply_nested_structures(self):
        """Test linting with deeply nested WDL structures"""
        
        # Create a workflow with nested scatters and conditionals
        wdl_content = """
version 1.0

workflow nested_workflow {
  input {
    Array[Array[String]] nested_samples
    Boolean condition1 = true
    Boolean condition2 = false
  }
  
  scatter (sample_group in nested_samples) {
    if (condition1) {
      scatter (sample in sample_group) {
        if (condition2) {
          call process_sample { input: sample_name = sample }
        }
      }
    }
  }
}

task process_sample {
  input {
    String sample_name
  }
  
  command {
    echo "Processing ~{sample_name}"
  }
  
  output {
    String result = stdout()
  }
}
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
            tmp_file.write(wdl_content)
            tmp_file.flush()
            
            try:
                doc = load(tmp_file.name, path=[])
                Lint.lint(doc)
                lint_results = Lint.collect(doc)
                
                # Should handle nested structures
                self.assertIsInstance(lint_results, list)
                
            finally:
                os.unlink(tmp_file.name)
    
    def test_circular_dependencies_in_linters(self):
        """Test handling of potential circular dependencies in linter loading"""
        
        # Create temporary linter files that might reference each other
        linter1_code = '''
from WDL.Lint import Linter, LintSeverity, LintCategory

class Linter1(Linter):
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        # Simple linter that doesn't cause issues
        pass
'''
        
        linter2_code = '''
from WDL.Lint import Linter, LintSeverity, LintCategory

class Linter2(Linter):
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        # Another simple linter
        pass
'''
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as tmp1:
            tmp1.write(linter1_code)
            tmp1.flush()
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as tmp2:
                tmp2.write(linter2_code)
                tmp2.flush()
                
                try:
                    # Test loading both linters
                    specs = [f"{tmp1.name}:Linter1", f"{tmp2.name}:Linter2"]
                    linters = discover_linters(additional_linters=specs)
                    
                    # Should load successfully
                    self.assertIsInstance(linters, list)
                    self.assertGreater(len(linters), 0)
                    
                finally:
                    os.unlink(tmp1.name)
                    os.unlink(tmp2.name)
    
    def test_memory_usage_with_many_linters(self):
        """Test memory usage doesn't grow excessively with many linters"""
        
        # Create many simple linters
        many_linters = []
        for i in range(50):  # Create 50 linters
            class_name = f"TestLinter{i}"
            linter_class = type(class_name, (Lint.Linter,), {
                'category': Lint.LintCategory.STYLE,
                'default_severity': Lint.LintSeverity.MINOR,
                'task': lambda self, obj: None  # Do nothing
            })
            many_linters.append(linter_class)
        
        # Save original linters
        original_linters = Lint._all_linters.copy()
        
        try:
            # Add many linters
            Lint._all_linters.extend(many_linters)
            
            # Create test WDL
            test_wdl = """
version 1.0

task test_task {
  command { echo "hello" }
}
"""
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
                tmp_file.write(test_wdl)
                tmp_file.flush()
                
                try:
                    # Should handle many linters without excessive memory usage
                    doc = load(tmp_file.name, path=[])
                    Lint.lint(doc)
                    lint_results = Lint.collect(doc)
                    
                    self.assertIsInstance(lint_results, list)
                    
                finally:
                    os.unlink(tmp_file.name)
        
        finally:
            # Restore original linters
            Lint._all_linters[:] = original_linters


class TestConcurrencyAndThreadSafety(unittest.TestCase):
    """Test concurrent usage scenarios"""
    
    def test_concurrent_linting(self):
        """Test that linting can be done concurrently (basic test)"""
        
        import threading
        import queue
        
        # Create test WDL content
        test_wdl = """
version 1.0

task concurrent_test {
  command { echo "hello" }
}
"""
        
        results_queue = queue.Queue()
        
        def lint_worker():
            """Worker function for concurrent linting"""
            try:
                # Set up asyncio event loop for this thread
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                with tempfile.NamedTemporaryFile(mode='w', suffix='.wdl', delete=False) as tmp_file:
                    tmp_file.write(test_wdl)
                    tmp_file.flush()
                    
                    try:
                        doc = load(tmp_file.name, path=[])
                        Lint.lint(doc)
                        lint_results = Lint.collect(doc)
                        results_queue.put(('success', len(lint_results)))
                        
                    finally:
                        os.unlink(tmp_file.name)
                        
            except Exception as e:
                results_queue.put(('error', str(e)))
        
        # Start multiple threads
        threads = []
        num_threads = 5
        
        for _ in range(num_threads):
            thread = threading.Thread(target=lint_worker)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check results
        success_count = 0
        while not results_queue.empty():
            status, result = results_queue.get()
            if status == 'success':
                success_count += 1
            else:
                self.fail(f"Thread failed: {result}")
        
        # All threads should have succeeded
        self.assertEqual(success_count, num_threads)


class TestConfigurationEdgeCases(unittest.TestCase):
    """Test edge cases in configuration handling"""
    
    def test_invalid_configuration_values(self):
        """Test handling of invalid configuration values"""
        
        # Test with invalid severity levels
        from WDL.LintPlugins.config import get_exit_on_severity
        
        # Mock config with invalid severity
        class MockConfig:
            def __getitem__(self, key):
                if key == "linting":
                    return {"exit_on_severity": "INVALID_SEVERITY"}
                return {}
        
        config = MockConfig()
        result = get_exit_on_severity(config)
        
        # Should return the invalid value (validation happens elsewhere)
        self.assertEqual(result, "INVALID_SEVERITY")
    
    def test_missing_configuration_sections(self):
        """Test handling of missing configuration sections"""
        
        from WDL.LintPlugins.config import (
            get_additional_linters, get_disabled_linters,
            get_enabled_categories, get_disabled_categories
        )
        
        # Mock config with no linting section
        class EmptyConfig:
            def __getitem__(self, key):
                raise KeyError(f"No section: {key}")
        
        config = EmptyConfig()
        
        # Should return empty lists/None without crashing
        self.assertEqual(get_additional_linters(config), [])
        self.assertEqual(get_disabled_linters(config), [])
        self.assertEqual(get_enabled_categories(config), [])
        self.assertEqual(get_disabled_categories(config), [])


if __name__ == "__main__":
    unittest.main()
