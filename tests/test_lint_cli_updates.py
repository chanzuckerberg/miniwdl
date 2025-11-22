import unittest
import os
import tempfile
import subprocess
import sys
from pathlib import Path


class TestLintCLIUpdates(unittest.TestCase):
    def setUp(self):
        # Create a temporary WDL file with a lint issue
        self.temp_wdl = tempfile.NamedTemporaryFile(mode="w+", suffix=".wdl", delete=False)
        self.temp_wdl.write("""
version 1.0

task foo {
  input {
    String? s = "hello"
  }
  
  command {
    echo ~{s}
  }
  
  output {
    String out = stdout()
  }
}
""")
        self.temp_wdl.close()
        
        # Path to the miniwdl CLI script
        self.miniwdl_path = sys.executable + " -m WDL"
    
    def tearDown(self):
        # Remove temporary WDL file
        os.unlink(self.temp_wdl.name)
    
    def test_list_linters(self):
        """Test the --list-linters option"""
        cmd = f"{self.miniwdl_path} check --list-linters"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        self.assertIn("Available linters:", result.stdout)
        self.assertIn("Built-in linters:", result.stdout)
        self.assertIn("Name", result.stdout)
        self.assertIn("Category", result.stdout)
        self.assertIn("Default Severity", result.stdout)
        
    def test_linter_categories(self):
        """Test the category filtering options"""
        # Test enabling specific categories
        cmd = f"{self.miniwdl_path} check --enable-lint-categories STYLE,SECURITY {self.temp_wdl.name}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        
        # Test disabling specific categories
        cmd = f"{self.miniwdl_path} check --disable-lint-categories CORRECTNESS {self.temp_wdl.name}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        
        # Test that the output changes when categories are filtered
        # First get baseline output
        cmd_baseline = f"{self.miniwdl_path} check {self.temp_wdl.name}"
        result_baseline = subprocess.run(cmd_baseline, shell=True, capture_output=True, text=True)
        
        # Then get output with category filtering
        cmd_filtered = f"{self.miniwdl_path} check --enable-lint-categories STYLE {self.temp_wdl.name}"
        result_filtered = subprocess.run(cmd_filtered, shell=True, capture_output=True, text=True)
        
        # The filtered output should be different (likely shorter) than baseline
        # This validates that category filtering is working
        self.assertNotEqual(result_baseline.stdout, result_filtered.stdout)
        
    def test_exit_on_severity(self):
        """Test the --exit-on-lint-severity option"""
        # Use the contrived.wdl file which is known to have lint issues
        contrived_wdl = "test_corpi/contrived/contrived.wdl"
        
        # Test that exit code is 0 when no severity threshold is set
        cmd = f"{self.miniwdl_path} check {contrived_wdl}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        
        # Test that exit code is non-zero when severity threshold is set to MINOR
        # (contrived.wdl has many MODERATE issues which are >= MINOR)
        cmd = f"{self.miniwdl_path} check --exit-on-lint-severity MINOR {contrived_wdl}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        # Should exit with non-zero code due to lint findings
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Found lint issues with severity", result.stderr)
        
        # Test that exit code is non-zero when severity threshold is set to MODERATE
        # (contrived.wdl has many MODERATE issues)
        cmd = f"{self.miniwdl_path} check --exit-on-lint-severity MODERATE {contrived_wdl}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        # Should exit with non-zero code due to lint findings
        self.assertNotEqual(result.returncode, 0)
        
        # Test that exit code is 0 when severity threshold is set very high
        cmd = f"{self.miniwdl_path} check --exit-on-lint-severity CRITICAL {contrived_wdl}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        # Should exit with 0 code since there are likely no CRITICAL findings
        self.assertEqual(result.returncode, 0)
        
        # Test invalid severity level - should handle gracefully
        cmd = f"{self.miniwdl_path} check --exit-on-lint-severity INVALID {contrived_wdl}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        # Should handle invalid severity gracefully (exit 0 and show warning)
        self.assertEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
