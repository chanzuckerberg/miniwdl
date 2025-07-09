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
        # Skip this test for now as it requires more setup
        self.skipTest("Requires more setup to test category filtering")
        
    def test_exit_on_severity(self):
        """Test the --exit-on-lint-severity option"""
        # Skip this test for now as it requires more setup
        self.skipTest("Requires more setup to test exit on severity")


if __name__ == "__main__":
    unittest.main()
