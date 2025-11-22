#!/usr/bin/env python3
import unittest
import tempfile
import os
import subprocess
import sys

class TestLintExitCodesSimple(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        # Get the path to the miniwdl executable
        self.miniwdl_path = sys.executable + " -m WDL"
    
    def test_exit_code_with_no_findings(self):
        """Test exit code behavior with no findings"""
        # Create WDL content with no lint issues
        wdl_content = R"""
version 1.0

task clean_task {
  command { echo "hello" }
  output { String out = stdout() }
}
"""
        
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".wdl", delete=False) as tmp:
            tmp.write(wdl_content)
            tmp.close()
            
            try:
                # Test with various severity thresholds - all should succeed
                for severity in ["MINOR", "MODERATE", "MAJOR", "CRITICAL"]:
                    cmd = f"{self.miniwdl_path} check --exit-on-lint-severity {severity} {tmp.name}"
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, f"Expected success with {severity} threshold but got error: {result.stderr}")
                
                # Test with --strict - should also succeed since no findings
                cmd = f"{self.miniwdl_path} check --strict {tmp.name}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, f"Expected success with --strict but got error: {result.stderr}")
                
            finally:
                os.unlink(tmp.name)
    
    def test_strict_option_with_findings(self):
        """Test --strict option with lint findings"""
        # Create WDL content that will trigger UnnecessaryQuantifier (MODERATE severity)
        wdl_content = R"""
version 1.0

task test_task {
  input {
    String? s = "hello"  # This should trigger UnnecessaryQuantifier
  }
  command { echo ~{s} }
  output { String out = stdout() }
}
"""
        
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".wdl", delete=False) as tmp:
            tmp.write(wdl_content)
            tmp.close()
            
            try:
                # Test without --strict - should succeed
                cmd = f"{self.miniwdl_path} check {tmp.name}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, f"Expected success without --strict but got error: {result.stderr}")
                
                # Test with --strict - should fail
                cmd = f"{self.miniwdl_path} check --strict {tmp.name}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0, f"Expected error with --strict but got success: {result.stdout}")
                
            finally:
                os.unlink(tmp.name)
    
    def test_exit_on_severity_with_findings(self):
        """Test exit-on-severity with lint findings"""
        # Create WDL content that will trigger UnnecessaryQuantifier (MODERATE severity)
        wdl_content = R"""
version 1.0

task test_task {
  input {
    String? s = "hello"  # This should trigger UnnecessaryQuantifier
  }
  command { echo ~{s} }
  output { String out = stdout() }
}
"""
        
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".wdl", delete=False) as tmp:
            tmp.write(wdl_content)
            tmp.close()
            
            try:
                # Test with CRITICAL threshold - should succeed (MODERATE < CRITICAL)
                cmd = f"{self.miniwdl_path} check --exit-on-lint-severity CRITICAL {tmp.name}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, f"Expected success with CRITICAL threshold but got error: {result.stderr}")
                
                # Test with MAJOR threshold - should succeed (MODERATE < MAJOR)
                cmd = f"{self.miniwdl_path} check --exit-on-lint-severity MAJOR {tmp.name}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, f"Expected success with MAJOR threshold but got error: {result.stderr}")
                
                # Test with MODERATE threshold - should fail (MODERATE >= MODERATE)
                cmd = f"{self.miniwdl_path} check --exit-on-lint-severity MODERATE {tmp.name}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0, f"Expected error with MODERATE threshold but got success: {result.stdout}")
                
                # Test with MINOR threshold - should fail (MODERATE >= MINOR)
                cmd = f"{self.miniwdl_path} check --exit-on-lint-severity MINOR {tmp.name}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0, f"Expected error with MINOR threshold but got success: {result.stdout}")
                
            finally:
                os.unlink(tmp.name)
    
    def test_invalid_severity_level(self):
        """Test behavior with invalid severity level"""
        # Create simple WDL content
        wdl_content = R"""
version 1.0

task simple_task {
  command { echo "hello" }
  output { String out = stdout() }
}
"""
        
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".wdl", delete=False) as tmp:
            tmp.write(wdl_content)
            tmp.close()
            
            try:
                # Test with invalid severity level
                cmd = f"{self.miniwdl_path} check --exit-on-lint-severity INVALID {tmp.name}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                # Should succeed but show warning
                self.assertEqual(result.returncode, 0, f"Expected success despite invalid severity but got error: {result.stderr}")
                self.assertIn("Invalid severity level 'INVALID'", result.stderr)
                
            finally:
                os.unlink(tmp.name)
    
    def test_strict_precedence_over_severity(self):
        """Test that --strict takes precedence over --exit-on-lint-severity"""
        # Create WDL content that will trigger UnnecessaryQuantifier (MODERATE severity)
        wdl_content = R"""
version 1.0

task test_task {
  input {
    String? s = "hello"  # This should trigger UnnecessaryQuantifier
  }
  command { echo ~{s} }
  output { String out = stdout() }
}
"""
        
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".wdl", delete=False) as tmp:
            tmp.write(wdl_content)
            tmp.close()
            
            try:
                # Test with both --strict and --exit-on-lint-severity CRITICAL
                # --strict should take precedence and cause exit with error
                cmd = f"{self.miniwdl_path} check --strict --exit-on-lint-severity CRITICAL {tmp.name}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0, f"Expected error with --strict precedence but got success: {result.stdout}")
                
            finally:
                os.unlink(tmp.name)


if __name__ == "__main__":
    unittest.main()
