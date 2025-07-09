import unittest
import tempfile
import os

class TestLintErrorHandling(unittest.TestCase):
    def test_lint_error_handling(self):
        """Test that the lint function handles errors gracefully"""
        # Create a valid WDL document
        wdl_content = """
version 1.0
task foo {
  command { echo "hello" }
  output { String out = stdout() }
}
"""
        
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".wdl", delete=False) as tmp:
            tmp.write(wdl_content)
            tmp.close()
            
            try:
                # Load the document using WDL.load (proper way)
                import WDL
                doc = WDL.load(tmp.name)
                
                # Test that linting works normally
                WDL.Lint.lint(doc)
                lint_results = WDL.Lint.collect(doc)
                self.assertTrue(isinstance(lint_results, list))
                
                # Test error handling by creating a linter that throws an exception
                class ErrorLinter(WDL.Lint.Linter):
                    category = WDL.Lint.LintCategory.OTHER
                    default_severity = WDL.Lint.LintSeverity.MINOR
                    
                    def task(self, obj):
                        raise RuntimeError("Test error in linter")
                
                # Add the error linter temporarily
                original_linters = WDL.Lint._all_linters.copy()
                WDL.Lint._all_linters.append(ErrorLinter)
                
                try:
                    # This should not crash despite the error linter
                    with self.assertLogs('wdl.lint.safe_walker', level='WARNING') as log:
                        WDL.Lint.lint(doc)
                        # Should log a warning about the error in the linter
                        self.assertTrue(any("Error in linter" in record.message for record in log.records))
                    
                    # Should still be able to collect results from other linters
                    lint_results = WDL.Lint.collect(doc)
                    self.assertTrue(isinstance(lint_results, list))
                    
                finally:
                    # Restore original linters
                    WDL.Lint._all_linters[:] = original_linters
                
            finally:
                os.unlink(tmp.name)


if __name__ == "__main__":
    unittest.main()
