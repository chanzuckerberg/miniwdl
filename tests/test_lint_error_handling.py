import unittest
import tempfile
import os
from WDL import parse_document

class TestLintErrorHandling(unittest.TestCase):
    def test_lint_error_handling(self):
        """Test that the lint function handles errors gracefully"""
        # Create a simple WDL document
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
                # Parse the document
                doc = parse_document(tmp.name)
                
                # Import the lint function
                from WDL import Lint
                
                # This should not raise an exception
                Lint.lint(doc)
                
                # Check that the document was linted
                lint_results = Lint.collect(doc)
                self.assertTrue(isinstance(lint_results, list))
                
            finally:
                os.unlink(tmp.name)


if __name__ == "__main__":
    unittest.main()
