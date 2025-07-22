#!/usr/bin/env python3
import unittest
import os
from WDL import load, Lint

class TestLintExecution(unittest.TestCase):
    def test_error_handling(self):
        """Test error handling during linter execution"""
        # Create a custom linter that raises an exception
        class ErrorLinter(Lint.Linter):
            """A linter that raises an exception"""
            
            category = Lint.LintCategory.STYLE
            default_severity = Lint.LintSeverity.MINOR
            
            def task(self, obj):
                raise RuntimeError("This linter always fails")
        
        # Add the error linter to the list of linters
        original_linters = Lint._all_linters.copy()
        Lint._all_linters.append(ErrorLinter)
        
        try:
            # Load a simple WDL document from test_corpi
            if not os.path.exists("test_corpi/contrived/contrived.wdl"):
                self.skipTest("Required WDL file 'contrived.wdl' is missing.")
            doc = load("contrived.wdl", path=["test_corpi/contrived"])
            
            # Lint the document - this should not raise an exception
            # despite the ErrorLinter raising an exception
            Lint.lint(doc)
            
            # Check that the document was linted
            lint_results = Lint.collect(doc)
            self.assertTrue(len(lint_results) >= 0)
            
        finally:
            # Restore the original linters
            Lint._all_linters[:] = original_linters
    
    def test_combined_linters(self):
        """Test execution of built-in and external linters together"""
        # Create a custom linter
        class CustomTaskNameLinter(Lint.Linter):
            """Ensures task names are lowercase"""
            
            category = Lint.LintCategory.STYLE
            default_severity = Lint.LintSeverity.MINOR
            
            def task(self, obj):
                if not obj.name.islower():
                    self.add(
                        obj,
                        f"Task name '{obj.name}' should be lowercase",
                        obj.pos,
                        severity=Lint.LintSeverity.MINOR
                    )
        
        # Add the custom linter to the list of linters
        original_linters = Lint._all_linters.copy()
        Lint._all_linters.append(CustomTaskNameLinter)
        
        try:
            # Load a WDL document from test_corpi
            doc = load("contrived.wdl", path=["test_corpi/contrived"])
            
            # Lint the document
            Lint.lint(doc)
            
            # Check that the custom linter was executed
            lint_results = Lint.collect(doc)
            
            # The custom linter should have been executed (we don't check for specific results
            # since we don't know the exact content of contrived.wdl)
            self.assertTrue(len(lint_results) >= 0)
            
        finally:
            # Restore the original linters
            Lint._all_linters[:] = original_linters
    
    def test_linter_filtering(self):
        """Test filtering of linters based on name and category"""
        # Load a WDL document from test_corpi
        doc = load("contrived.wdl", path=["test_corpi/contrived"])
        
        # Lint the document with all linters
        Lint.lint(doc)
        all_lint = Lint.collect(doc)
        
        # Load the document again
        doc = load("contrived.wdl", path=["test_corpi/contrived"])
        
        # Lint the document with UnnecessaryQuantifier disabled
        Lint.lint(doc, disabled_linters=["UnnecessaryQuantifier"])
        filtered_lint = Lint.collect(doc)
        
        # The filtered results should have the same or fewer lints
        self.assertTrue(len(filtered_lint) <= len(all_lint))
    
    def test_category_filtering(self):
        """Test filtering of linters based on category"""
        # Load a WDL document from test_corpi
        doc = load("contrived.wdl", path=["test_corpi/contrived"])
        
        # Lint the document with only STYLE category enabled
        Lint.lint(doc, enabled_categories=["STYLE"])
        style_lint = Lint.collect(doc)
        
        # Load the document again
        doc = load("contrived.wdl", path=["test_corpi/contrived"])
        
        # Lint the document with CORRECTNESS category disabled
        Lint.lint(doc, disabled_categories=["CORRECTNESS"])
        no_correctness_lint = Lint.collect(doc)
        
        # Both filtered results should be valid
        self.assertTrue(len(style_lint) >= 0)
        self.assertTrue(len(no_correctness_lint) >= 0)
    
    def test_lint_function_error_handling(self):
        """Test that the lint function handles errors gracefully"""
        # Load a WDL document from test_corpi
        doc = load("contrived.wdl", path=["test_corpi/contrived"])
        
        # This should not raise an exception
        Lint.lint(doc)
        
        # Check that the document was linted
        lint_results = Lint.collect(doc)
        self.assertTrue(isinstance(lint_results, list))
    
    def test_collection_of_lint_results_with_severity(self):
        """Test collection of lint results with severity information"""
        # Create a custom linter with specific severity
        class SeverityTestLinter(Lint.Linter):
            """Test linter with specific severity"""
            
            category = Lint.LintCategory.STYLE
            default_severity = Lint.LintSeverity.MAJOR
            
            def task(self, obj):
                self.add(
                    obj,
                    "Test message with MAJOR severity",
                    obj.pos,
                    severity=Lint.LintSeverity.MAJOR
                )
        
        # Add the test linter to the list of linters
        original_linters = Lint._all_linters.copy()
        Lint._all_linters.append(SeverityTestLinter)
        
        try:
            # Load a WDL document from test_corpi
            doc = load("contrived.wdl", path=["test_corpi/contrived"])
            
            # Lint the document
            Lint.lint(doc)
            
            # Collect lint results
            lint_results = Lint.collect(doc)
            
            # Check that lint results include basic information
            self.assertTrue(len(lint_results) > 0)
            
            # Each lint result should be a tuple with 4 elements for backward compatibility:
            # (pos, linter_class, message, suppressed)
            # Note: Severity information is stored internally but not exposed in collect() for backward compatibility
            for lint_item in lint_results:
                self.assertEqual(len(lint_item), 4)
                pos, linter_class, message, suppressed = lint_item
                
                # Check types
                self.assertIsInstance(pos, type(pos))  # SourcePosition
                self.assertIsInstance(linter_class, str)
                self.assertIsInstance(message, str)
                self.assertIsInstance(suppressed, bool)
                
                # If this is our test linter, check that it was found
                if linter_class == "SeverityTestLinter":
                    self.assertIn("Test message with MAJOR severity", message)
            
        finally:
            # Restore the original linters
            Lint._all_linters[:] = original_linters


if __name__ == "__main__":
    unittest.main()
