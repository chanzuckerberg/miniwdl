#!/usr/bin/env python3
import os
import unittest
import tempfile
from WDL import Lint

# Import the functions directly from the module to avoid circular imports
from WDL.LintPlugins.plugins import _is_valid_linter_class, _load_linter_from_file, discover_linters

class TestLintDiscovery(unittest.TestCase):
    def test_is_valid_linter_class(self):
        """Test the _is_valid_linter_class function"""
        # Valid linter class
        class ValidLinter(Lint.Linter):
            pass
        
        # Invalid linter classes
        class NotALinter:
            pass
        
        # Test validation
        self.assertTrue(_is_valid_linter_class(ValidLinter))
        self.assertFalse(_is_valid_linter_class(NotALinter))
        self.assertFalse(_is_valid_linter_class(Lint.Linter))  # Base class is not valid
        self.assertFalse(_is_valid_linter_class("not a class"))
    
    def test_discover_linters_basic(self):
        """Test basic linter discovery"""
        # Should return all built-in linters by default
        linters = discover_linters()
        self.assertEqual(len(linters), len(Lint._all_linters))
        
        # All returned linters should be valid
        for linter in linters:
            self.assertTrue(_is_valid_linter_class(linter))
    
    def test_discover_linters_filtering(self):
        """Test filtering linters by name and category"""
        # Get a sample linter for testing
        sample_linter = Lint._all_linters[0]
        
        # Test disabling a specific linter
        linters = discover_linters(disabled_linters=[sample_linter.__name__])
        self.assertEqual(len(linters), len(Lint._all_linters) - 1)
        self.assertNotIn(sample_linter, linters)
        
        # Test enabling only specific categories
        # First, find a linter with a known category
        style_linters = [l for l in Lint._all_linters if hasattr(l, "category") and l.category == Lint.LintCategory.STYLE]
        if style_linters:
            # If we have style linters, test filtering to only include them
            linters = discover_linters(enabled_categories=["STYLE"])
            for linter in linters:
                if hasattr(linter, "category"):
                    self.assertEqual(linter.category, Lint.LintCategory.STYLE)
        
        # Test disabling specific categories
        # First, find a linter with a known category
        portability_linters = [l for l in Lint._all_linters if hasattr(l, "category") and l.category == Lint.LintCategory.PORTABILITY]
        if portability_linters:
            # If we have portability linters, test filtering to exclude them
            linters = discover_linters(disabled_categories=["PORTABILITY"])
            for linter in linters:
                if hasattr(linter, "category"):
                    self.assertNotEqual(linter.category, Lint.LintCategory.PORTABILITY)
    
    def test_load_linter_from_file(self):
        """Test loading a linter from a file"""
        # Create a temporary file with a linter class
        with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
            f.write("""
from WDL import Lint

class TestFileLinter(Lint.Linter):
    category = Lint.LintCategory.STYLE
    default_severity = Lint.LintSeverity.MINOR
    
    def task(self, obj):
        self.add(obj, "Test message from file linter")
""")
            temp_file = f.name
        
        try:
            # Load the linter from the file
            linter_class = _load_linter_from_file(temp_file, "TestFileLinter")
            
            # Verify the linter was loaded correctly
            self.assertIsNotNone(linter_class)
            self.assertTrue(_is_valid_linter_class(linter_class))
            self.assertEqual(linter_class.__name__, "TestFileLinter")
            self.assertEqual(linter_class.category, Lint.LintCategory.STYLE)
            self.assertEqual(linter_class.default_severity, Lint.LintSeverity.MINOR)
            
            # Test discovering the linter from the file
            linters = discover_linters(additional_linters=[f"{temp_file}:TestFileLinter"])
            
            # Check that a linter with the expected name and properties was loaded
            # (use functional comparison instead of identity comparison)
            test_linters = [l for l in linters if l.__name__ == "TestFileLinter"]
            self.assertEqual(len(test_linters), 1)
            
            loaded_linter = test_linters[0]
            self.assertEqual(loaded_linter.category, Lint.LintCategory.STYLE)
            self.assertEqual(loaded_linter.default_severity, Lint.LintSeverity.MINOR)
            
        finally:
            # Clean up the temporary file
            os.unlink(temp_file)
    
    def test_invalid_linter_specs(self):
        """Test handling of invalid linter specifications"""
        # Invalid format should log warning and continue gracefully
        with self.assertLogs('wdl.lint.plugins', level='WARNING') as log:
            linters = discover_linters(additional_linters=["invalid_format"])
            # Should still return built-in linters despite invalid spec
            self.assertGreater(len(linters), 0)
            # Should log a warning about the invalid specification
            self.assertTrue(any("Invalid linter specification" in record.message for record in log.records))
        
        # Non-existent file
        linters = discover_linters(additional_linters=["/path/to/nonexistent/file.py:SomeLinter"])
        self.assertEqual(len(linters), len(Lint._all_linters))  # Should only have built-in linters
        
        # Non-existent module
        linters = discover_linters(additional_linters=["nonexistent.module:SomeLinter"])
        self.assertEqual(len(linters), len(Lint._all_linters))  # Should only have built-in linters

if __name__ == "__main__":
    unittest.main()
