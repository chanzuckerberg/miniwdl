#!/usr/bin/env python3
import unittest
from WDL import Lint
from WDL.Lint import LintSeverity, LintCategory

class TestLintSeverity(unittest.TestCase):
    def test_severity_enum(self):
        """Test that the LintSeverity enum exists and has the expected values"""
        self.assertTrue(hasattr(Lint, 'LintSeverity'))
        self.assertIsInstance(LintSeverity.MINOR, LintSeverity)
        self.assertIsInstance(LintSeverity.MODERATE, LintSeverity)
        self.assertIsInstance(LintSeverity.MAJOR, LintSeverity)
        self.assertIsInstance(LintSeverity.CRITICAL, LintSeverity)
    
    def test_category_enum(self):
        """Test that the LintCategory enum exists and has the expected values"""
        self.assertTrue(hasattr(Lint, 'LintCategory'))
        self.assertIsInstance(LintCategory.STYLE, LintCategory)
        self.assertIsInstance(LintCategory.SECURITY, LintCategory)
        self.assertIsInstance(LintCategory.PERFORMANCE, LintCategory)
        self.assertIsInstance(LintCategory.CORRECTNESS, LintCategory)
        self.assertIsInstance(LintCategory.PORTABILITY, LintCategory)
        self.assertIsInstance(LintCategory.BEST_PRACTICE, LintCategory)
        self.assertIsInstance(LintCategory.OTHER, LintCategory)
    
    def test_linter_attributes(self):
        """Test that the Linter class has the expected attributes"""
        self.assertTrue(hasattr(Lint.Linter, 'category'))
        self.assertTrue(hasattr(Lint.Linter, 'default_severity'))
        self.assertEqual(Lint.Linter.category, LintCategory.OTHER)
        self.assertEqual(Lint.Linter.default_severity, LintSeverity.MODERATE)
    
    def test_custom_linter(self):
        """Test creating a custom linter with specific category and severity"""
        class CustomLinter(Lint.Linter):
            category = LintCategory.STYLE
            default_severity = LintSeverity.MINOR
        
        self.assertEqual(CustomLinter.category, LintCategory.STYLE)
        self.assertEqual(CustomLinter.default_severity, LintSeverity.MINOR)

if __name__ == "__main__":
    unittest.main()
