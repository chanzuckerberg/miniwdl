#!/usr/bin/env python3
import unittest
from WDL import Lint

class TestLinterTestingFramework(unittest.TestCase):
    def test_linter_testing_framework(self):
        """Test the linter testing framework"""
        
        # Define a simple test linter
        class TestLinter(Lint.Linter):
            """Test linter that flags tasks named 'foo'"""
            
            category = Lint.LintCategory.STYLE
            default_severity = Lint.LintSeverity.MINOR
            
            def task(self, obj):
                if obj.name == "foo":
                    self.add(
                        obj,
                        f"Task name '{obj.name}' should not be 'foo'",
                        obj.pos,
                        severity=Lint.LintSeverity.MINOR
                    )
        
        # Test with a WDL fragment that should trigger a warning
        results = Lint.test_linter(
            TestLinter,
            """
            task foo {
              command { echo "Hello" }
            }
            """,
            expected_lint=["Task name 'foo' should not be 'foo'"]
        )
        
        # Verify the result structure
        self.assertEqual(len(results), 1)
        pos, linter_class, message, suppressed, severity = results[0]
        self.assertEqual(linter_class, "TestLinter")
        self.assertIn("Task name 'foo' should not be 'foo'", message)
        self.assertEqual(severity, Lint.LintSeverity.MINOR)
        self.assertFalse(suppressed)
        
        # Test with a WDL fragment that should not trigger any warnings
        results = Lint.test_linter(
            TestLinter,
            """
            task bar {
              command { echo "Hello" }
            }
            """,
            expected_lint=[]
        )
        
        # Should have no results
        self.assertEqual(len(results), 0)
    
    def test_test_linter_with_count(self):
        """Test test_linter with expected_count parameter"""
        
        class MultiLinter(Lint.Linter):
            """Test linter that flags multiple issues"""
            
            category = Lint.LintCategory.STYLE
            default_severity = Lint.LintSeverity.MINOR
            
            def task(self, obj):
                if obj.name.startswith("bad"):
                    self.add(
                        obj,
                        f"Task name '{obj.name}' starts with 'bad'",
                        obj.pos
                    )
        
        # Test with expected count
        results = Lint.test_linter(
            MultiLinter,
            """
            task bad_task1 {
              command { echo "Hello" }
            }
            
            task bad_task2 {
              command { echo "World" }
            }
            
            task good_task {
              command { echo "Good" }
            }
            """,
            expected_count=2
        )
        
        self.assertEqual(len(results), 2)
    
    def test_test_linter_validation(self):
        """Test test_linter input validation"""
        
        # Test with invalid linter class
        with self.assertRaises(ValueError):
            Lint.test_linter(
                "NotAClass",
                "task foo { command { echo 'hello' } }"
            )
        
        # Test with class that's not a Linter subclass
        class NotALinter:
            pass
        
        with self.assertRaises(ValueError):
            Lint.test_linter(
                NotALinter,
                "task foo { command { echo 'hello' } }"
            )
    
    def test_test_linter_assertion_failures(self):
        """Test test_linter assertion failures"""
        
        class TestLinter(Lint.Linter):
            category = Lint.LintCategory.STYLE
            default_severity = Lint.LintSeverity.MINOR
            
            def task(self, obj):
                if obj.name == "foo":
                    self.add(obj, "Found foo task", obj.pos)
        
        # Test expecting lint but getting none
        with self.assertRaises(AssertionError):
            Lint.test_linter(
                TestLinter,
                "task bar { command { echo 'hello' } }",
                expected_lint=["Should find something"]
            )
        
        # Test expecting no lint but getting some
        with self.assertRaises(AssertionError):
            Lint.test_linter(
                TestLinter,
                "task foo { command { echo 'hello' } }",
                expected_lint=[]
            )
        
        # Test wrong message content
        with self.assertRaises(AssertionError):
            Lint.test_linter(
                TestLinter,
                "task foo { command { echo 'hello' } }",
                expected_lint=["Wrong message"]
            )
    
    def test_create_test_wdl(self):
        """Test the create_test_wdl utility function"""
        
        # Test basic task creation
        wdl_code = Lint.create_test_wdl()
        self.assertIn("version 1.0", wdl_code)
        self.assertIn("task test_task", wdl_code)
        self.assertIn("echo 'hello'", wdl_code)
        
        # Test with custom parameters
        wdl_code = Lint.create_test_wdl(
            task_name="my_task",
            command="echo ~{message}",
            inputs={"message": "String"},
            outputs={"result": "String"}
        )
        
        self.assertIn("task my_task", wdl_code)
        self.assertIn("input {", wdl_code)
        self.assertIn("String message", wdl_code)
        self.assertIn("output {", wdl_code)
        self.assertIn("String result", wdl_code)
        self.assertIn("echo ~{message}", wdl_code)
    
    def test_assert_lint_contains(self):
        """Test the assert_lint_contains helper function"""
        
        # Create some mock lint results
        lint_results = [
            (None, "TestLinter", "This is a test message", False, Lint.LintSeverity.MINOR),
            (None, "OtherLinter", "Another message", False, Lint.LintSeverity.MAJOR),
        ]
        
        # Test successful assertion
        Lint.assert_lint_contains(lint_results, "test message")
        Lint.assert_lint_contains(lint_results, "Another message", "OtherLinter")
        
        # Test failed assertion
        with self.assertRaises(AssertionError):
            Lint.assert_lint_contains(lint_results, "nonexistent message")
        
        with self.assertRaises(AssertionError):
            Lint.assert_lint_contains(lint_results, "test message", "WrongLinter")
    
    def test_assert_lint_count(self):
        """Test the assert_lint_count helper function"""
        
        lint_results = [
            (None, "TestLinter", "Message 1", False, Lint.LintSeverity.MINOR),
            (None, "TestLinter", "Message 2", False, Lint.LintSeverity.MINOR),
            (None, "OtherLinter", "Message 3", False, Lint.LintSeverity.MAJOR),
        ]
        
        # Test successful assertions
        Lint.assert_lint_count(lint_results, 3)
        Lint.assert_lint_count(lint_results, 2, "TestLinter")
        Lint.assert_lint_count(lint_results, 1, "OtherLinter")
        
        # Test failed assertions
        with self.assertRaises(AssertionError):
            Lint.assert_lint_count(lint_results, 5)
        
        with self.assertRaises(AssertionError):
            Lint.assert_lint_count(lint_results, 3, "TestLinter")
    
    def test_assert_lint_severity(self):
        """Test the assert_lint_severity helper function"""
        
        lint_results = [
            (None, "TestLinter", "Minor message", False, Lint.LintSeverity.MINOR),
            (None, "TestLinter", "Another minor", False, Lint.LintSeverity.MINOR),
            (None, "OtherLinter", "Major message", False, Lint.LintSeverity.MAJOR),
        ]
        
        # Test successful assertions
        Lint.assert_lint_severity(lint_results, Lint.LintSeverity.MINOR, "TestLinter")
        Lint.assert_lint_severity(lint_results, Lint.LintSeverity.MAJOR, "OtherLinter")
        
        # Test failed assertion
        with self.assertRaises(AssertionError):
            Lint.assert_lint_severity(lint_results, Lint.LintSeverity.MAJOR, "TestLinter")
    
    def test_different_severity_levels(self):
        """Test linters with different severity levels"""
        
        class SeverityTestLinter(Lint.Linter):
            category = Lint.LintCategory.SECURITY
            default_severity = Lint.LintSeverity.CRITICAL
            
            def task(self, obj):
                if "dangerous" in obj.name:
                    self.add(
                        obj,
                        "Dangerous task detected",
                        obj.pos,
                        severity=Lint.LintSeverity.CRITICAL
                    )
        
        results = Lint.test_linter(
            SeverityTestLinter,
            """
            task dangerous_task {
              command { echo "danger" }
            }
            """,
            expected_lint=["Dangerous task detected"]
        )
        
        # Verify severity
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][4], Lint.LintSeverity.CRITICAL)
    
    def test_different_categories(self):
        """Test linters with different categories"""
        
        class StyleLinter(Lint.Linter):
            category = Lint.LintCategory.STYLE
            default_severity = Lint.LintSeverity.MINOR
            
            def task(self, obj):
                if obj.name.isupper():
                    self.add(obj, "Task name should not be all uppercase", obj.pos)
        
        class SecurityLinter(Lint.Linter):
            category = Lint.LintCategory.SECURITY
            default_severity = Lint.LintSeverity.MAJOR
            
            def task(self, obj):
                if "rm" in str(obj.command):
                    self.add(obj, "Potentially dangerous rm command", obj.pos)
        
        # Test style linter
        results = Lint.test_linter(
            StyleLinter,
            """
            task UPPERCASE_TASK {
              command { echo "hello" }
            }
            """,
            expected_lint=["Task name should not be all uppercase"]
        )
        
        self.assertEqual(len(results), 1)
        
        # Test security linter
        results = Lint.test_linter(
            SecurityLinter,
            """
            task cleanup {
              command { rm -rf /tmp/data }
            }
            """,
            expected_lint=["Potentially dangerous rm command"]
        )
        
        self.assertEqual(len(results), 1)
    
    def test_version_handling(self):
        """Test WDL version handling in test framework"""
        
        class VersionLinter(Lint.Linter):
            category = Lint.LintCategory.CORRECTNESS
            default_severity = Lint.LintSeverity.MINOR
            
            def task(self, obj):
                self.add(obj, "Version test", obj.pos)
        
        # Test with explicit version
        results = Lint.test_linter(
            VersionLinter,
            """
            version 1.1
            task test {
              command { echo "hello" }
            }
            """,
            expected_count=1
        )
        
        self.assertEqual(len(results), 1)
        
        # Test with version parameter
        results = Lint.test_linter(
            VersionLinter,
            """
            task test {
              command { echo "hello" }
            }
            """,
            expected_count=1,
            version="1.1"
        )
        
        self.assertEqual(len(results), 1)


if __name__ == "__main__":
    unittest.main()
