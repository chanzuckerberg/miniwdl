#!/usr/bin/env python3
"""
Example demonstrating the miniwdl linter testing framework.

This example shows how to create custom linters and test them using
the built-in testing framework.
"""

from WDL.Lint import (
    validate_linter, create_test_wdl, assert_lint_contains, assert_lint_count,
    Linter, LintSeverity, LintCategory
)


class TaskNamingLinter(Linter):
    """
    A linter that enforces task naming conventions.
    
    Rules:
    - Task names should be lowercase
    - Task names should use underscores, not camelCase
    - Task names should not be too long (>30 characters)
    """
    
    category = LintCategory.STYLE
    default_severity = LintSeverity.MINOR
    
    def task(self, obj):
        name = obj.name
        
        # Check for camelCase or uppercase (prefer more specific message)
        if any(c.isupper() for c in name):
            if name.isupper():
                self.add(
                    obj,
                    f"Task name '{name}' should be lowercase",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
            elif any(c.isupper() for c in name[1:]):  # camelCase
                self.add(
                    obj,
                    f"Task name '{name}' should use snake_case instead of camelCase",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
            else:  # First letter uppercase only
                self.add(
                    obj,
                    f"Task name '{name}' should be lowercase",
                    obj.pos,
                    severity=LintSeverity.MINOR
                )
        
        # Check length
        if len(name) > 30:
            self.add(
                obj,
                f"Task name '{name}' is too long ({len(name)} characters, max 30)",
                obj.pos,
                severity=LintSeverity.MODERATE
            )


class SecurityLinter(Linter):
    """
    A linter that checks for potential security issues.
    
    Rules:
    - Warn about potentially dangerous commands
    - Flag use of sudo or su
    - Check for hardcoded credentials patterns
    """
    
    category = LintCategory.SECURITY
    default_severity = LintSeverity.MAJOR
    
    def task(self, obj):
        command_str = str(obj.command).lower()
        
        # Check for dangerous commands
        dangerous_commands = ['rm -rf', 'dd if=', 'mkfs', 'fdisk']
        for cmd in dangerous_commands:
            if cmd in command_str:
                self.add(
                    obj,
                    f"Potentially dangerous command detected: {cmd}",
                    obj.pos,
                    severity=LintSeverity.MAJOR
                )
        
        # Check for privilege escalation
        if 'sudo' in command_str or 'su ' in command_str:
            self.add(
                obj,
                "Privilege escalation detected (sudo/su)",
                obj.pos,
                severity=LintSeverity.CRITICAL
            )
        
        # Check for potential credentials
        credential_patterns = ['password=', 'passwd=', 'secret=', 'token=']
        for pattern in credential_patterns:
            if pattern in command_str:
                self.add(
                    obj,
                    f"Potential hardcoded credential detected: {pattern}",
                    obj.pos,
                    severity=LintSeverity.CRITICAL
                )


def test_task_naming_linter():
    """Test the TaskNamingLinter with various scenarios."""
    
    print("Testing TaskNamingLinter...")
    
    # Test 1: Valid task name (should pass)
    validate_linter(
        TaskNamingLinter,
        """
        task good_task_name {
          command { echo "hello" }
        }
        """,
        expected_lint=[]
    )
    print("✅ Valid task name test passed")
    
    # Test 2: Uppercase task name (should fail)
    results = validate_linter(
        TaskNamingLinter,
        """
        task BadTaskName {
          command { echo "hello" }
        }
        """,
        expected_lint=["should use snake_case instead of camelCase"]
    )
    print("✅ CamelCase task name test passed")
    
    # Test 3: All uppercase task name (should fail)
    validate_linter(
        TaskNamingLinter,
        """
        task UPPERCASE_TASK {
          command { echo "hello" }
        }
        """,
        expected_lint=["should be lowercase"]
    )
    print("✅ All uppercase task name test passed")
    
    # Test 4: Task name with camelCase (should fail)
    validate_linter(
        TaskNamingLinter,
        """
        task taskWithCamelCase {
          command { echo "hello" }
        }
        """,
        expected_lint=["should use snake_case instead of camelCase"]
    )
    print("✅ CamelCase task name test passed")
    
    # Test 5: Very long task name (should fail with MODERATE severity)
    long_name = "very_long_task_name_that_exceeds_the_thirty_character_limit"
    wdl_code = create_test_wdl(task_name=long_name)
    
    results = validate_linter(
        TaskNamingLinter,
        wdl_code,
        expected_count=1
    )
    
    # Verify it's the right severity
    assert results[0][4] == LintSeverity.MODERATE
    print("✅ Long task name test passed")
    
    # Test 6: Multiple issues
    validate_linter(
        TaskNamingLinter,
        """
        task BadTaskNameThatIsWayTooLongAndHasMultipleIssues {
          command { echo "hello" }
        }
        """,
        expected_count=2  # camelCase and length
    )
    print("✅ Multiple issues test passed")


def test_security_linter():
    """Test the SecurityLinter with various scenarios."""
    
    print("\nTesting SecurityLinter...")
    
    # Test 1: Safe command (should pass)
    validate_linter(
        SecurityLinter,
        """
        task safe_task {
          command { echo "hello world" }
        }
        """,
        expected_lint=[]
    )
    print("✅ Safe command test passed")
    
    # Test 2: Dangerous rm command (should fail)
    validate_linter(
        SecurityLinter,
        """
        task cleanup {
          command { rm -rf /tmp/data }
        }
        """,
        expected_lint=["Potentially dangerous command detected: rm -rf"]
    )
    print("✅ Dangerous rm command test passed")
    
    # Test 3: Sudo usage (should fail with CRITICAL)
    results = validate_linter(
        SecurityLinter,
        """
        task install_package {
          command { sudo apt-get install package }
        }
        """,
        expected_lint=["Privilege escalation detected (sudo/su)"]
    )
    
    # Verify it's CRITICAL severity
    assert results[0][4] == LintSeverity.CRITICAL
    print("✅ Sudo usage test passed")
    
    # Test 4: Hardcoded credentials (should fail with CRITICAL)
    results = validate_linter(
        SecurityLinter,
        """
        task connect_db {
          command { mysql -u user -ppassword=secret123 }
        }
        """,
        expected_lint=["Potential hardcoded credential detected"]
    )
    
    assert results[0][4] == LintSeverity.CRITICAL
    print("✅ Hardcoded credentials test passed")


def test_using_assertion_helpers():
    """Demonstrate using the assertion helper functions."""
    
    print("\nTesting assertion helpers...")
    
    # Run a linter and get results
    results = validate_linter(
        TaskNamingLinter,
        """
        task BadTaskName {
          command { echo "hello" }
        }
        """,
        expected_count=1  # camelCase only
    )
    
    # Use assertion helpers
    assert_lint_contains(results, "snake_case instead of camelCase")
    assert_lint_count(results, 1)
    assert_lint_count(results, 1, "TaskNamingLinter")
    
    print("✅ Assertion helpers test passed")


def test_create_test_wdl_utility():
    """Demonstrate the create_test_wdl utility function."""
    
    print("\nTesting create_test_wdl utility...")
    
    # Create a basic task
    wdl_code = create_test_wdl()
    print("Basic task WDL:")
    print(wdl_code)
    
    # Create a task with inputs and outputs
    wdl_code = create_test_wdl(
        task_name="process_file",
        command="cat ~{input_file} | wc -l",
        inputs={"input_file": "File"},
        outputs={"line_count": "String"}  # stdout() returns String
    )
    print("\nTask with inputs/outputs:")
    print(wdl_code)
    
    # Test the generated WDL with a linter
    validate_linter(
        TaskNamingLinter,
        wdl_code,
        expected_lint=[]  # Should be valid
    )
    
    print("✅ create_test_wdl utility test passed")


if __name__ == "__main__":
    print("🧪 miniwdl Linter Testing Framework Example")
    print("=" * 50)
    
    test_task_naming_linter()
    test_security_linter()
    test_using_assertion_helpers()
    test_create_test_wdl_utility()
    
    print("\n🎉 All tests passed! The linter testing framework is working correctly.")
    print("\nThis example demonstrates:")
    print("- Creating custom linters with different categories and severities")
    print("- Testing linters with various WDL code scenarios")
    print("- Using assertion helpers for flexible testing")
    print("- Generating test WDL code with the utility functions")
    print("- Comprehensive test coverage for linter behavior")
