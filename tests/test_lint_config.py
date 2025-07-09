import unittest
import os
import tempfile
from WDL.LintPlugins.config import (
    get_additional_linters,
    get_disabled_linters,
    get_enabled_categories,
    get_disabled_categories,
    get_exit_on_severity,
)
from WDL.runtime.config import Loader
import logging


class TestLintConfig(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("test_lint_config")
        # Create a temporary config file
        self.temp_cfg = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        self.temp_cfg.write("""
[linting]
additional_linters = ["module1:Linter1", "module2:Linter2"]
disabled_linters = ["StringCoercion", "FileCoercion"]
enabled_categories = ["STYLE", "SECURITY"]
disabled_categories = ["PERFORMANCE"]
exit_on_severity = "MAJOR"
""")
        self.temp_cfg.close()
        self.cfg = Loader(self.logger, [self.temp_cfg.name])
        
        # Save original environment variables
        self.original_env = {}
        for var in ["MINIWDL_ADDITIONAL_LINTERS", "MINIWDL_DISABLED_LINTERS", 
                    "MINIWDL_ENABLED_LINT_CATEGORIES", "MINIWDL_DISABLED_LINT_CATEGORIES",
                    "MINIWDL_EXIT_ON_LINT_SEVERITY"]:
            self.original_env[var] = os.environ.get(var)
            if var in os.environ:
                del os.environ[var]
    
    def tearDown(self):
        # Remove temporary config file
        os.unlink(self.temp_cfg.name)
        
        # Restore original environment variables
        for var, value in self.original_env.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]
    
    def test_get_from_config(self):
        """Test getting configuration from config file"""
        self.assertEqual(get_additional_linters(self.cfg), ["module1:Linter1", "module2:Linter2"])
        self.assertEqual(get_disabled_linters(self.cfg), ["StringCoercion", "FileCoercion"])
        self.assertEqual(get_enabled_categories(self.cfg), ["STYLE", "SECURITY"])
        self.assertEqual(get_disabled_categories(self.cfg), ["PERFORMANCE"])
        self.assertEqual(get_exit_on_severity(self.cfg), "MAJOR")
    
    def test_get_from_env(self):
        """Test getting configuration from environment variables"""
        os.environ["MINIWDL_ADDITIONAL_LINTERS"] = "env1:Linter1,env2:Linter2"
        os.environ["MINIWDL_DISABLED_LINTERS"] = "EnvLinter1,EnvLinter2"
        os.environ["MINIWDL_ENABLED_LINT_CATEGORIES"] = "STYLE,CORRECTNESS"
        os.environ["MINIWDL_DISABLED_LINT_CATEGORIES"] = "SECURITY,PORTABILITY"
        os.environ["MINIWDL_EXIT_ON_LINT_SEVERITY"] = "CRITICAL"
        
        self.assertEqual(get_additional_linters(self.cfg), ["env1:Linter1", "env2:Linter2"])
        self.assertEqual(get_disabled_linters(self.cfg), ["EnvLinter1", "EnvLinter2"])
        self.assertEqual(get_enabled_categories(self.cfg), ["STYLE", "CORRECTNESS"])
        self.assertEqual(get_disabled_categories(self.cfg), ["SECURITY", "PORTABILITY"])
        self.assertEqual(get_exit_on_severity(self.cfg), "CRITICAL")
    
    def test_empty_config(self):
        """Test with empty configuration"""
        # Create a mock config object that will raise an exception when accessing linting section
        class MockConfig:
            def __getitem__(self, section):
                if section == "linting":
                    raise KeyError("No such section")
                return {}
        
        empty_cfg = MockConfig()
        
        self.assertEqual(get_additional_linters(empty_cfg), [])
        self.assertEqual(get_disabled_linters(empty_cfg), [])
        self.assertEqual(get_enabled_categories(empty_cfg), [])
        self.assertEqual(get_disabled_categories(empty_cfg), [])
        self.assertEqual(get_exit_on_severity(empty_cfg), None)


if __name__ == "__main__":
    unittest.main()
