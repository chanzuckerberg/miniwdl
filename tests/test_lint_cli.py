import unittest
import os
import tempfile
from WDL.Lint.config import (
    get_additional_linters,
    get_disabled_linters,
    get_enabled_categories,
    get_disabled_categories,
    get_exit_on_severity,
)
from WDL.runtime.config import Loader
import logging


class TestLintCLIConfig(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("test_lint_cli_config")
        
        # Save original environment variables
        self.original_env = {}
        for var in ["MINIWDL_ADDITIONAL_LINTERS", "MINIWDL_DISABLED_LINTERS", 
                    "MINIWDL_ENABLED_LINT_CATEGORIES", "MINIWDL_DISABLED_LINT_CATEGORIES",
                    "MINIWDL_EXIT_ON_LINT_SEVERITY"]:
            self.original_env[var] = os.environ.get(var)
            if var in os.environ:
                del os.environ[var]
    
    def tearDown(self):
        # Restore original environment variables
        for var, value in self.original_env.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]
    
    def test_env_variables(self):
        """Test environment variables for linting configuration"""
        # Set environment variables
        os.environ["MINIWDL_ADDITIONAL_LINTERS"] = "module1:Linter1,module2:Linter2"
        os.environ["MINIWDL_DISABLED_LINTERS"] = "StringCoercion,FileCoercion"
        os.environ["MINIWDL_ENABLED_LINT_CATEGORIES"] = "STYLE,SECURITY"
        os.environ["MINIWDL_DISABLED_LINT_CATEGORIES"] = "PERFORMANCE"
        os.environ["MINIWDL_EXIT_ON_LINT_SEVERITY"] = "MAJOR"
        
        # Create a config with empty linting section
        empty_cfg_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        empty_cfg_file.write("[linting]\n")
        empty_cfg_file.close()
        
        cfg = Loader(self.logger, [empty_cfg_file.name])
        
        # Environment variables should take precedence
        self.assertEqual(get_additional_linters(cfg), ["module1:Linter1", "module2:Linter2"])
        self.assertEqual(get_disabled_linters(cfg), ["StringCoercion", "FileCoercion"])
        self.assertEqual(get_enabled_categories(cfg), ["STYLE", "SECURITY"])
        self.assertEqual(get_disabled_categories(cfg), ["PERFORMANCE"])
        self.assertEqual(get_exit_on_severity(cfg), "MAJOR")
        
        os.unlink(empty_cfg_file.name)
    
    def test_config_precedence(self):
        """Test that environment variables take precedence over config file"""
        # Create a config file with linting settings
        cfg_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        cfg_file.write("""
[linting]
additional_linters = ["config1:Linter1", "config2:Linter2"]
disabled_linters = ["ConfigLinter1", "ConfigLinter2"]
enabled_categories = ["CONFIG_CATEGORY1", "CONFIG_CATEGORY2"]
disabled_categories = ["CONFIG_CATEGORY3"]
exit_on_severity = "MODERATE"
""")
        cfg_file.close()
        
        # Set environment variables
        os.environ["MINIWDL_ADDITIONAL_LINTERS"] = "env1:Linter1,env2:Linter2"
        
        cfg = Loader(self.logger, [cfg_file.name])
        
        # Environment variable should override config file for additional_linters
        self.assertEqual(get_additional_linters(cfg), ["env1:Linter1", "env2:Linter2"])
        
        # But config file should be used for other settings
        self.assertEqual(get_disabled_linters(cfg), ["ConfigLinter1", "ConfigLinter2"])
        self.assertEqual(get_enabled_categories(cfg), ["CONFIG_CATEGORY1", "CONFIG_CATEGORY2"])
        self.assertEqual(get_disabled_categories(cfg), ["CONFIG_CATEGORY3"])
        self.assertEqual(get_exit_on_severity(cfg), "MODERATE")
        
        os.unlink(cfg_file.name)


if __name__ == "__main__":
    unittest.main()
