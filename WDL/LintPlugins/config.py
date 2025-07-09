"""
Configuration module for the linting system
"""

import os
import logging
from typing import List, Optional

_logger = logging.getLogger("wdl.lint.config")


def get_additional_linters(cfg) -> List[str]:
    """
    Get additional linters from configuration

    Args:
        cfg: Configuration object

    Returns:
        List of linter specifications
    """
    # Priority order:
    # 1. Environment variable
    # 2. Configuration file

    env_linters = os.environ.get("MINIWDL_ADDITIONAL_LINTERS")
    if env_linters:
        return env_linters.split(",")

    try:
        return cfg["linting"].get_list("additional_linters", [])
    except Exception:
        return []


def get_disabled_linters(cfg) -> List[str]:
    """
    Get disabled linters from configuration

    Args:
        cfg: Configuration object

    Returns:
        List of linter names to disable
    """
    env_linters = os.environ.get("MINIWDL_DISABLED_LINTERS")
    if env_linters:
        return env_linters.split(",")

    try:
        return cfg["linting"].get_list("disabled_linters", [])
    except Exception:
        return []


def get_enabled_categories(cfg) -> List[str]:
    """
    Get enabled linter categories from configuration

    Args:
        cfg: Configuration object

    Returns:
        List of category names to enable
    """
    env_categories = os.environ.get("MINIWDL_ENABLED_LINT_CATEGORIES")
    if env_categories:
        return env_categories.split(",")

    try:
        return cfg["linting"].get_list("enabled_categories", [])
    except Exception:
        return []


def get_disabled_categories(cfg) -> List[str]:
    """
    Get disabled linter categories from configuration

    Args:
        cfg: Configuration object

    Returns:
        List of category names to disable
    """
    env_categories = os.environ.get("MINIWDL_DISABLED_LINT_CATEGORIES")
    if env_categories:
        return env_categories.split(",")

    try:
        return cfg["linting"].get_list("disabled_categories", [])
    except Exception:
        return []


def get_exit_on_severity(cfg) -> Optional[str]:
    """
    Get exit on severity level from configuration

    Args:
        cfg: Configuration object

    Returns:
        Severity level name or None
    """
    env_severity = os.environ.get("MINIWDL_EXIT_ON_LINT_SEVERITY")
    if env_severity:
        return env_severity

    try:
        severity = cfg["linting"].get("exit_on_severity")
        # Handle the case where the value includes a comment
        if severity and "#" in severity:
            severity = severity.split("#")[0].strip()
        return severity if severity else None
    except Exception:
        return None
