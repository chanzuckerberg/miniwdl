"""
Plugin system for discovering and loading external linters
"""

import os
import sys
import importlib
import importlib.util
import inspect
import logging
from typing import List, Any, Optional

_logger = logging.getLogger("wdl.lint.plugins")


def discover_linters(
    additional_linters: Optional[List[str]] = None,
    disabled_linters: Optional[List[str]] = None,
    enabled_categories: Optional[List[str]] = None,
    disabled_categories: Optional[List[str]] = None,
):
    """
    Discover and load linter classes from various sources.

    Args:
        additional_linters: List of linter specifications to load (module:class or path:class)
        disabled_linters: List of linter names to disable
        enabled_categories: List of linter categories to enable
        disabled_categories: List of linter categories to disable

    Returns:
        List of linter classes
    """
    # Import here to avoid circular imports
    from .. import Lint

    # Start with built-in linters
    all_linters = list(Lint._all_linters)

    # Convert lists to sets for faster lookups
    disabled_linters_set = set(disabled_linters or [])
    enabled_categories_set = set(enabled_categories or [])
    disabled_categories_set = set(disabled_categories or [])

    # Load entry points if available
    entry_point_linters = _discover_entry_point_linters()
    all_linters.extend(entry_point_linters)

    # Load additional linters
    if additional_linters:
        for linter_spec in additional_linters:
            try:
                linter_class = _load_linter_from_spec(linter_spec)
                if linter_class and linter_class not in all_linters:
                    all_linters.append(linter_class)
            except Exception as e:
                _logger.warning(f"Failed to load linter {linter_spec}: {str(e)}")

    # Filter linters based on name and category
    filtered_linters = []
    for linter_class in all_linters:
        # Skip if explicitly disabled
        if linter_class.__name__ in disabled_linters_set:
            _logger.debug(f"Skipping disabled linter: {linter_class.__name__}")
            continue

        # Skip if category is disabled
        if (
            hasattr(linter_class, "category")
            and linter_class.category.name in disabled_categories_set
        ):
            _logger.debug(
                f"Skipping linter {linter_class.__name__} with disabled category: {linter_class.category.name}"
            )
            continue

        # Skip if enabled categories are specified and this category is not in the list
        if (
            enabled_categories_set
            and hasattr(linter_class, "category")
            and linter_class.category.name not in enabled_categories_set
        ):
            _logger.debug(
                f"Skipping linter {linter_class.__name__} with category {linter_class.category.name} not in enabled categories"
            )
            continue

        filtered_linters.append(linter_class)

    return filtered_linters


def _discover_entry_point_linters():
    """
    Discover linters registered via entry points

    Returns:
        List of linter classes
    """

    linters = []

    try:
        import importlib.metadata as metadata
    except ImportError:
        try:
            import importlib_metadata as metadata  # type: ignore
        except ImportError:
            _logger.debug("importlib.metadata not available, skipping entry point discovery")
            return linters

    try:
        for entry_point in metadata.entry_points(group="miniwdl.linters"):
            try:
                linter_class = entry_point.load()
                if _is_valid_linter_class(linter_class):
                    linters.append(linter_class)
                    _logger.debug(
                        f"Loaded linter {linter_class.__name__} from entry point {entry_point.name}"
                    )
                else:
                    _logger.warning(
                        f"Entry point {entry_point.name} does not point to a valid Linter class"
                    )
            except Exception as e:
                _logger.warning(
                    f"Failed to load linter from entry point {entry_point.name}: {str(e)}"
                )
    except Exception as e:
        _logger.warning(f"Error discovering entry points: {str(e)}")

    return linters


def _load_linter_from_spec(spec: str):
    """
    Load a linter class from a specification string

    Args:
        spec: Linter specification in the format "module:class" or "/path/to/file.py:class"

    Returns:
        Linter class or None if loading failed
    """
    if ":" not in spec:
        raise ValueError(
            f"Invalid linter specification: {spec}. Expected format: module:class or /path/to/file.py:class"
        )

    module_path, class_name = spec.rsplit(":", 1)

    # Check if it's a file path
    if os.path.isabs(module_path) and module_path.endswith(".py"):
        return _load_linter_from_file(module_path, class_name)
    else:
        return _load_linter_from_module(module_path, class_name)


def _load_linter_from_file(file_path: str, class_name: str):
    """
    Load a linter class from a Python file

    Args:
        file_path: Path to the Python file
        class_name: Name of the linter class

    Returns:
        Linter class or None if loading failed
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Linter file not found: {file_path}")

    # Generate a unique module name
    module_name = (
        f"wdl_lint_plugin_{os.path.basename(file_path).replace('.', '_')}_{hash(file_path)}"
    )

    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Failed to create module spec from {file_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        if not hasattr(module, class_name):
            raise AttributeError(f"Module {file_path} has no class named {class_name}")

        linter_class = getattr(module, class_name)

        if not _is_valid_linter_class(linter_class):
            raise TypeError(f"Class {class_name} in {file_path} is not a valid Linter class")

        return linter_class
    except Exception as e:
        _logger.warning(f"Failed to load linter {class_name} from {file_path}: {str(e)}")
        return None


def _load_linter_from_module(module_path: str, class_name: str):
    """
    Load a linter class from a Python module

    Args:
        module_path: Import path to the module
        class_name: Name of the linter class

    Returns:
        Linter class or None if loading failed
    """
    try:
        module = importlib.import_module(module_path)

        if not hasattr(module, class_name):
            raise AttributeError(f"Module {module_path} has no class named {class_name}")

        linter_class = getattr(module, class_name)

        if not _is_valid_linter_class(linter_class):
            raise TypeError(
                f"Class {class_name} in module {module_path} is not a valid Linter class"
            )

        return linter_class
    except Exception as e:
        _logger.warning(f"Failed to load linter {class_name} from module {module_path}: {str(e)}")
        return None


def _is_valid_linter_class(cls: Any) -> bool:
    """
    Check if a class is a valid Linter class

    Args:
        cls: Class to check

    Returns:
        True if the class is a valid Linter class
    """
    # Import here to avoid circular imports
    from .. import Lint

    return inspect.isclass(cls) and issubclass(cls, Lint.Linter) and cls is not Lint.Linter
