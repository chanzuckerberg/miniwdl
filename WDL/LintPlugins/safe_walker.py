"""
Safe walker for linter execution
"""

import logging
from typing import List, Any, Optional
from .. import Walker

_logger = logging.getLogger("wdl.lint.safe_walker")


class SafeLinterWalker(Walker.Base):
    """
    A wrapper around Walker.Multi that catches exceptions during linter execution
    """

    def __init__(self, linters: List[Walker.Base], descend_imports: bool = True):
        """
        Initialize the safe linter walker

        Args:
            linters: List of linter instances to run
            descend_imports: Whether to descend into imported documents
        """
        super().__init__(auto_descend=True)
        self.linters = linters
        self.descend_imports = descend_imports

    def __call__(self, obj: Any, descend: Optional[bool] = None):
        """
        Call each linter on the object, catching and logging any exceptions

        Args:
            obj: The AST node to lint
            descend: Whether to descend into child nodes
        """
        for linter in self.linters:
            try:
                linter(obj, descend)
            except Exception as e:
                _logger.warning(
                    f"Error in linter {linter.__class__.__name__} on {obj.__class__.__name__}: {str(e)}"
                )

        # Continue traversal
        if descend is None:
            descend = self.auto_descend
        if descend:
            for child in obj.children:
                self(child)
