from typing import Dict, Any
import logging

DEFAULT_LEVEL_STYLES: Dict[str,Any] = ...
DEFAULT_FIELD_STYLES: Dict[str,Any] = ...

def install(logger: logging.Logger, level: int, level_styles: Dict[str, Any], field_styles: Dict[str, Any], fmt: str) -> None:
    """
    Install a logger.

    Args:
        logger: (todo): write your description
        logging: (bool): write your description
        Logger: (todo): write your description
        level: (str): write your description
        level_styles: (int): write your description
        field_styles: (str): write your description
        fmt: (array): write your description
    """
    ...

class StandardErrorHandler(logging.StreamHandler):
    ...
