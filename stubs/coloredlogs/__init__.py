from typing import Dict, Any
import logging

DEFAULT_LEVEL_STYLES: Dict[str,Any] = ...

def install(logger: logging.Logger, level: int, level_styles: Dict[str, Any], fmt: str) -> None:
    ...
