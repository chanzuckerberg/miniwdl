import logging
from typing import Dict, Any

class JsonFormatter(logging.Formatter):
    def format(self, rec: logging.LogRecord) -> str:
        ...

    def add_fields(self, log_record: Dict[str, Any], record: logging.LogRecord, message_dict: Dict[str, Any]) -> None:
        ...
