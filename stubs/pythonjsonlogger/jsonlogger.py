import logging
from typing import Dict, Any

class JsonFormatter(logging.Formatter):
    def format(self, rec: logging.LogRecord) -> str:
        """
        Format the log message.

        Args:
            self: (todo): write your description
            rec: (todo): write your description
            logging: (todo): write your description
            LogRecord: (todo): write your description
        """
        ...

    def add_fields(self, log_record: Dict[str, Any], record: logging.LogRecord, message_dict: Dict[str, Any]) -> None:
        """
        Add log fields.

        Args:
            self: (todo): write your description
            log_record: (todo): write your description
            record: (todo): write your description
            logging: (todo): write your description
            LogRecord: (todo): write your description
            message_dict: (dict): write your description
        """
        ...
