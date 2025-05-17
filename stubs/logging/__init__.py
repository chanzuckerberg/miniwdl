# mypy: ignore-errors

from logging import Logger as OriginalLogger, Formatter, FileHandler, LogRecord, addLevelName
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL

class Logger(OriginalLogger):
    def notice(self, *args, **kwargs) -> None: ...
    def verbose(self, *args, **kwargs) -> None: ...

def getLogger(*args, **kwargs) -> Logger:
    ...
