from logging import Logger as OriginalLogger
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL

class Logger(OriginalLogger):
    def notice(self, *args, **kwargs) -> None: ...
    def verbose(self, *args, **kwargs) -> None: ...
