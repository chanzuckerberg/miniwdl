from typing import Any

POSIX: int
UNICODE: int
VERBOSE: int

class Pattern:
    def fullmatch(self, string: str) -> Any:
        ...

    def sub(self, repl: str, string: str) -> str:
        ...

def compile(pattern, flags=0, **kwargs) -> Pattern:
    ...

def fullmatch(pat: str, string: str) -> Any:
    ...
