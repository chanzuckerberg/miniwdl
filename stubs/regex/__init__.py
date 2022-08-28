from typing import Any, List

POSIX: int
UNICODE: int
VERBOSE: int

class Pattern:
    def fullmatch(self, string: str) -> Any:
        ...

    def sub(self, repl: str, string: str) -> str:
        ...

    def split(self, string: str) -> List[str]:
        ...

def compile(pattern, flags=0, **kwargs) -> Pattern:
    ...

def fullmatch(pat: str, string: str) -> Any:
    ...
