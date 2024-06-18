# mypy: ignore-errors

from typing import Any, Iterator, Tuple, List, Callable

POSIX: int
UNICODE: int
VERBOSE: int

class Pattern:
    def fullmatch(self, string: str) -> Any:
        ...

    def sub(self, repl: str | Callable, string: str) -> str:
        ...

    def split(self, string: str) -> List[str]:
        ...

    def search(self, string: str) -> Any:
        ...

def compile(pattern, flags=0, **kwargs) -> Pattern:
    ...

def fullmatch(pat: str, string: str) -> Any:
    ...

class Match:
    def span(self) -> Tuple[int, int]:
        ...

def finditer(pattern: str, string:str) -> Iterator[Match]:
    ...
