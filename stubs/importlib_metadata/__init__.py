# mypy: ignore-errors

from typing import Iterable, Dict, Any

class EntryPoint:
    group: str
    name: str
    value: str

    def __init__(self, *, group: str, name: str, value: str) -> None:
        ...

    def load() -> Any:
        ...

def version(pkg: str) -> str:
    ...

def entry_points(**kwargs) -> Iterable[EntryPoint]:
    ...

class PackageNotFoundError(Exception):
    ...
