from typing import Iterable, Dict, Any

class EntryPoint:
    name: str
    value: str

    def load() -> Any:
        ...

def version(pkg: str) -> str:
    ...

def entry_points(**kwargs) -> Iterable[EntryPoint]:
    ...

class PackageNotFoundError(Exception):
    ...
