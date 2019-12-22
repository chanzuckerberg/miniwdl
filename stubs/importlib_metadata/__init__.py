from typing import Iterable, Dict, Any

class EntryPoint:
    name: str
    value: str

    def load() -> Any:
        ...

def version(pkg: str) -> str:
    ...

def entry_points() -> Dict[str, Iterable[EntryPoint]]:
    ...

class PackageNotFoundError(Exception):
    ...
