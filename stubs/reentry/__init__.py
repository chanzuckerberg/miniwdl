from typing import Iterable, Any

class EntryPoint:
    name: str
    def load() -> Any:
        ...

class EntryPointManager:
    def iter_entry_points(group: str) -> Iterable[EntryPoint]:
        ...

manager: EntryPointManager = ...
