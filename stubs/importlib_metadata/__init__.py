from typing import Iterable, Dict, Any

class EntryPoint:
    name: str
    value: str

    def load() -> Any:
        """
        Loads the given load balancer.

        Args:
        """
        ...

def version(pkg: str) -> str:
    """
    Return the version string.

    Args:
        pkg: (todo): write your description
    """
    ...

def entry_points() -> Dict[str, Iterable[EntryPoint]]:
    """
    The entry point entry point.

    Args:
    """
    ...

class PackageNotFoundError(Exception):
    ...
