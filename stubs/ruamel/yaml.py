from typing import Dict, Any

class YAML:
    def __init__(self, type: str, pure: bool) -> None:
        ...

    def load(self, s: str) -> Dict[str, Any]:
        ...
