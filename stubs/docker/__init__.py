from typing import Dict

class Container:
    @property
    def name(self) -> str:
        ...

    @property
    def id(self) -> str:
        ...

    def wait(self, timeout: int = None) -> Dict:
        ...

    def remove(self, force: bool = False) -> None:
        ...

class Containers:
    def run(self, image_tag: str, **kwargs) -> Container:
        ...

class Client:
    @property
    def containers() -> Containers:
        ...

    def close() -> None:
        ...

def from_env() -> Client:
    ...

