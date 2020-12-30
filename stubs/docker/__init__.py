from typing import Dict, Any, List, Iterable

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

    def logs(self, stdout: bool = False) -> bytes:
        ...

class Containers:
    def run(self, image_tag: str, **kwargs) -> Container:
        ...

class Node:
    attrs: Dict[str,Any]

class Swarm:
    def init(self, **kwargs) -> str:
        ...

class models:
    class services:
        class Service:
            short_id: str
            name: str

            def tasks(self) -> List[Dict[str, Any]]:
                ...

            def reload(self) -> None:
                ...

            def remove(self) -> None:
                ...

            def logs(self, **kwargs) -> Iterable[bytes]:
                ...

            @property
            def attrs(self) -> Dict[str, Any]:
                ...

class Services:
    def create(self, image: str, **kwargs) -> models.services.Service:
        ...

    def list(**kwargs) -> List[models.services.Service]:
        ...

class Nodes:
    def list(**kwargs) -> List[Node]:
        ...

class Image:
    @property
    def attrs(self) -> Dict[str,Any]:
        ...

class Images:
    def get(self, tag: str, **kwargs) -> Image:
        ...

    def pull(self, tag: str, **kwargs) -> None:
        ...

class types:
    def RestartPolicy(p: str) -> Any:
        ...

    def Resources(**kwargs) -> Any:
        ...

    class LogConfig:
        class types:
            JSON: str

        def __init__(self, type: str):
            ...

    class Mount:
        def __init__(self, *args, **kwargs):
            ...

class errors:
    class ImageNotFound(Exception):
        pass

class DockerClient:
    @property
    def containers(self) -> Containers:
        ...

    def close(self) -> None:
        ...

    def info(self) -> Dict[str, Any]:
        ...

    @property
    def swarm(self) -> Swarm:
        ...

    @property
    def services(self) -> Services:
        ...

    def version(self) -> Dict[str, Any]:
        ...

    @property
    def nodes(self) -> Nodes:
        ...

    @property
    def images(self) -> Images:
        ...

def from_env(version: Optional[str] = None, timeout: Optional[int] = None) -> DockerClient:
    ...

