from typing import Dict, Any, List

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

class types:
    def RestartPolicy(p: str) -> Any:
        ...

    def Resources(**kwargs) -> Any:
        ...

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

def from_env() -> DockerClient:
    ...

