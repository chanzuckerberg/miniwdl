# mypy: ignore-errors

from typing import Dict, Any, List, Iterable, Tuple, Optional
class APIClient:
    api_version: str
    def create_service(
        self,
        task_template: Any,
        name: Optional[str] = None,
        labels: Optional[Dict[str, Any]] = None,
        mode: Any = None,
        update_config: Any = None,
        networks: Any = None,
        endpoint_config: Any = None,
        endpoint_spec: Any = None,
        rollback_config: Any = None,
    ) -> Dict[str, Any]:
        ...

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

    def list(self, **kwargs) -> List[models.services.Service]:
        ...
    def get(self, service_id: str, *args, **kwargs) -> models.services.Service: ...

class Nodes:
    def list(self, **kwargs) -> List[Node]:
        ...

class Image:
    id: str
    tags: List[str]
    attrs: Dict[str,Any]

class Images:
    def get(self, tag: str, **kwargs) -> Image:
        ...

    def pull(self, tag: str, **kwargs) -> None:
        ...

    def build(self, **kwargs) -> Tuple[Image, Iterable[Dict[str,str]]]:
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
    class ContainerSpec(dict):
        def __init__(
            self,
            image: str,
            command=None,
            env=None,
            workdir=None,
            user=None,
            groups=None,
            mounts=None,
            labels=None,
            cap_add=None,
        ) -> None: ...
    class TaskTemplate(dict):
        def __init__(
            self,
            container_spec: Any,
            resources=None,
            restart_policy=None,
        ) -> None: ...
    class DeviceRequest(dict):
        def __init__(
            self,
            driver=None,
            count=None,
            capabilities=None,
            options=None,
        ) -> None: ...
    class HostConfig:
        def __init__(
            self,
            version: Any,
            device_requests=None,
        ) -> None: ...

class DockerClient:
    api: APIClient
    @property
    def containers(self) -> Containers:
        ...

    @property
    def images(self) -> Images:
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

def from_env(version: Optional[str] = None, timeout: Optional[int] = None) -> DockerClient:
    ...

