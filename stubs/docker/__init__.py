from typing import Dict, Any, List, Iterable

class Container:
    @property
    def name(self) -> str:
        """
        Returns the name of the node.

        Args:
            self: (todo): write your description
        """
        ...

    @property
    def id(self) -> str:
        """
        Returns the id of the message.

        Args:
            self: (todo): write your description
        """
        ...

    def wait(self, timeout: int = None) -> Dict:
        """
        Waits until timeout isochrone.

        Args:
            self: (todo): write your description
            timeout: (float): write your description
        """
        ...

    def remove(self, force: bool = False) -> None:
        """
        Removes the given value from the list.

        Args:
            self: (todo): write your description
            force: (bool): write your description
        """
        ...

    def logs(self, stdout: bool = False) -> bytes:
        """
        Logs the current logs.

        Args:
            self: (todo): write your description
            stdout: (todo): write your description
        """
        ...

class Containers:
    def run(self, image_tag: str, **kwargs) -> Container:
        """
        Run an image

        Args:
            self: (todo): write your description
            image_tag: (str): write your description
        """
        ...

class Node:
    attrs: Dict[str,Any]

class Swarm:
    def init(self, **kwargs) -> str:
        """
        Initialize the class.

        Args:
            self: (todo): write your description
        """
        ...

class models:
    class services:
        class Service:
            short_id: str
            name: str

            def tasks(self) -> List[Dict[str, Any]]:
                """
                Return a list of tasks.

                Args:
                    self: (todo): write your description
                """
                ...

            def reload(self) -> None:
                """
                Reloads the configuration.

                Args:
                    self: (todo): write your description
                """
                ...

            def remove(self) -> None:
                """
                Removes the first element.

                Args:
                    self: (todo): write your description
                """
                ...

            def logs(self, **kwargs) -> Iterable[bytes]:
                """
                Recursively iterable logs.

                Args:
                    self: (todo): write your description
                """
                ...

            @property
            def attrs(self) -> Dict[str, Any]:
                """
                Returns a dictionary of the attributes.

                Args:
                    self: (todo): write your description
                """
                ...

class Services:
    def create(self, image: str, **kwargs) -> models.services.Service:
        """
        Create a new image.

        Args:
            self: (int): write your description
            image: (array): write your description
        """
        ...

    def list(**kwargs) -> List[models.services.Service]:
        """
        List services.

        Args:
        """
        ...

class Nodes:
    def list(**kwargs) -> List[Node]:
        """
        List all the list.

        Args:
        """
        ...

class types:
    def RestartPolicy(p: str) -> Any:
        """
        Convert a third - value.

        Args:
            p: (todo): write your description
        """
        ...

    def Resources(**kwargs) -> Any:
        """
        Create a new : class that : meth : meth : ~simple.

        Args:
        """
        ...

    class LogConfig:
        class types:
            JSON: str

        def __init__(self, type: str):
            """
            Initialize the given type.

            Args:
                self: (todo): write your description
                type: (str): write your description
            """
            ...

    class Mount:
        def __init__(self, *args, **kwargs):
            """
            Initialize this class.

            Args:
                self: (todo): write your description
            """
            ...

class DockerClient:
    @property
    def containers(self) -> Containers:
        """
        Containers.

        Args:
            self: (todo): write your description
        """
        ...

    def close(self) -> None:
        """
        Closes the connection.

        Args:
            self: (todo): write your description
        """
        ...

    def info(self) -> Dict[str, Any]:
        """
        Get information about the info object.

        Args:
            self: (todo): write your description
        """
        ...

    @property
    def swarm(self) -> Swarm:
        """
        Swarm the configuration.

        Args:
            self: (todo): write your description
        """
        ...

    @property
    def services(self) -> Services:
        """
        The service.

        Args:
            self: (todo): write your description
        """
        ...

    def version(self) -> Dict[str, Any]:
        """
        Returns the version of the server.

        Args:
            self: (todo): write your description
        """
        ...

    @property
    def nodes(self) -> Nodes:
        """
        Returns a list of nodes in this graph.

        Args:
            self: (todo): write your description
        """
        ...

def from_env(version: Optional[str] = None, timeout: Optional[int] = None) -> DockerClient:
    """
    Create a new environment from the environment.

    Args:
        version: (str): write your description
        timeout: (int): write your description
    """
    ...

