from typing import Iterable, Dict

class BuildError(Exception):
    msg : str
    build_log : Iterable[Dict[str,str]]

class ImageNotFound(Exception):
    pass

class APIError(Exception):
    def is_server_error(self) -> bool:
        ...