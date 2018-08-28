# pyre-strict
from abc import ABC, abstractmethod
from typing import Any, List, Optional, TypeVar
import WDL.Type as Ty

class RuntimeTypeError(Exception):
    def __init__(self, message : str) -> None:
        super().__init__(message)

BaseT = TypeVar('BaseT', bound='Base')
class Base(ABC):
    type : Ty.Base
    value : Any # pyre-ignore

    def __init__(self, type : Ty.Base, value : Any) -> None:
        assert isinstance(type, Ty.Base)
        self.type = type
        self.value = value

    def __eq__(self, other) -> bool:
        return (self.type == other.type and self.value == other.value)

    def __str__(self) -> str:
        return str(self.value)

    def coerce(self, desired_type : Optional[Ty.Base] = None) -> BaseT:
        if desired_type is not None and self.type != desired_type:
            raise RuntimeTypeError(str(desired_type)) # TODO: better error message
        return self
    def expect(self, desired_type : Optional[Ty.Base] = None) -> BaseT:
        return self.coerce(desired_type)

class Boolean(Base):
    def __init__(self, value : bool) -> None:
        super(Boolean, self).__init__(Ty.Boolean(), value)
    def __str__(self) -> str:
        return str(self.value).lower()

class Float(Base):
    def __init__(self, value : float) -> None:
        super().__init__(Ty.Float(), value)

class Int(Base):
    def __init__(self, value : int) -> None:
        super().__init__(Ty.Int(), value)
    def coerce(self, desired_type : Optional[Ty.Base] = None) -> Base:
        if desired_type is not None and isinstance(desired_type, Ty.Float):
            return Float(float(self.value)) # pyre-ignore
        return super().coerce(desired_type)

class Array(Base):
    value : List[Any] = []
    def __init__(self, type : Ty.Array, value : List[Any]) -> None:
        super().__init__(type, value)
    def __str__(self) -> str:
        return "[" + ", ".join([str(item) for item in self.value]) + "]"
