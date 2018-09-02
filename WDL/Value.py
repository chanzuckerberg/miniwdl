# pyre-strict
from abc import ABC, abstractmethod
from typing import Any, List, Optional, TypeVar
import WDL.Type as T
import json

BaseT = TypeVar('BaseT', bound='Base')
class Base(ABC):
    type : T.Base
    value : Any # pyre-ignore

    def __init__(self, type : T.Base, value : Any) -> None:
        assert isinstance(type, T.Base)
        self.type = type
        self.value = value

    def __eq__(self, other) -> bool:
        return (self.type == other.type and self.value == other.value)

    def __str__(self) -> str:
        return str(self.value)

    def coerce(self, desired_type : Optional[T.Base] = None) -> BaseT:
        assert desired_type is None or self.type == desired_type
        return self
    def expect(self, desired_type : Optional[T.Base] = None) -> BaseT:
        return self.coerce(desired_type)

class Boolean(Base):
    def __init__(self, value : bool) -> None:
        super().__init__(T.Boolean(), value)
    def __str__(self) -> str:
        return str(self.value).lower()

class Float(Base):
    def __init__(self, value : float) -> None:
        super().__init__(T.Float(), value)

class Int(Base):
    def __init__(self, value : int) -> None:
        super().__init__(T.Int(), value)
    def coerce(self, desired_type : Optional[T.Base] = None) -> Base:
        if desired_type is not None and isinstance(desired_type, T.Float):
            return Float(float(self.value)) # pyre-ignore
        return super().coerce(desired_type)

class String(Base):
    def __init__(self, value : str) -> None:
        super().__init__(T.String(), value)
    def __str__(self) -> str:
        return json.dumps(self.value)

class Array(Base):
    value : List[Any] = []
    def __init__(self, type : T.AnyArray, value : List[Any]) -> None:
        super().__init__(type, value)
    def __str__(self) -> str:
        return "[" + ", ".join([str(item) for item in self.value]) + "]"
