# pyre-strict
from abc import ABC, abstractmethod
from typing import Any, List, Optional
import WDL.Type as Ty

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

class Boolean(Base):
    def __init__(self, value : bool) -> None:
        super(Boolean, self).__init__(Ty.Boolean(), value)
    def __str__(self) -> str:
        return str(self.value).lower()

class Int(Base):
    def __init__(self, value : int) -> None:
        super().__init__(Ty.Int(), value)

class Float(Base):
    def __init__(self, value : float) -> None:
        super().__init__(Ty.Float(), value)

class Array(Base):
    value : List[Any] = []
    def __init__(self, type : Ty.Array, value : List[Any]) -> None:
        super().__init__(type, value)
    def __str__(self) -> str:
        return "[" + ", ".join([str(item) for item in self.value]) + "]"
