from abc import ABC, abstractmethod
from typing import Any, List, Optional
import WDL.Type as Ty

class Base(ABC):
    type : Ty.Base
    value : Any

    def __init__(self, type : Ty.Base, value : Any):
        assert isinstance(type, Ty.Base)
        self.type = type
        self.value = value

    def __eq__(self, other):
        return self.type == other.type and self.value == other.value

    def __str__(self):
        return str(self.value)

class Boolean(Base):
    def __init__(self, value : bool):
        assert value == True or value == False
        super(Boolean, self).__init__(Ty.Boolean(), value)
    def __str__(self):
        return str(self.value).lower()

class Int(Base):
    def __init__(self, value : int):
        super().__init__(Ty.Int(), value)

class Float(Base):
    def __init__(self, value : float):
        super().__init__(Ty.Float(), value)

class Array(Base):
    def __init__(self, type : Ty.Array, value : List[Any]):
        assert isinstance(type, Ty.Array)
        super().__init__(type, value)
    def __str__(self):
        return "[" + ", ".join([str(item) for item in self.value]) + "]"
