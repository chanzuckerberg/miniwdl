# pyre-strict
from abc import ABC, abstractmethod
from typing import TypeVar, Optional

BaseT = TypeVar('BaseT', bound='Base')

class StaticTypeError(Exception):
    def __init__(self, message : str) -> None:
        super().__init__(message)
class StaticTypeMismatchError(StaticTypeError):
    def __init__(self, expected : BaseT, actual : BaseT, message : Optional[str] = None) -> None:
        msg = "expected {} instead of {}".format(str(expected), str(actual)) # pyre-ignore
        if message is not None:
            msg = message + "; " + message
        super().__init__(msg)

class Base(ABC):
    @abstractmethod
    def __str__(self) -> str:
        pass
    def __eq__(self, rhs) -> bool:
        return str(self) == str(rhs)
    def expect(self, ty : Optional[BaseT] = None) -> BaseT:
        if ty is not None and self != ty:
            raise StaticTypeMismatchError(self, ty)
        return self

class Boolean(Base):
    def __str__(self) -> str:
        return "Boolean"

class Float(Base):
    def __str__(self) -> str:
        return "Float"

class Int(Base):
    def __str__(self) -> str:
        return "Int"
    def expect(self, ty : Optional[BaseT] = None) -> BaseT:
        # permit Int->Float value promotion
        if ty is not None and isinstance(ty, Float):
            return self
        return super().expect(ty)

class Array(Base):
    item_type : Base
    def __init__(self, item_type : Base) -> None:
        self.item_type = item_type
    def __str__(self) -> str:
        return "Array[" + str(self.item_type) + "]"
