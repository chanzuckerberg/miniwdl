# pyre-strict
from abc import ABC, abstractmethod
from typing import Optional

class Base(ABC):
    @abstractmethod
    def __str__(self) -> str:
        pass
    def __eq__(self, rhs) -> bool:
        return str(self) == str(rhs)

class Boolean(Base):
    def __str__(self) -> str:
        return "Boolean"

class Float(Base):
    def __str__(self) -> str:
        return "Float"

class Int(Base):
    def __str__(self) -> str:
        return "Int"

class Array(Base):
    item_type : Base
    def __init__(self, item_type : Base) -> None:
        self.item_type = item_type
    def __str__(self) -> str:
        return "Array[" + str(self.item_type) + "]"
