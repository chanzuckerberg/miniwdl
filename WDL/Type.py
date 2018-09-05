# pyre-strict
"""
WDL data types

WDL has both atomic types such as ``Int``, ``Boolean``, and ``String``; and 
parametric types like ``Array[String]`` and
``Map[String,Array[Array[Float]]]``. Here, each type is represented by an
instance of a Python class inheriting from ``WDL.Type.Base``.

An atomic type like ``Int`` is represented by ``WDL.Type.Int()``. All such
instances are equivalent, so a given type instance ``t`` can be checked with either
``t == WDL.Type.Int()`` or ``isinstance(t,WDL.Type.Int)``. 

A parametric type like ``Array[String]`` is represented by
``WDL.Type.Array(WDL.Type.String())``. Any kind of array satisfies
``isinstance(t,WDL.Type.Array)``, and
``WDL.Type.Array(WDL.Type.String()) == WDL.Type.Array(WDL.Type.String())``, but
for example
``WDL.Type.Array(WDL.Type.String()) != WDL.Type.Array(WDL.Type.Float())``.
"""
from abc import ABC, abstractmethod
from typing import Optional

class Base(ABC):
    """The abstract base class for WDL types

    Each specific type inherits from this base, e.g.::

        assert issubclass(WDL.Type.Int, WDL.Type.Base)
        assert isinstance(WDL.Type.Array(WDL.Type.Int()), WDL.Type.Base)
    """
    @abstractmethod
    def __str__(self) -> str:
        pass
    def __eq__(self, rhs) -> bool:
        return str(self) == str(rhs)

class Boolean(Base):
    def __str__(self) -> str:
        return "Boolean"

class Int(Base):
    def __str__(self) -> str:
        return "Int"

class Float(Base):
    def __str__(self) -> str:
        return "Float"

class String(Base):
    def __str__(self) -> str:
        return "String"

class AnyArray(Base):
    """
    The superclass of all array types, however parameterized.

    ``AnyArray`` may be instantiated to represent the type of a literal empty
    array ``[]``, which is compatible with any array type. Therefore, checking
    ``isinstance(t,WDl.Type.AnyArray)`` should be used instead of
    ``isinstance(t,WDl.Type.Array)``.
    """
    def __str__(self) -> str:
        return "Array[]"

class Array(AnyArray):
    item_type : Base
    def __init__(self, item_type : Base) -> None:
        self.item_type = item_type
    def __str__(self) -> str:
        return "Array[" + str(self.item_type) + "]"
