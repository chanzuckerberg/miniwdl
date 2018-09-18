# pyre-strict
"""
WDL data types

WDL has both atomic types such as ``Int``, ``Boolean``, and ``String``; and 
parametric types like ``Array[String]`` and
``Map[String,Array[Array[Float]]]``. Here, each type is represented by an
instance of a Python class inheriting from ``WDL.Type.Base``. Such types are
associated with expressions, statically prior to evaluation, as well as with
values and identifier bindings after evaluation.

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

class Array(Base):
    """
    Array type, paramaterized by the type of the constituent items.

    ``item_type`` may be None to represent the type of the literal empty array
    ``[]``, which is considered compatible with any array type (lacking the
    nonempty quantifier). This special case should be considered explicitly
    when comparing array types.
    """
    item_type : Optional[Base]
    nonempty : bool
    """True in declarations with the nonempty quantifier, ``Array[type]+``"""

    def __init__(self, item_type : Optional[Base], nonempty : bool = False) -> None:
        self.item_type = item_type
        assert isinstance(nonempty, bool)
        self.nonempty = nonempty
    def __str__(self) -> str:
        ans = "Array[" + (str(self.item_type) if self.item_type is not None else "") + "]"
        if self.nonempty:
            ans = ans + "+"
        return ans
