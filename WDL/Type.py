# pyre-strict
"""
WDL data types

WDL has both atomic types such as ``Int``, ``Boolean``, and ``String``; and 
parametric types like ``Array[String]`` and
``Map[String,Array[Array[Float]]]``. Here, each type is represented by an
instance of a Python class inheriting from ``WDL.Type.Base``. Such types are
associated with expressions, statically prior to evaluation, as well as with
values and identifier bindings after evaluation.

An atomic type like ``Int`` is represented by ``WDL.Type.Int()``. Atomic types
can be checked either with ``isinstance(t,WDL.Type.Int)``, which ignores the
possible optional quantifier (thus satisfied by ``Int`` or ``Int?``), or with
``t == WDL.Type.Int(optional=True)`` to include the quantifier in the
comparison.

A parametric type like ``Array[String]`` is represented by
``WDL.Type.Array(WDL.Type.String())``. Any kind of array satisfies
``isinstance(t,WDL.Type.Array)``, and
``WDL.Type.Array(WDL.Type.String()) == WDL.Type.Array(WDL.Type.String())``, but
for example
``WDL.Type.Array(WDL.Type.String()) != WDL.Type.Array(WDL.Type.Float())``.
"""
from abc import ABC, abstractmethod
from typing import Optional, TypeVar

TVBase = TypeVar("TVBase", bound="Base")
class Base(ABC):
    """The abstract base class for WDL types

    Each specific type inherits from this base, e.g.::

        assert issubclass(WDL.Type.Int, WDL.Type.Base)
        assert isinstance(WDL.Type.Array(WDL.Type.Int()), WDL.Type.Base)
    """

    optional : bool
    """True in declarations with the optional quantifier, ``Type?``"""

    def coerces(self, rhs : TVBase) -> bool:
        """True if ``rhs`` is the same type, or can be coerced to, ``self``. Optional/nonempty quantifiers are disregarded for this purpose."""
        return (self == rhs) or isinstance(rhs, String)

    def __str__(self) -> str:
        return type(self).__name__
    def __eq__(self, rhs) -> bool:
        return isinstance(rhs,Base) and str(self) == str(rhs)

class Boolean(Base):
    def __init__(self, optional : bool = False) -> None:
        self.optional = optional
    def coerces(self, rhs : Base) -> bool:
        if isinstance(rhs, String):
            return True
        return super().coerces(rhs)

class Float(Base):
    def __init__(self, optional : bool = False) -> None:
        self.optional = optional
    def coerces(self, rhs : Base) -> bool:
        if isinstance(rhs, String):
            return True
        return super().coerces(rhs)

class Int(Base):
    def __init__(self, optional : bool = False) -> None:
        self.optional = optional
    def coerces(self, rhs : Base) -> bool:
        if isinstance(rhs, Float) or isinstance(rhs, String):
            return True
        return super().coerces(rhs)

class File(Base):
    def __init__(self, optional : bool = False) -> None:
        self.optional = optional
    def coerces(self, rhs : Base) -> bool:
        if isinstance(rhs, String):
            return True
        return super().coerces(rhs)

class String(Base):
    def __init__(self, optional : bool = False) -> None:
        self.optional = optional
    def coerces(self, rhs : Base) -> bool:
        if isinstance(rhs, File):
            return True
        return super().coerces(rhs)

class Array(Base):
    """
    Array type, parameterized by the type of the constituent items.

    ``item_type`` may be None to represent the type of the literal empty array
    ``[]``, which is considered compatible with any array type (lacking the
    nonempty quantifier). This special case should be considered explicitly
    when comparing array types.
    """
    item_type : Optional[Base]
    nonempty : bool
    """True in declarations with the nonempty quantifier, ``Array[type]+``"""

    def __init__(self, item_type : Optional[Base], optional : bool = False, nonempty : bool = False) -> None:
        self.item_type = item_type
        assert isinstance(nonempty, bool)
        self.optional = optional
        self.nonempty = nonempty
    def __str__(self) -> str:
        ans = "Array[" + (str(self.item_type) if self.item_type is not None else "") + "]"
        return ans
    def coerces(self, rhs : Base) -> bool:
        if self.item_type is None and isinstance(rhs, Array) and not rhs.nonempty:
            return True
        if isinstance(rhs, Array):
            if self.item_type is None:
                return (not rhs.nonempty)
            else:
                return self.item_type.coerces(rhs.item_type)
        return super().coerces(rhs)
