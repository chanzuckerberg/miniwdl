# pyre-strict
"""
WDL data types

WDL has both atomic types such as ``Int``, ``Boolean``, and ``String``; and 
parametric types like ``Array[String]`` and
``Map[String,Array[Array[Float]]]``. Here, each type is represented by an
immutable instance of a Python class inheriting from ``WDL.Type.Base``. Such
types are associated with expressions, statically prior to evaluation, as well
as with values and identifier bindings after evaluation.

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

The type classes include a method indicating if a value of the type can be
coerced to some other desired type, according to the following rules:

1. ``Boolean``, ``Int``, ``Float``, and ``File`` coerce to ``String``
2. ``Array[T]`` coerces to ``String`` provided ``T`` does as well.
3. ``T`` coerces to ``T?`` but the reverse is not true in general.
4. ``T`` coerces to ``Array[T]`` (an array of length 1).

"""
from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Tuple
import copy

TVBase = TypeVar("TVBase", bound="Base")
class Base(ABC):
    """The abstract base class for WDL types

    Each specific type inherits from this base, e.g.::

        assert issubclass(WDL.Type.Int, WDL.Type.Base)
        assert isinstance(WDL.Type.Array(WDL.Type.Int()), WDL.Type.Base)

    All instances are immutable.
    """

    _optional : bool # immutable!!!

    def coerces(self, rhs : TVBase) -> bool:
        """
        True if this is the same type as, or can be coerced to, ``rhs``.
        """
        if isinstance(rhs, Array) and rhs.item_type == self: # coerce T to Array[T]
            return True
        return (type(self).__name__ == type(rhs).__name__) and (not self.optional or rhs.optional)

    @property
    def optional(self) -> bool:
        """
        :type: bool

        True when the type has the optional quantifier, ``T?``"""
        return self._optional

    def copy(self, optional : Optional[bool] = None) -> TVBase:
        """
        copy(self, optional : Optional[bool] = None) -> WDL.Type.Base
        
        Create a copy of the type, possibly with a different setting of the ``optional`` quantifier."""
        ans : Base = copy.copy(self)
        if optional is not None:
            ans._optional = optional
        return ans

    def __str__(self) -> str:
        return type(self).__name__ + ('?' if self.optional else '')
    def __eq__(self, rhs) -> bool:
        return isinstance(rhs,Base) and str(self) == str(rhs)

class Boolean(Base):
    def __init__(self, optional : bool = False) -> None:
        self._optional = optional
    def coerces(self, rhs : Base) -> bool:
        ""
        if isinstance(rhs, String):
            return True
        return super().coerces(rhs)

class Float(Base):
    def __init__(self, optional : bool = False) -> None:
        self._optional = optional
    def coerces(self, rhs : Base) -> bool:
        ""
        if isinstance(rhs, String):
            return True
        return super().coerces(rhs)

class Int(Base):
    def __init__(self, optional : bool = False) -> None:
        self._optional = optional
    def coerces(self, rhs : Base) -> bool:
        ""
        if isinstance(rhs, Float) or isinstance(rhs, String):
            return True
        return super().coerces(rhs)

class File(Base):
    def __init__(self, optional : bool = False) -> None:
        self._optional = optional
    def coerces(self, rhs : Base) -> bool:
        ""
        if isinstance(rhs, String):
            return True
        return super().coerces(rhs)

class String(Base):
    def __init__(self, optional : bool = False) -> None:
        self._optional = optional
    def coerces(self, rhs : Base) -> bool:
        ""
        if isinstance(rhs, File):
            return True
        return super().coerces(rhs)

class Array(Base):
    """
    Array type, parameterized by the type of the constituent items.
    """
    item_type : Optional[Base] # TODO: make immutable property
    """
    :type: Optional[WDL.Type.Base]

    ``item_type`` may be None to represent an array whose item type isn't
    known statically, such as a literal empty array ``[]``, or the result
    of the ``read_array()`` standard library function. This is statically
    coercible to any array type (but may fail at runtime).
    """
    _nonempty : bool

    def __init__(self, item_type : Optional[Base], optional : bool = False, nonempty : bool = False) -> None:
        self.item_type = item_type
        assert isinstance(nonempty, bool)
        self._optional = optional
        self._nonempty = nonempty
    def __str__(self) -> str:
        ans = "Array[" + (str(self.item_type) if self.item_type is not None else "") + "]" \
                + ('+' if self.nonempty else '') \
                + ('?' if self.optional else '')
        return ans
    @property
    def nonempty(self) -> bool:
        """
        :type: bool

        True when the type has the nonempty quantifier, ``Array[T]+``
        """
        return self._nonempty
    def coerces(self, rhs : Base) -> bool:
        ""
        if isinstance(rhs, Array):
            if self.item_type is None or rhs.item_type is None:
                return True
            else:
                return self.item_type.coerces(rhs.item_type) and (not rhs.nonempty or self.nonempty)
        if isinstance(rhs, String):
            return self.item_type is None or self.item_type.coerces(String())
        return False
    def copy(self, optional : Optional[bool] = None, nonempty : Optional[bool] = None) -> Base:
        ans : Array = super().copy(optional)
        if nonempty is not None:
            ans._nonempty = nonempty
        return ans

class Map(Base):
    """
    Map type, parameterized by the (key,value) item type.
    """

    item_type : Optional[Tuple[Base,Base]]
    """
    :type: Optional[Tuple[WDL.Type.Base,WDL.Type.Base]]

    ``item_type`` may be None to represent a map whose type isn't known
    statically, such as a literal empty map ``{}``, or the result of the
    ``read_map()`` standard library function. This is statically coercible
    to any map type (but may fail at runtime).
    """

    def __init__(self, item_type : Optional[Tuple[Base,Base]], optional : bool = False) -> None:
        self._optional = optional
        self.item_type = item_type
    def __str__(self) -> str:
        return "Map[" + (str(self.item_type[0]) + "," + str(self.item_type[1]) if self.item_type is not None else "") + "]" + ('?' if self.optional else '') # pyre-fixme
    def coerces(self, rhs : Base) -> bool:
        ""
        if isinstance(rhs, Map):
            if self.item_type is None or rhs.item_type is None:
                return True
            else:
                return self.item_type[0].coerces(rhs.item_type[0]) and self.item_type[1].coerces(rhs.item_type[1]) # pyre-fixme
        return super().coerces(rhs)

class Pair(Base):
    """
    Pair type, parameterized by the left and right item types.
    """
    left_type : Base
    """
    :type: WDL.Type.Base
    """
    right_type : Base
    """
    :type: WDL.Type.Base
    """

    def __init__(self, left_type : Base, right_type : Base, optional : bool = False) -> None:
        self._optional = optional
        self.left_type = left_type
        self.right_type = right_type
    def __str__(self) -> str:
        return "Pair[" + (str(self.left_type) + "," + str(self.right_type)) + "]" + ('?' if self.optional else '')
