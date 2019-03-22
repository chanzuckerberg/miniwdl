"""
WDL values instantiated at runtime

Each value is represented by an instance of a Python class inheriting from
``WDL.Value.Base``.

.. inheritance-diagram:: WDL.Value
   :top-classes: WDL.Value.Base
"""
from abc import ABC
from typing import Any, List, Optional, TypeVar, Tuple
import json
import WDL.Type as T

class Base(ABC):
    """The abstract base class for WDL values"""

    type: T.Base
    ":type: WDL.Type.Base"

    value: Any
    """The "raw" Python value"""

    def __init__(self, type: T.Base, value: Any) -> None:
        assert isinstance(type, T.Base)
        self.type = type
        self.value = value

    def __eq__(self, other) -> bool:
        return self.type == other.type and self.value == other.value

    def __str__(self) -> str:
        return str(self.value)

    def coerce(self, desired_type: Optional[T.Base] = None) -> "Base":
        """
        Coerce the value to the desired type and return it

        The result is undefined if the coercion is not valid. Types should be
        checked statically on ``WDL.Expr.Base`` prior to evaluation.

        :raises: ReferenceError for a null value and non-optional type
        """
        if isinstance(desired_type, T.String):
            return String(str(self.value))
        # TODO: coerce T to Array[T] (x to [x])
        return self

    def expect(self, desired_type: Optional[T.Base] = None) -> "Base":
        """Alias for coerce"""
        return self.coerce(desired_type)


class Boolean(Base):
    """``value`` has Python type ``bool``"""

    def __init__(self, value: bool) -> None:
        super().__init__(T.Boolean(), value)

    def __str__(self) -> str:
        return str(self.value).lower()

    def coerce(self, desired_type: Optional[T.Base] = None) -> Base:
        ""
        if isinstance(desired_type, T.String):
            return String(str(self))
        return super().coerce(desired_type)


class Float(Base):
    """``value`` has Python type ``float``"""

    def __init__(self, value: float) -> None:
        super().__init__(T.Float(), value)


class Int(Base):
    """``value`` has Python type ``int``"""

    def __init__(self, value: int) -> None:
        super().__init__(T.Int(), value)

    def coerce(self, desired_type: Optional[T.Base] = None) -> Base:
        ""
        if desired_type is not None and isinstance(desired_type, T.Float):
            return Float(float(self.value))
        return super().coerce(desired_type)


class String(Base):
    """``value`` has Python type ``str``"""

    def __init__(self, value: str) -> None:
        super().__init__(T.String(), value)

    def __str__(self) -> str:
        return json.dumps(self.value)


class Array(Base):
    """``value`` is a Python ``list`` of other ``WDL.Value.Base`` instances"""

    value: List[Base]

    def __init__(self, type: T.Array, value: List[Base]) -> None:
        self.value = []
        super().__init__(type, value)

    def __str__(self) -> str:
        return "[" + ", ".join([str(item) for item in self.value]) + "]"


class Map(Base):
    value: List[Tuple[Base, Base]]

    def __init__(self, type: T.Map, value: List[Tuple[Base, Base]]) -> None:
        self.value = []
        super().__init__(type, value)

    def __str__(self) -> str:
        raise NotImplementedError()  # TODO


class Pair(Base):
    value: Tuple[Base, Base]

    def __init__(self, type: T.Pair, value: Tuple[Base, Base]) -> None:
        super().__init__(type, value)
        self.value = value

    def __str__(self) -> str:
        assert isinstance(self.value, tuple)
        return "(" + str(self.value[0]) + "," + str(self.value[1]) + ")"


class Null(Base):
    """Represents the missing value which optional inputs may take.
    ``type`` and ``value`` are both None."""

    type: Optional[Any]
    value: Optional[Any]

    def __init__(self) -> None:
        # pylint: disable=super-init-not-called
        self.type = None
        self.value = None

    def __str__(self) -> str:
        assert False
        return ""

    def coerce(self, desired_type: Optional[T.Base] = None) -> Base:
        ""
        if desired_type is None or not desired_type.optional:
            raise ReferenceError()
        return self
