"""
WDL values instantiated at runtime

Each value is represented by an instance of a Python class inheriting from
``WDL.Value.Base``.

.. inheritance-diagram:: WDL.Value
   :top-classes: WDL.Value.Base
"""
from abc import ABC, abstractstaticmethod
from typing import Any, List, Optional, Tuple, Dict
import json
import WDL.Type as T
import WDL.Error as Error


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
        return json.dumps(self.json)

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

    @property
    def json(self) -> Any:
        """Return a value representation which can be serialized to JSON using ``json.dumps`` (str, int, float, list, dict, or null)"""
        return self.value


class Boolean(Base):
    """``value`` has Python type ``bool``"""

    def __init__(self, value: bool) -> None:
        super().__init__(T.Boolean(), value)

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


class Array(Base):
    """``value`` is a Python ``list`` of other ``WDL.Value.Base`` instances"""

    value: List[Base]

    def __init__(self, type: T.Array, value: List[Base]) -> None:
        self.value = []
        super().__init__(type, value)

    @property
    def json(self) -> Any:
        return [item.json for item in self.value]


class Map(Base):
    value: List[Tuple[Base, Base]]

    def __init__(self, type: T.Map, value: List[Tuple[Base, Base]]) -> None:
        self.value = []
        super().__init__(type, value)

    @property
    def json(self) -> Any:
        ans = {}
        for k, v in self.value:
            assert isinstance(k, String)  # TODO
            ans[k.value] = v.json
        return ans


class Pair(Base):
    value: Tuple[Base, Base]

    def __init__(self, type: T.Pair, value: Tuple[Base, Base]) -> None:
        super().__init__(type, value)
        self.value = value

    def __str__(self) -> str:
        assert isinstance(self.value, tuple)
        return "(" + str(self.value[0]) + "," + str(self.value[1]) + ")"

    @property
    def json(self) -> Any:
        raise NotImplementedError()


class Null(Base):
    """Represents the missing value which optional inputs may take.
    ``type`` and ``value`` are both None."""

    type: Optional[Any]
    value: Optional[Any]

    def __init__(self) -> None:
        # pylint: disable=super-init-not-called
        self.type = None
        self.value = None

    def coerce(self, desired_type: Optional[T.Base] = None) -> Base:
        ""
        if desired_type is None or not desired_type.optional:
            raise ReferenceError()
        return self

    @property
    def json(self) -> Any:
        return None


class Struct(Base):
    value: Dict[str, Base]

    def __init__(self, type: T.Object, value: Dict[str, Base]) -> None:
        super().__init__(type, value)
        self.value = value

    def __str__(self) -> str:
        return json.dumps(self.json)

    @property
    def json(self) -> Any:
        ans = {}
        for k, v in self.value.items():
            ans[k] = v.json
        return ans


def from_json(type: T.Base, value: Any) -> Base:
    """
    Instantiate a WDL value of the specified type from a parsed JSON value (str, int, float, list, dict, or null).

    :raise WDL.Error.InputError: if the given value isn't coercible to the specified type
    """
    if isinstance(type, T.Boolean) and value in [True, False]:
        return Boolean(value)
    if isinstance(type, T.Int) and isinstance(value, int):
        return Int(value)
    if isinstance(type, T.Float) and isinstance(value, (float, int)):
        return Float(float(value))
    if isinstance(type, T.String) and isinstance(value, str):
        return String(value)
    if isinstance(type, T.Array) and isinstance(value, list):
        return Array(type, [from_json(type.item_type, item) for item in value])
    if isinstance(type, T.Map) and type.item_type[0] == T.String() and isinstance(value, dict):
        items = []
        for k, v in value.items():
            assert isinstance(k, str)
            items.append((from_json(type.item_type[0], k), from_json(type.item_type[1], v)))
        return Map(type, items)
    if (
        isinstance(type, T.StructInstance)
        and isinstance(value, dict)
        and type.members
        and set(type.members.keys()) == set(value.keys())
    ):
        items = {}
        for k, v in value.items():
            assert isinstance(k, str)
            items[k] = from_json(type.members[k], v)
        return Struct(T.Object(type.members), items)
    if type.optional and value is None:
        return Null()
    raise Error.InputError(
        "couldn't construct {} from input {}".format(str(type), json.dumps(value))
    )
