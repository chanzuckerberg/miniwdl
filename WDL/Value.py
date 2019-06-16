"""
WDL values instantiated at runtime

Each value is represented by an instance of a Python class inheriting from
``WDL.Value.Base``.

.. inheritance-diagram:: WDL.Value
   :top-classes: WDL.Value.Base
"""
from abc import ABC
from typing import Any, List, Optional, Tuple, Dict, Iterable
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
        if isinstance(desired_type, T.Array) and self.type.coerces(
            desired_type.item_type, check_quant=False
        ):
            # coercion of T to Array[T] (x to [x])
            # if self is an Array, then Array.coerce precludes this path
            return Array(desired_type, [self.coerce(desired_type.item_type)])
        return self

    def expect(self, desired_type: Optional[T.Base] = None) -> "Base":
        """Alias for coerce"""
        return self.coerce(desired_type)

    @property
    def json(self) -> Any:
        """Return a value representation which can be serialized to JSON using ``json.dumps`` (str, int, float, list, dict, or null)"""
        return self.value

    @property
    def children(self) -> "Iterable[Base]":
        return []


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
        if isinstance(desired_type, T.Float):
            return Float(float(self.value))
        return super().coerce(desired_type)


class String(Base):
    """``value`` has Python type ``str``"""

    def __init__(self, value: str) -> None:
        super().__init__(T.String(), value)

    def coerce(self, desired_type: Optional[T.Base] = None) -> Base:
        ""
        if isinstance(desired_type, T.File):
            return File(self.value)
        return super().coerce(desired_type)


class File(String):
    """``value`` has Python type ``str``"""

    pass


class Array(Base):
    """``value`` is a Python ``list`` of other ``WDL.Value.Base`` instances"""

    value: List[Base]
    type: T.Array

    def __init__(self, type: T.Array, value: List[Base]) -> None:
        self.value = []
        self.type = type
        super().__init__(type, value)

    @property
    def json(self) -> Any:
        return [item.json for item in self.value]

    @property
    def children(self) -> Iterable[Base]:
        return self.value

    def coerce(self, desired_type: Optional[T.Base] = None) -> Base:
        ""
        if isinstance(desired_type, T.Array):
            if desired_type.nonempty and not self.value:
                raise ValueError("Empty array for Array+ input/declaration")
            if desired_type.item_type == self.type.item_type or (
                isinstance(desired_type.item_type, T.Any) or isinstance(self.type.item_type, T.Any)
            ):
                return self
            return Array(desired_type, [v.coerce(desired_type.item_type) for v in self.value])
        return super().coerce(desired_type)


class Map(Base):
    value: List[Tuple[Base, Base]]
    type: T.Map

    def __init__(self, type: T.Map, value: List[Tuple[Base, Base]]) -> None:
        self.value = []
        self.type = type
        super().__init__(type, value)

    @property
    def json(self) -> Any:
        ans = {}
        for k, v in self.value:
            assert isinstance(k, String)  # TODO
            ans[k.value] = v.json
        return ans

    @property
    def children(self) -> Iterable[Base]:
        for (k, v) in self.value:
            yield k
            yield v

    def coerce(self, desired_type: Optional[T.Base] = None) -> Base:
        ""
        if isinstance(desired_type, T.Map) and desired_type != self.type:
            return Map(
                desired_type,
                [
                    (k.coerce(desired_type.item_type[0]), v.coerce(desired_type.item_type[1]))
                    for (k, v) in self.value
                ],
            )
        return super().coerce(desired_type)


class Pair(Base):
    value: Tuple[Base, Base]
    type: T.Pair

    def __init__(self, type: T.Pair, value: Tuple[Base, Base]) -> None:
        self.value = value
        self.type = type
        super().__init__(type, value)

    def __str__(self) -> str:
        assert isinstance(self.value, tuple)
        return "(" + str(self.value[0]) + "," + str(self.value[1]) + ")"

    @property
    def json(self) -> Any:
        raise NotImplementedError()

    @property
    def children(self) -> Iterable[Base]:
        yield self.value[0]
        yield self.value[1]

    def coerce(self, desired_type: Optional[T.Base] = None) -> Base:
        ""
        if isinstance(desired_type, T.Pair) and desired_type != self.type:
            return Pair(
                desired_type,
                (
                    self.value[0].coerce(desired_type.left_type),
                    self.value[1].coerce(desired_type.right_type),
                ),
            )
        return super().coerce(desired_type)


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
        if desired_type and not desired_type.optional:
            # normally the typechecker should prevent this, but it might have
            # had check_quant=False
            raise ValueError("'None' for non-optional input/declaration")
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

    @property
    def children(self) -> Iterable[Base]:
        return self.value.values()


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
    if isinstance(type, T.File) and isinstance(value, str):
        return File(value)
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
