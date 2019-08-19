"""
WDL values instantiated at runtime

Each value is represented by an instance of a Python class inheriting from
``WDL.Value.Base``.

.. inheritance-diagram:: WDL.Value
   :top-classes: WDL.Value.Base
"""
from abc import ABC
from typing import Any, List, Optional, Tuple, Dict, Iterable, Union
import json
from . import Error, Type


class Base(ABC):
    """The abstract base class for WDL values"""

    type: Type.Base
    ":type: WDL.Type.Base"

    value: Any
    """The "raw" Python value"""

    expr: "Optional[WDL.Expr.Base]"
    """
    Reference to the WDL expression that generated this value, if it originated
    from ``WDL.Expr.eval``
    """

    def __init__(self, type: Type.Base, value: Any) -> None:
        assert isinstance(type, Type.Base)
        self.type = type
        self.value = value
        self.expr = None

    def __eq__(self, other) -> bool:
        return self.type == other.type and self.value == other.value

    def __str__(self) -> str:
        return json.dumps(self.json)

    def coerce(self, desired_type: Optional[Type.Base] = None) -> "Base":
        """
        Coerce the value to the desired type and return it

        The result is undefined if the coercion is not valid. Types should be
        checked statically on ``WDL.Expr.Base`` prior to evaluation.

        :raises: ReferenceError for a null value and non-optional type
        """
        if isinstance(desired_type, Type.String):
            return String(str(self.value))
        if isinstance(desired_type, Type.Array) and self.type.coerces(
            desired_type.item_type, check_quant=False
        ):
            # coercion of T to Array[T] (x to [x])
            # if self is an Array, then Array.coerce precludes this path
            return Array(desired_type, [self.coerce(desired_type.item_type)])
        return self

    def expect(self, desired_type: Optional[Type.Base] = None) -> "Base":
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
        super().__init__(Type.Boolean(), value)

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if isinstance(desired_type, Type.String):
            return String(str(self))
        return super().coerce(desired_type)


class Float(Base):
    """``value`` has Python type ``float``"""

    def __init__(self, value: float) -> None:
        super().__init__(Type.Float(), value)


class Int(Base):
    """``value`` has Python type ``int``"""

    def __init__(self, value: int) -> None:
        super().__init__(Type.Int(), value)

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if isinstance(desired_type, Type.Float):
            return Float(float(self.value))
        return super().coerce(desired_type)


class String(Base):
    """``value`` has Python type ``str``"""

    def __init__(self, value: str) -> None:
        super().__init__(Type.String(), value)

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if isinstance(desired_type, Type.File) and not isinstance(self, File):
            return File(self.value)
        try:
            if isinstance(desired_type, Type.Int):
                return Int(int(self.value))
            if isinstance(desired_type, Type.Float):
                return Float(float(self.value))
        except ValueError as exn:
            if self.expr:
                raise Error.EvalError(self.expr, "coercing String to number: " + str(exn)) from exn
            raise
        return super().coerce(desired_type)


class File(String):
    """``value`` has Python type ``str``"""

    pass


class Array(Base):
    """``value`` is a Python ``list`` of other ``WDL.Value.Base`` instances"""

    value: List[Base]
    type: Type.Array

    def __init__(self, item_type: Type.Base, value: List[Base]) -> None:
        self.value = []
        self.type = Type.Array(item_type, nonempty=(len(value) > 0))
        super().__init__(self.type, value)

    @property
    def json(self) -> Any:
        ""
        return [item.json for item in self.value]

    @property
    def children(self) -> Iterable[Base]:
        return self.value

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if isinstance(desired_type, Type.Array):
            if desired_type.nonempty and not self.value:
                if self.expr:
                    raise Error.EmptyArray(self.expr)
                else:
                    raise ValueError("Empty array for Array+ input/declaration")
            if desired_type.item_type == self.type.item_type or (
                isinstance(desired_type.item_type, Type.Any)
                or isinstance(self.type.item_type, Type.Any)
            ):
                return self
            return Array(desired_type, [v.coerce(desired_type.item_type) for v in self.value])
        return super().coerce(desired_type)


class Map(Base):
    value: List[Tuple[Base, Base]]
    type: Type.Map

    def __init__(
        self, item_type: Tuple[Type.Base, Type.Base], value: List[Tuple[Base, Base]]
    ) -> None:
        self.value = []
        self.type = Type.Map(item_type)
        super().__init__(self.type, value)

    @property
    def json(self) -> Any:
        ""
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

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if isinstance(desired_type, Type.Map) and desired_type != self.type:
            return Map(
                desired_type.item_type,
                [
                    (k.coerce(desired_type.item_type[0]), v.coerce(desired_type.item_type[1]))
                    for (k, v) in self.value
                ],
            )
        if isinstance(desired_type, Type.StructInstance):
            assert desired_type.members
            ans = {}
            for k, v in self.value:
                k = k.coerce(Type.String()).value
                assert k in desired_type.members
                ans[k] = v
            return Struct(desired_type, ans)
        return super().coerce(desired_type)


class Pair(Base):
    value: Tuple[Base, Base]
    type: Type.Pair

    def __init__(
        self, left_type: Type.Base, right_type: Type.Base, value: Tuple[Base, Base]
    ) -> None:
        self.value = value
        self.type = Type.Pair(left_type, right_type)
        super().__init__(self.type, value)

    def __str__(self) -> str:
        assert isinstance(self.value, tuple)
        return "(" + str(self.value[0]) + "," + str(self.value[1]) + ")"

    @property
    def json(self) -> Any:
        ""
        return [self.value[0].json, self.value[1].json]

    @property
    def children(self) -> Iterable[Base]:
        yield self.value[0]
        yield self.value[1]

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if isinstance(desired_type, Type.Pair) and desired_type != self.type:
            return Pair(
                desired_type.left_type,
                desired_type.right_type,
                (
                    self.value[0].coerce(desired_type.left_type),
                    self.value[1].coerce(desired_type.right_type),
                ),
            )
        return super().coerce(desired_type)


class Null(Base):
    """Represents the missing value which optional inputs may take.
    ``type`` and ``value`` are both None."""

    def __init__(self) -> None:
        super().__init__(Type.Any(optional=True), None)

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if desired_type and not desired_type.optional and not isinstance(desired_type, Type.Any):
            # normally the typechecker should prevent this, but it might have
            # had check_quant=False
            if self.expr:
                raise Error.NullValue(self.expr)
            else:
                raise ValueError("'None' for non-optional input/declaration")
        return self

    @property
    def json(self) -> Any:
        ""
        return None


class Struct(Base):
    value: Dict[str, Base]

    def __init__(
        self, type: Union[Type.Object, Type.StructInstance], value: Dict[str, Base]
    ) -> None:
        super().__init__(type, value)
        self.value = dict(value)
        if isinstance(type, Type.StructInstance):
            assert type.members
            # coerce values to member types
            for k in self.value:
                assert k in type.members
                self.value[k] = self.value[k].coerce(type.members[k])
            # if initializer (map or object literal) omits optional members,
            # fill them in with null
            for k in type.members:
                if k not in self.value:
                    assert type.members[k].optional
                    self.value[k] = Null()

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if isinstance(self.type, Type.Object) and isinstance(desired_type, Type.StructInstance):
            return Struct(desired_type, self.value)
        return self

    def __str__(self) -> str:
        return json.dumps(self.json)

    @property
    def json(self) -> Any:
        ""
        ans = {}
        for k, v in self.value.items():
            ans[k] = v.json
        return ans

    @property
    def children(self) -> Iterable[Base]:
        return self.value.values()


def from_json(type: Type.Base, value: Any) -> Base:
    """
    Instantiate a WDL value of the specified type from a parsed JSON value (str, int, float, list, dict, or null).

    :raise WDL.Error.InputError: if the given value isn't coercible to the specified type
    """
    if isinstance(type, Type.Boolean) and value in [True, False]:
        return Boolean(value)
    if isinstance(type, Type.Int) and isinstance(value, int):
        return Int(value)
    if isinstance(type, Type.Float) and isinstance(value, (float, int)):
        return Float(float(value))
    if isinstance(type, Type.File) and isinstance(value, str):
        return File(value)
    if isinstance(type, Type.String) and isinstance(value, str):
        return String(value)
    if isinstance(type, Type.Array) and isinstance(value, list):
        return Array(type, [from_json(type.item_type, item) for item in value])
    if (
        isinstance(type, Type.Map)
        and type.item_type[0] == Type.String()
        and isinstance(value, dict)
    ):
        items = []
        for k, v in value.items():
            assert isinstance(k, str)
            items.append((from_json(type.item_type[0], k), from_json(type.item_type[1], v)))
        return Map(type.item_type, items)
    if (
        isinstance(type, Type.StructInstance)
        and isinstance(value, dict)
        and type.members
        and set(type.members.keys()) == set(value.keys())
    ):
        items = {}
        for k, v in value.items():
            assert isinstance(k, str)
            items[k] = from_json(type.members[k], v)
        return Struct(Type.Object(type.members), items)
    if type.optional and value is None:
        return Null()
    raise Error.InputError(
        "couldn't construct {} from input {}".format(str(type), json.dumps(value))
    )
