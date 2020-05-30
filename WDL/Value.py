"""
WDL values instantiated at runtime

Each value is represented by an instance of a Python class inheriting from
``WDL.Value.Base``.

.. inheritance-diagram:: WDL.Value
   :top-classes: WDL.Value.Base
"""
import json
import copy
from abc import ABC
from typing import Any, List, Optional, Tuple, Dict, Iterable, Union, Callable
from . import Error, Type, Env


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

    def __init__(self, type: Type.Base, value: Any, expr: "Optional[Expr.Base]" = None) -> None:
        assert isinstance(type, Type.Base)
        self.type = type
        self.value = value
        self.expr = expr

    def __eq__(self, other) -> bool:
        return self.type == other.type and self.value == other.value

    def __str__(self) -> str:
        return json.dumps(self.json)

    def __deepcopy__(self, memo: Dict[int, Any]) -> Any:
        cls = self.__class__
        cp = cls.__new__(cls)
        shallow = ("expr", "type")  # avoid deep-copying large, immutable structures
        for k, v in self.__dict__.items():
            if k != "value":
                setattr(cp, k, copy.deepcopy(v, memo) if k not in shallow else v)
        # override deepcopy of self.value to eliminate sharing; this accommodates rewrite_files()
        # which wants a deep copy for the purpose of modifying the copied File.value, and isn't
        # expecting to encounter shared ones.
        if isinstance(self.value, list):
            value2 = []
            for elt in self.value:
                if isinstance(elt, tuple):
                    assert len(elt) == 2
                    value2.append((copy.deepcopy(elt[0]), copy.deepcopy(elt[1])))
                else:
                    assert isinstance(elt, Base)
                    value2.append(copy.deepcopy(elt))
            cp.value = value2
        elif isinstance(self.value, tuple):
            assert len(self.value) == 2
            cp.value = (copy.deepcopy(self.value[0]), copy.deepcopy(self.value[1]))
        elif isinstance(self.value, dict):
            value2 = {}
            for key in self.value:
                value2[copy.deepcopy(key)] = copy.deepcopy(self.value[key])
            cp.value = value2
        else:
            assert self.value is None or isinstance(self.value, (int, float, bool, str))
            cp.value = self.value
        return cp

    def coerce(self, desired_type: Optional[Type.Base] = None) -> "Base":
        """
        Coerce the value to the desired type and return it. Types should be
        checked statically on ``WDL.Expr.Base`` prior to evaluation.

        :raises: ReferenceError for a null value and non-optional type
        """
        if isinstance(desired_type, Type.String):
            return String(str(self.value), self.expr)
        if isinstance(desired_type, Type.Array) and self.type.coerces(
            desired_type.item_type, check_quant=False
        ):
            # coercion of T to Array[T] (x to [x])
            # if self is an Array, then Array.coerce precludes this path
            return Array(desired_type, [self.coerce(desired_type.item_type)], self.expr)
        if desired_type and not self.type.coerces(desired_type):
            # owing to static type-checking, this path should arise only rarely e.g. read_json()
            raise Error.InputError(f"cannot coerce {str(self.type)} to {str(desired_type)}")
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

    def __init__(self, value: bool, expr: "Optional[Expr.Base]" = None) -> None:
        super().__init__(Type.Boolean(), value, expr)

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if isinstance(desired_type, Type.String):
            return String(str(self), self.expr)
        return super().coerce(desired_type)


class Float(Base):
    """``value`` has Python type ``float``"""

    def __init__(self, value: float, expr: "Optional[Expr.Base]" = None) -> None:
        super().__init__(Type.Float(), value, expr)


class Int(Base):
    """``value`` has Python type ``int``"""

    def __init__(self, value: int, expr: "Optional[Expr.Base]" = None) -> None:
        super().__init__(Type.Int(), value, expr)

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if isinstance(desired_type, Type.Float):
            return Float(float(self.value), self.expr)
        return super().coerce(desired_type)


class String(Base):
    """``value`` has Python type ``str``"""

    def __init__(self, value: str, expr: "Optional[Expr.Base]" = None) -> None:
        super().__init__(Type.String(), value, expr)

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if isinstance(desired_type, Type.File) and not isinstance(self, File):
            return File(self.value, self.expr)
        try:
            if isinstance(desired_type, Type.Int):
                return Int(int(self.value), self.expr)
            if isinstance(desired_type, Type.Float):
                return Float(float(self.value), self.expr)
        except ValueError as exn:
            if self.expr:
                raise Error.EvalError(self.expr, "coercing String to number: " + str(exn)) from exn
            raise
        return super().coerce(desired_type)


class File(String):
    """``value`` has Python type ``str``"""

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if self.value is None:
            # special case for dealing with File? task outputs; see _eval_task_outputs in
            # runtime/task.py. Only on that path should self.value possibly be None.
            if isinstance(desired_type, Type.File) and desired_type.optional:
                return Null(self.expr)
            else:
                raise FileNotFoundError()
        return super().coerce(desired_type)


class Array(Base):
    """``value`` is a Python ``list`` of other ``WDL.Value.Base`` instances"""

    value: List[Base]
    type: Type.Array

    def __init__(
        self, item_type: Type.Base, value: List[Base], expr: "Optional[Expr.Base]" = None
    ) -> None:
        self.value = []
        self.type = Type.Array(item_type, nonempty=(len(value) > 0))
        super().__init__(self.type, value, expr)

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
            return Array(
                desired_type, [v.coerce(desired_type.item_type) for v in self.value], self.expr
            )
        return super().coerce(desired_type)


class Map(Base):
    value: List[Tuple[Base, Base]]
    type: Type.Map

    def __init__(
        self,
        item_type: Tuple[Type.Base, Type.Base],
        value: List[Tuple[Base, Base]],
        expr: "Optional[Expr.Base]" = None,
    ) -> None:
        self.value = []
        self.type = Type.Map(item_type)
        super().__init__(self.type, value, expr)

    def __str__(self) -> str:
        return json.dumps(self.json)

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
                self.expr,
            )
        if isinstance(desired_type, Type.StructInstance):
            assert desired_type.members
            ans = {}
            for k, v in self.value:
                k = k.coerce(Type.String()).value
                assert k in desired_type.members
                ans[k] = v
            return Struct(desired_type, ans, self.expr)
        return super().coerce(desired_type)


class Pair(Base):
    value: Tuple[Base, Base]
    type: Type.Pair

    def __init__(
        self,
        left_type: Type.Base,
        right_type: Type.Base,
        value: Tuple[Base, Base],
        expr: "Optional[Expr.Base]" = None,
    ) -> None:
        self.value = value
        self.type = Type.Pair(left_type, right_type)
        super().__init__(self.type, value, expr)

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
                self.expr,
            )
        return super().coerce(desired_type)


class Null(Base):
    """Represents the missing value which optional inputs may take.
    ``type`` and ``value`` are both None."""

    def __init__(self, expr: "Optional[Expr.Base]" = None) -> None:
        super().__init__(Type.Any(optional=True), None, expr)

    def coerce(self, desired_type: Optional[Type.Base] = None) -> Base:
        ""
        if desired_type and not desired_type.optional and not isinstance(desired_type, Type.Any):
            # normally the typechecker should prevent this, but it might have
            # had check_quant=False
            if isinstance(desired_type, Type.String):
                return String("", self.expr)
            if isinstance(desired_type, Type.Array) and desired_type.item_type.optional:
                return Array(desired_type, [self.coerce(desired_type.item_type)], self.expr)
            if self.expr:
                raise Error.NullValue(self.expr)
            raise Error.InputError("'None' for non-optional input/declaration")
        return self

    @property
    def json(self) -> Any:
        ""
        return None


class Struct(Base):
    value: Dict[str, Base]

    def __init__(
        self,
        type: Union[Type.Object, Type.StructInstance],
        value: Dict[str, Base],
        expr: "Optional[Expr.Base]" = None,
    ) -> None:
        super().__init__(type, value, expr)
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
            return Struct(desired_type, self.value, self.expr)
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
    Instantiate a WDL value of the specified type from a parsed JSON value (str, int, float, list,
    dict, or null).

    If type is :class:`WDL.Type.Any()`, attempts to infer a WDL type & value from the JSON's
    intrinsic types. This isn't ideal; for example, Files can't be distinguished from Strings, and
    JSON lists and dicts with heterogeneous item types may give undefined results.

    :raise WDL.Error.InputError: if the given value isn't coercible to the specified type
    """
    if isinstance(type, Type.Any):
        return _infer_from_json(value)
    if isinstance(type, (Type.Boolean, Type.Any)) and value in [True, False]:
        return Boolean(value)
    if isinstance(type, (Type.Int, Type.Any)) and isinstance(value, int):
        return Int(value)
    if isinstance(type, (Type.Float, Type.Any)) and isinstance(value, (float, int)):
        return Float(float(value))
    if isinstance(type, Type.File) and isinstance(value, str):
        return File(value)
    if isinstance(type, (Type.String, Type.Any)) and isinstance(value, str):
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
    raise Error.InputError(f"couldn't construct {str(type)} from {json.dumps(value)}")


def _infer_from_json(j: Any) -> Base:
    if isinstance(j, str):
        return String(j)
    if isinstance(j, bool):
        return Boolean(j)
    if isinstance(j, int):
        return Int(j)
    if isinstance(j, float):
        return Float(j)
    if j is None:
        return Null()
    if isinstance(j, list):
        items = [_infer_from_json(v) for v in j]
        item_type = Type.unify([item.type for item in items])
        return Array(item_type, [item.coerce(item_type) for item in items])
    if isinstance(j, dict):
        items = [(String(str(k)), _infer_from_json(j[k])) for k in j]
        value_type = Type.unify([v.type for _, v in items])
        return Map((Type.String(), value_type), [(k, v.coerce(value_type)) for k, v in items])
    raise Error.InputError(f"couldn't construct value from: {json.dumps(j)}")


def rewrite_files(v: Base, f: Callable[[str], str]) -> Base:
    """
    Produce a deep copy of the given Value with all File names rewritten by the given function
    (including Files nested inside compound Values).
    """

    mapped_files = set()

    def map_files(v2: Base) -> Base:
        if isinstance(v2, File):
            assert id(v2) not in mapped_files, f"File {id(v2)} reused in deepcopy"
            v2.value = f(v2.value)
            mapped_files.add(id(v2))
        for ch in v2.children:
            map_files(ch)
        return v2

    return map_files(copy.deepcopy(v))


def rewrite_env_files(env: Env.Bindings[Base], f: Callable[[str], str]) -> Env.Bindings[Base]:
    """
    Produce a deep copy of the given Value Env with all File names rewritten by the given function.
    """
    return env.map(lambda binding: Env.Binding(binding.name, rewrite_files(binding.value, f)))
