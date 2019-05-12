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

1. ``Int`` coerces to ``Float``
2. ``Boolean``, ``Int``, ``Float``, and ``File`` coerce to ``String``
3. ``String`` coerces to ``File``
4. ``Array[T]`` coerces to ``String`` provided ``T`` does as well
5. ``T`` coerces to ``T?`` but the reverse is not true in general*
6. ``Array[T]+`` coerces to ``Array[T]`` but the reverse is not true in general*

(*) The reverse coercions are statically permitted in expressions set up with
``Expr.infer_type(check_quant=False)`` although they may fail at runtime. This
also enables coercion of ``T`` to ``Array[T]+`` (an array of length 1).

.. inheritance-diagram:: WDL.Type
   :top-classes: WDL.Type.Base
"""
from abc import ABC
from typing import Optional, Tuple, Dict, Iterable, List
import copy


class Base(ABC):
    """The abstract base class for WDL types

    Each specific type inherits from this base, e.g.::

        assert issubclass(WDL.Type.Int, WDL.Type.Base)
        assert isinstance(WDL.Type.Array(WDL.Type.Int()), WDL.Type.Base)

    All instances are immutable.
    """

    _optional: bool = False  # immutable!!!

    def coerces(self, rhs: "Base", check_quant: bool = True) -> bool:
        """
        True if this is the same type as, or can be coerced to, ``rhs``.

        :param check_quant: when ``False``, disables static enforcement of the optional (?) type quantifier
        """
        if not check_quant and isinstance(rhs, Array) and self.coerces(rhs.item_type, check_quant):
            # coerce T to Array[T]
            return True
        return (type(rhs).__name__ in [type(self).__name__, "Any"]) and self._check_optional(
            rhs, check_quant
        )

    def _check_optional(self, rhs: "Base", check_quant: bool) -> bool:
        return not (check_quant and (self.optional and not rhs.optional))

    @property
    def optional(self) -> bool:
        """
        :type: bool

        True when the type has the optional quantifier, ``T?``"""
        return self._optional

    @property
    def parameters(self) -> Iterable["Base"]:
        """
        :type: Iterable[WDL.Type.Base]

        The type's parameters, if any (e.g. item type of Array; left & right
        types of Pair; etc.)
        """
        return []

    def copy(self, optional: Optional[bool] = None) -> "Base":
        """
        copy(self, optional : Optional[bool] = None) -> WDL.Type.Base

        Create a copy of the type, possibly with a different setting of the
        ``optional`` quantifier.
        """
        ans: Base = copy.copy(self)
        if optional is not None:
            ans._optional = optional
        return ans

    def __str__(self) -> str:
        return type(self).__name__ + ("?" if self.optional else "")

    def __eq__(self, rhs: "Base") -> bool:
        return isinstance(rhs, Base) and str(self) == str(rhs)


class Any(Base):
    """
    A symbolic type which coerces to any other type; used to represent e.g. the item type of an empty array literal, or the result of read_json().
    """

    def __init__(self, optional: bool = False) -> None:
        self._optional = optional

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        return self._check_optional(rhs, check_quant)


class Boolean(Base):
    def __init__(self, optional: bool = False) -> None:
        self._optional = optional

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        ""
        if isinstance(rhs, String):
            return True
        return super().coerces(rhs, check_quant)


class Float(Base):
    def __init__(self, optional: bool = False) -> None:
        self._optional = optional

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        ""
        if isinstance(rhs, String):
            return True
        return super().coerces(rhs, check_quant)


class Int(Base):
    def __init__(self, optional: bool = False) -> None:
        self._optional = optional

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        ""
        if isinstance(rhs, (Float, String)):
            return True
        return super().coerces(rhs, check_quant)


class File(Base):
    def __init__(self, optional: bool = False) -> None:
        self._optional = optional

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        ""
        if isinstance(rhs, String):
            return True
        return super().coerces(rhs, check_quant)


class String(Base):
    def __init__(self, optional: bool = False) -> None:
        self._optional = optional

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        ""
        if isinstance(rhs, File):
            return self._check_optional(rhs, check_quant)
        return super().coerces(rhs, check_quant)


class Array(Base):
    """
    Array type, parameterized by the type of the constituent items.
    """

    item_type: Base  # TODO: make immutable property
    """
    :type: WDL.Type.Base

    ``item_type`` may be ``Any`` when not known statically, such as in a literal empty array ``[]``.
    """
    _nonempty: bool

    def __init__(self, item_type: Base, optional: bool = False, nonempty: bool = False) -> None:
        assert item_type
        self.item_type = item_type
        assert isinstance(nonempty, bool)
        self._optional = optional
        self._nonempty = nonempty

    def __str__(self) -> str:
        ans = (
            "Array["
            + str(self.item_type)
            + "]"
            + ("+" if self.nonempty else "")
            + ("?" if self.optional else "")
        )
        return ans

    @property
    def nonempty(self) -> bool:
        """
        :type: bool

        True when the type has the nonempty quantifier, ``Array[T]+``
        """
        return self._nonempty

    @property
    def parameters(self) -> Iterable[Base]:
        yield self.item_type

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        ""
        if isinstance(rhs, Array):
            return self.item_type.coerces(rhs.item_type, check_quant) and self._check_optional(
                rhs, check_quant
            )
        if isinstance(rhs, String):
            return self.item_type is None or self.item_type.coerces(String())
        if isinstance(rhs, Any):
            return self._check_optional(rhs, check_quant)
        return False

    def copy(self, optional: Optional[bool] = None, nonempty: Optional[bool] = None) -> Base:
        ans = super().copy(optional)
        if nonempty is not None:
            ans._nonempty = nonempty
        return ans


class Map(Base):
    """
    Map type, parameterized by the (key,value) item type.
    """

    item_type: Tuple[Base, Base]
    """
    :type: Tuple[WDL.Type.Base,WDL.Type.Base]

    The key and value types may be ``Any`` when not known statically, such as in a literal empty map ``{}``.
    """

    def __init__(self, item_type: Tuple[Base, Base], optional: bool = False) -> None:
        self._optional = optional
        if item_type is None:
            item_type = (Any(), Any())
        self.item_type = item_type

    def __str__(self) -> str:
        return (
            "Map["
            + (
                str(self.item_type[0]) + "," + str(self.item_type[1])
                if self.item_type is not None
                else ""
            )
            + "]"
            + ("?" if self.optional else "")
        )

    @property
    def parameters(self) -> Iterable[Base]:
        yield self.item_type[0]
        yield self.item_type[1]

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        ""
        if isinstance(rhs, Map):
            return (
                self.item_type[0].coerces(rhs.item_type[0], check_quant)
                and self.item_type[1].coerces(rhs.item_type[1], check_quant)
                and self._check_optional(rhs, check_quant)
            )
        if isinstance(rhs, Any):
            return self._check_optional(rhs, check_quant)
        return False


class Pair(Base):
    """
    Pair type, parameterized by the left and right item types.
    """

    left_type: Base
    """
    :type: WDL.Type.Base
    """
    right_type: Base
    """
    :type: WDL.Type.Base
    """

    def __init__(self, left_type: Base, right_type: Base, optional: bool = False) -> None:
        self._optional = optional
        self.left_type = left_type
        self.right_type = right_type

    def __str__(self) -> str:
        return (
            "Pair["
            + (str(self.left_type) + "," + str(self.right_type))
            + "]"
            + ("?" if self.optional else "")
        )

    @property
    def parameters(self) -> Iterable[Base]:
        yield self.left_type
        yield self.right_type

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        ""
        if isinstance(rhs, Pair):
            return (
                self.left_type.coerces(rhs.left_type, check_quant)
                and self.right_type.coerces(rhs.right_type, check_quant)
                and self._check_optional(rhs, check_quant)
            )
        if isinstance(rhs, Any):
            return self._check_optional(rhs, check_quant)
        return False


class StructInstance(Base):
    """
    Type of an instance of a struct

    Not to be confused with struct type definition, ``WDL.Tree.StructTypeDef``.
    """

    type_name: str
    """
    :type: str

    The struct type name with which the instance is declared; note that the
    same struct type can go by different names.
    """

    members: Optional[Dict[str, Base]]
    """
    :type: Dict[str,WDL.Type.Base]

    Names and types of the struct members, from the struct type definition
    (available after typechecking)
    """

    def __init__(self, type_name: str, optional: bool = False) -> None:
        self._optional = optional
        self.type_name = type_name
        self.members = None

    def __str__(self) -> str:
        assert self.members
        return _struct_type_id(self.members) + ("?" if self.optional else "")

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        ""
        if isinstance(rhs, StructInstance):
            return self.type_id == rhs.type_id and self._check_optional(rhs, check_quant)
        if isinstance(rhs, Any):
            return self._check_optional(rhs, check_quant)
        return False

    @property
    def type_id(self) -> str:
        """
        :type: str

        A string uniquely describing the member names and types, excluding the struct type name; useful to identify aliased struct types.
        """
        assert isinstance(self.members, dict)
        return _struct_type_id(self.members)

    @property
    def parameters(self) -> Iterable[Base]:
        assert self.members is not None
        return self.members.values()


def _struct_type_id(members: Dict[str, Base]) -> str:
    # generates a content hash of the struct type definition, used to recognize
    # equivalent struct types going by different aliases
    ans = []
    for (name, ty) in sorted(members.items()):
        if isinstance(ty, StructInstance):
            assert ty.members
            ty = _struct_type_id(ty.members) + ("?" if ty.optional else "")
        else:
            ty = str(ty)
        ans.append(name + " : " + ty)
    return "struct(" + ", ".join(ans) + ")"


class Object(Base):
    ""
    # In WDL 1.0, struct instances are created by coercion from object
    # literals. So we need something to represent the type of an object literal
    # (a bag of keys and values) prior to its coercion to a named struct type.
    # But we hide this from docs to avoid confusion with general Object
    # support.

    members: Dict[str, Base]

    def __init__(self, members: Dict[str, Base]) -> None:
        self.members = members

    def __str__(self) -> str:
        ans = []
        for name, ty in sorted(self.members.items()):
            ans.append(name + " : " + str(ty))
        return "object(" + ", ".join(ans) + ")"

    @property
    def parameters(self) -> Iterable[Base]:
        return self.members.values()

    def coerces(self, rhs: Base, check_quant: bool = True) -> bool:
        if isinstance(rhs, (StructInstance, Object)):
            rhs_members = rhs.members
            assert rhs_members is not None
            # Check whether our keys match the struct members, and our types
            # are coercible to the respective member types.
            # TODO: in the event of StaticTypeMismatch errors, this may produce
            # unwieldy error messages
            if set(self.members.keys()) != set(rhs_members.keys()):
                return False
            for k in self.members.keys():
                if not self.members[k].coerces(rhs_members[k], check_quant):
                    return False
            return True
        if isinstance(rhs, Any):
            return self._check_optional(rhs, check_quant)
        return False
