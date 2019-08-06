# pyre-strict
from typing import (
    NamedTuple,
    Optional,
    TypeVar,
    Generic,
    Any,
    List,
    Dict,
    Callable,
    Iterable,
    Union,
    Set,
    Iterator,
)

T = TypeVar("T")
S = TypeVar("S")


class Binding(Generic[T]):
    _name: str
    _value: T
    _info: Any  # pyre-ignore

    def __init__(self, name: str, value: T, info: Any = None) -> None:  # pyre-ignore
        self._name = name
        self._value = value
        self._info = info

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self) -> T:
        return self._value

    @property
    def info(self) -> Any:  # pyre-ignore
        return self._info

    def __str__(self):
        return self.name + ": " + str(self.value)


class _EmptyNamespace:
    namespace: str

    def __init__(self, namespace: str) -> None:
        assert namespace.endswith(".")
        self.namespace = namespace


class Bindings(Generic[T]):
    _binding: Union[None, Binding[T], _EmptyNamespace]
    _next: "Optional[Bindings[T]]"
    _namespaces: Optional[Set[str]] = None

    def __init__(
        self,
        binding: Union[None, Binding[T], _EmptyNamespace] = None,
        next: "Optional[Bindings[T]]" = None,
    ) -> None:
        assert binding or not next
        self._binding = binding
        self._next = next

    def __bool__(self) -> bool:
        return self._binding is not None

    def __iter__(self) -> Iterator[Binding[T]]:
        mask = set()
        pos = self
        while pos:
            if isinstance(pos._binding, Binding) and pos._binding.name not in mask:
                mask.add(pos._binding.name)
                yield pos._binding
            pos = pos._next

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def bind(self, name: str, value: T, info: Any = None) -> "Bindings[T]":  # pyre-ignore
        assert name and not (name.startswith(".") or name.endswith("."))
        return Bindings(Binding(name, value, info), self)

    def resolve_binding(self, name: str) -> Binding[T]:
        for b in self:
            if b.name == name:
                return b
        raise KeyError()

    def resolve(self, name: str) -> T:
        return self.resolve_binding(name).value

    def has_binding(self, name: str) -> bool:
        try:
            self.resolve(name)
            return True
        except KeyError:
            return False

    def map(self, f: Callable[[Binding[T]], Optional[Binding[S]]]) -> "Bindings[S]":
        ans = Bindings()
        for b in self:
            fb = f(b)
            if isinstance(fb, Binding):
                ans = Bindings(fb, ans)
        return _rev(ans)

    def iter(self, f: Callable[[Binding[T]], None]) -> None:
        self.map(f)

    def filter(self, pred: Callable[[Binding[T]], bool]) -> "Bindings[T]":
        return self.map(lambda b: b if pred(b) else None)

    def subtract(self, rhs: "Bindings[S]") -> "Bindings[T]":
        def flt(b: Binding[T]) -> bool:
            try:
                rhs.resolve(b.name)
                return False
            except KeyError:
                return True

        return self.filter(flt)

    def with_empty_namespace(self, namespace: str) -> "Bindings[T]":
        assert namespace
        if not namespace.endswith("."):
            namespace += "."
        try:
            self.resolve(namespace[:-1])
            assert False
        except KeyError:
            pass
        return Bindings(_EmptyNamespace(namespace), self)

    @property
    def namespaces(self) -> Set[str]:
        if self._namespaces is None:
            self._namespaces = self._next.namespaces if self._next else set()
            if isinstance(self._binding, _EmptyNamespace):
                self._namespaces.add(self._binding.namespace)
            if isinstance(self._binding, Binding):
                pi = self._binding.name.rfind(".")
                if pi >= 0:
                    assert pi > 0 and pi < len(self._binding.name) - 1
                    ns = self._binding.name[: pi + 1]
                    assert ns.endswith(".")
                    self._namespaces.add(ns)
        return self._namespaces.copy()

    def has_namespace(self, namespace: str) -> bool:
        assert namespace
        if not namespace.endswith("."):
            namespace += "."
        return namespace in (self._namespaces if self._namespaces else self.namespaces)

    def enter_namespace(self, namespace: str) -> "Bindings[T]":
        assert namespace
        if not namespace.endswith("."):
            namespace += "."

        def enter(b: Binding[T]) -> Optional[Binding[T]]:
            if b.name.startswith(namespace):
                return Binding(b.name[len(namespace) :], b.value, b.info)
            else:
                return None

        return self.map(enter)

    def wrap_namespace(self, namespace: str) -> "Bindings[T]":
        assert namespace
        if not namespace.endswith("."):
            namespace += "."
        ans = Bindings()
        pos = self
        while pos:
            if isinstance(pos._binding, Binding):
                ans = Bindings(
                    Binding(namespace + pos._binding.name, pos._binding.value, pos._binding.info),
                    ans,
                )
            if isinstance(pos._binding, _EmptyNamespace):
                ans = Bindings(_EmptyNamespace(namespace + pos._binding.namespace), ans)
            pos = pos._next
        return _rev(ans)


def _rev(env: Bindings[T]) -> Bindings[T]:
    ans = Bindings()
    pos = env
    while pos:
        if pos._binding:
            ans = Bindings(pos._binding, ans)
        pos = pos._next
    return ans


def merge(*args: List[Bindings[T]]) -> Bindings[T]:
    """
    Merge evironments. If multiple environments have bindings for the same (namespaced) name, the
    result includes one of these bindings chosen arbitrarily.
    """
    ans = [Bindings()]

    def visit(b: Binding[T]) -> None:
        if not ans[0].has_binding(b.name):
            ans[0] = Bindings(b, ans[0])

    for env in args:
        env.iter(visit)
    # TODO: add empty namespaces
    return ans[0]
