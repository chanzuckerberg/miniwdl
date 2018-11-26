# pyre-strict
"""
Environments, for identifier resolution during WDL typechecking and evaluation.
"""
from typing import List, TypeVar, Generic, Any
import WDL.Type as T
import WDL.Value as V

"""
FIXME: we haven't found exactly the right way to write the type annotations for
       a recursive, polymorphic data structure. We are trying to do this:

type 'a node =
    | Binding of string*'a
    | Namespace of string*('a tree)
and 'a tree = 'a node list
type types = Type.Base tree
type values = Value.Base tree

It is a recursive tree to handle namespaces, and polymorphic because we're trying
to share code for both type and value environments.
"""

R = TypeVar("R")
Tree = TypeVar("Tree", bound="List[Node[R]]")


class Binding(Generic[R]):
    """A single binding"""

    name: str
    ":type: str"
    rhs: R
    """:type: Union[WDL.Type.Base,WDL.Value.Base]"""

    ctx: Any
    "Arbitrary, secondary context also associated with name"

    def __init__(self, name: str, rhs: R, ctx: Any = None) -> None:
        self.name = name
        self.rhs = rhs
        self.ctx = ctx


class Namespace(Generic[R]):
    """Encapsulates binding(s) under a namespace"""

    namespace: str
    """:type: str"""
    bindings: "Tree[R]"
    """
    :type: List[Union[WDL.Env.Binding,WDL.Env.Namespace]]

    a list of bindings and/or sub-namespaces"""

    def __init__(self, namespace: str, bindings: "Tree[R]") -> None:
        self.namespace = namespace
        self.bindings = bindings


Node = TypeVar("Node", Binding[R], Namespace[R])

Types = TypeVar("Types", bound="Tree[Type.Base]")
"""Environment of types, an immutable list of bindings to types and/or namespaces"""

Values = TypeVar("Values", bound="Tree[Value.Base]")
"""Environment of values, an immutable list of bindings to values and/or namespaces"""


def bind(name: str, rhs: R, tree: "Tree[R]", ctx: Any = None) -> "Tree[R]":
    """Prepend a new binding to an environment"""
    return [Binding(name, rhs, ctx)] + tree


def namespace(namespace: str, bindings: "Tree[R]", tree: "Tree[R]") -> "Tree[R]":
    """Prepend a namespace to an environment"""
    return [Namespace(namespace, bindings)] + tree


def resolve_namespace(tree: "Tree[R]", namespace: List[str]) -> R:
    if not namespace:
        return tree
    for node in tree:
        if isinstance(node, Namespace):
            if namespace and namespace[0] == node.namespace:
                return resolve_namespace(node.bindings, namespace[1:])
    raise KeyError()


def resolve_binding(tree: "Tree[R]", namespace: List[str], name: str) -> Binding[R]:
    """
    Resolve a name within an environment to the corresponding Binding object
    """
    ns = resolve_namespace(tree, namespace)
    for node in ns:
        if isinstance(node, Binding) and node.name == name:
            ans: Binding[R] = node
            return ans
    raise KeyError()


def resolve(tree: "Tree[R]", namespace: List[str], name: str) -> R:
    """Resolve a name within an environment"""
    ans: R = resolve_binding(tree, namespace, name).rhs
    return ans


def resolve_ctx(tree: "Tree[R]", namespace: List[str], name: str) -> Any:  # pyre-ignore
    """Resolve a name to its secondary context value"""
    ans: Any = resolve_binding(tree, namespace, name).ctx
    return ans


# print(arrayize([Binding('x',T.Int())])[0].rhs)
# assert resolve([Binding('x',T.Int())], [], 'x') == T.Int()
