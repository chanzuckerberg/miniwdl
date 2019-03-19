# pyre-strict
"""
Environments, for identifier resolution during WDL typechecking and evaluation.
"""
from typing import List, TypeVar, Generic, Any, Callable, Optional

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
S = TypeVar("S")
Tree = TypeVar("Tree", bound="List[Node[R]]")
""":type: List[Union[WDL.Env.Binding,WDL.Env.Namespace]]

``WDL.Env.Tree`` is the polymorphic data structure for an environment mapping
names onto some associated values (nicknamed ``rhs`` for right-hand side of
bindings). It consists of a Python list of ``WDL.Env.Binding`` and/or
``WDL.Env.Namespace`` objects, where the latter has a nested ``WDL.Env.Tree``.

For example, type bindings for 'x : Float' and 'adder.sum : Int' would be
represented as:

``[Binding("x",Float), Namespace("adder",[Binding("sum",Int)])]``

Once constructed, environments should be considered immutable. There should be
no name or namespace collisions.
"""


class Binding(Generic[R]):
    """A single binding"""

    name: str
    ":type: str"
    rhs: R
    """:type: Union[WDL.Type.Base,WDL.Value.Base,WDL.Decl.Base]"""

    ctx: Any
    "Arbitrary, secondary context also associated with name"

    def __init__(self, name: str, rhs: R, ctx: Any = None) -> None:
        self.name = name
        self.rhs = rhs
        self.ctx = ctx

    def __repr__(self):
        return "{}: {}".format(self.name, str(self.rhs))


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

    def __repr__(self):
        return "{}. {}".format(self.namespace, str(self.bindings))


Node = TypeVar("Node", Binding[R], Namespace[R])

Types = TypeVar("Types", bound="Tree[Type.Base]")
""":type: WDL.Env.Tree[WDL.Type.Base]

Type nickname for environment tree of names to types (``WDL.Type.Base`` instances)"""

Values = TypeVar("Values", bound="Tree[Value.Base]")
""":type: WDL.Env.Tree[WDL.Value.Base]

Type nickname for environment tree of names to WDL values (``WDL.Value.Base`` instances)"""

Decls = TypeVar("Decls", bound="Tree[WDL.Tree.Decl]")
""":type: WDL.Env.Tree[WDL.Tree.Decl]

Type nickname for environment tree of names to ``WDL.Tree.Decl`` instances"""


def resolve_namespace(tree: "Tree[R]", namespace: List[str]) -> "Tree[R]":
    if not namespace:
        return tree
    for node in tree:
        if isinstance(node, Namespace):
            if namespace[0] == node.namespace:
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


def bind(tree: "Tree[R]", namespace: List[str], name: str, rhs: R, ctx: Any = None) -> "Tree[R]":
    """
    Return a copy of ``tree`` with a new binding prepended. (Does not check for
    name collision!)

    :param namespace: the binding is added to any existing bindings under a matching ``Namespace`` node, with any new nodes added as needed.
    """
    assert name
    if not namespace:
        return [Binding(name, rhs, ctx)] + tree
    assert namespace[0]
    ans = []
    new_namespace = True
    for node in tree:
        if isinstance(node, Namespace) and node.namespace == namespace[0]:
            ans.append(
                Namespace(node.namespace, bind(node.bindings, namespace[1:], name, rhs, ctx=ctx))
            )
            new_namespace = False
        else:
            ans.append(node)
    if new_namespace:
        ans = [Namespace(namespace[0], bind([], namespace[1:], name, rhs, ctx=ctx))] + ans
    return ans


S = TypeVar("S")


def map(
    tree: "Tree[R]",
    fn: "Callable[[List[str], Binding[R]], S]",
    namespace: Optional[List[str]] = None,
) -> "Tree[S]":
    """
    Copy ``tree`` with the ``rhs`` of each binding replaced by
    ``fn(namespace, binding)``
    """
    namespace = namespace or []
    ans = []
    for node in tree:
        if isinstance(node, Binding):
            ans.append(Binding(node.name, fn(namespace, node), ctx=node.ctx))
        else:
            assert isinstance(node, Namespace)
            ans.append(
                Namespace(node.namespace, map(node.bindings, fn, namespace + [node.namespace]))
            )
    return ans


def filter(
    tree: "Tree[R]",
    keep: "Callable[[List[str], Binding[R]], bool]",
    namespace: Optional[List[str]] = None,
) -> "Tree[R]":
    """
    Copy ``tree`` with only those bindings satisfying the predicate
    ``keep(namespace, binding)``. Any ``Namespace`` nodes which become empty
    are also removed.
    """
    namespace = namespace or []
    ans = []
    for node in tree:
        if isinstance(node, Binding) and keep(namespace, node):
            ans.append(node)
        elif isinstance(node, Namespace):
            children = filter(node.bindings, keep, namespace + [node.namespace])
            if children:
                ans.append(Namespace(node.namespace, children))
    return ans


def unbind(tree: "Tree[R]", namespace: List[str], name: str) -> "Tree[R]":
    """
    Return a copy of ``tree`` without the specified binding. No error is raised
    if there is no such binding.

    :param namespace: any ``Namespace`` nodes which become empty as a result of the binding's removal, are also removed.
    """
    assert name
    return filter(
        tree, lambda a_namespace, binding: a_namespace != namespace or binding.name != name
    )


def subtract(lhs: "Tree[R]", rhs: "Tree[S]") -> "Tree[R]":
    """
    Return a copy of ``lhs`` without any binding matching one in ``rhs`` (by
    name+namespace). Bindings in ``rhs`` but not ``lhs`` are ignored.
    """

    def flt(namespace: List[str], binding: Binding):
        try:
            resolve(rhs, namespace, binding.name)
            return False
        except KeyError:
            return True

    return filter(lhs, flt)
