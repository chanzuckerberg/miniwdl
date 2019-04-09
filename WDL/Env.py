"""
Environments, for identifier resolution during WDL typechecking and evaluation.
"""
from typing import List, Any, Callable, Optional, Union

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


class Binding:
    """A single binding"""

    name: str
    ":type: str"
    rhs: Any
    """:type: Union[WDL.Type.Base,WDL.Value.Base,WDL.StructTypeDef,WDL.Decl.Base]
    
    "Right-hand side" of the binding"""

    ctx: Optional[Any]
    "Arbitrary, secondary context also associated with name"

    def __init__(self, name: str, rhs: Any, ctx: Optional[Any] = None) -> None:
        self.name = name
        self.rhs = rhs
        self.ctx = ctx

    def __repr__(self) -> str:
        return "{}: {}".format(self.name, str(self.rhs))


class Namespace:
    """Encapsulates binding(s) under a namespace"""

    namespace: str
    """:type: str"""
    bindings: List[Union[Binding, "Namespace"]]
    """
    :type: List[Union[WDL.Env.Binding,WDL.Env.Namespace]]

    a list of bindings and/or sub-namespaces"""

    def __init__(self, namespace: str, bindings: List[Union[Binding, "Namespace"]]) -> None:
        self.namespace = namespace
        self.bindings = bindings

    def __repr__(self):
        return "{}. {}".format(self.namespace, str(self.bindings))


Node = Union[Binding, Namespace]
""":type: Union[WDL.Env.Binding,WDL.Env.Namespace]

``WDL.Env.Tree = List[WDL.Env.Node]`` is the polymorphic data structure for an
environment mapping names onto some associated values (nicknamed ``rhs`` for
right-hand side of bindings). It consists of a Python list of
``WDL.Env.Binding`` and/or ``WDL.Env.Namespace`` objects, where the latter has
a nested ``WDL.Env.Tree``.

For example, type bindings for ``x : Float`` and ``adder.sum : Int`` would be
represented as:

``[Binding("x",Float), Namespace("adder",[Binding("sum",Int)])]``

Once constructed, environments should be considered immutable. There should be
no name or namespace collisions.

``WDL.Env.{Types,Values,StructTypeDefs,Decls}`` are type aliases for ``Tree``
with the respective `Binding.rhs` type.
"""

Tree = List[Node]
""":type: List[Node]"""

Types = Tree
""":type: WDL.Env.Tree[WDL.Type.Base]
Type nickname for environment tree of names to WDL types (``WDL.Type.Base`` instances)"""

Values = Tree
""":type: WDL.Env.Tree[WDL.Value.Base]"""

StructTypeDefs = Tree
""":type: WDL.Env.Tree[WDL.Tree.StructTypeDef]"""

Decls = Tree
""":type: WDL.Env.Tree[WDL.Tree.Decl]"""


def resolve_namespace(tree: Tree, namespace: List[str]) -> Tree:
    if not namespace:
        return tree
    for node in tree:
        if isinstance(node, Namespace):
            if namespace[0] == node.namespace:
                return resolve_namespace(node.bindings, namespace[1:])
    raise KeyError()


def resolve_binding(tree: Tree, namespace: List[str], name: str) -> Binding:
    """
    Resolve a name within an environment to the corresponding ``Binding``
    object
    """
    ns = resolve_namespace(tree, namespace)
    for node in ns:
        if isinstance(node, Binding) and node.name == name:
            return node
    raise KeyError()


def resolve(tree: Tree, namespace: List[str], name: str) -> Any:
    """Resolve a name within an environment to its ``Binding.rhs``"""
    return resolve_binding(tree, namespace, name).rhs


def resolve_ctx(tree: Tree, namespace: List[str], name: str) -> Any:
    """Resolve a name to its secondary context value"""
    return resolve_binding(tree, namespace, name).ctx


def bind(tree: Tree, namespace: List[str], name: str, rhs: Any, ctx: Any = None) -> Tree:
    """
    Return a copy of ``tree`` with a new binding prepended. (Does not check for
    name collision!)

    :param namespace: the binding is added to any existing bindings under a matching ``Namespace`` node, with any new nodes added as needed.
    """
    assert name
    if not namespace:
        return [Binding(name, rhs, ctx)] + tree  # pyre-ignore
    assert namespace[0]
    ans: Tree = []
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
        # pyre-ignore
        ans = [Namespace(namespace[0], bind([], namespace[1:], name, rhs, ctx=ctx))] + ans
    return ans


def map(
    tree: Tree, fn: Callable[[List[str], Binding], Any], namespace: Optional[List[str]] = None
) -> Tree:
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
    tree: Tree, keep: Callable[[List[str], Binding], bool], namespace: Optional[List[str]] = None
) -> Tree:
    """
    Copy ``tree`` with only those bindings satisfying the predicate
    ``keep(namespace, binding)``. Any ``Namespace`` nodes which become empty
    are also removed.
    """
    namespace = namespace or []
    ans: Tree = []
    for node in tree:
        if isinstance(node, Binding) and keep(namespace, node):
            ans.append(node)
        elif isinstance(node, Namespace):
            children = filter(node.bindings, keep, namespace + [node.namespace])
            if children:
                ans.append(Namespace(node.namespace, children))
    return ans


def unbind(tree: Tree, namespace: List[str], name: str) -> Tree:
    """
    Return a copy of ``tree`` without the specified binding. No error is raised
    if there is no such binding.

    :param namespace: any ``Namespace`` nodes which become empty as a result of the binding's removal, are also removed.
    """
    assert name
    return filter(
        tree, lambda a_namespace, binding: a_namespace != namespace or binding.name != name
    )


def subtract(lhs: Tree, rhs: Tree) -> Tree:
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
