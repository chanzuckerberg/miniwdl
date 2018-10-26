# pyre-strict
"""
Environments, for identifier resolution during WDL typechecking and evaluation
"""
from typing import List, TypeVar, Generic, Union
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

R = TypeVar('R')
Tree = TypeVar('Tree', bound='List[Node[R]]')

class Binding(Generic[R]):
    """A single binding"""
    name : str
    """bound name"""
    rhs : R
    """a WDL type or value"""

    def __init__(self, name : str, rhs : R) -> None:
        self.name = name
        self.rhs = rhs

class Namespace(Generic[R]):
    """Encapsulates binding(s) under a namespace"""
    namespace : str
    """namespace"""
    bindings : 'Tree[R]'
    """a list of bindings and/or sub-namespaces"""

    def __init__(self, namespace : str, bindings : 'Tree[R]') -> None:
        self.namespace = namespace
        self.bindings = bindings

Node = TypeVar('Node', Binding[R], Namespace[R])

Types = TypeVar('Types', bound='Tree[Type.Base]')
"""Environment of types, an immutable list of bindings to types and/or namespaces"""

Values = TypeVar('Values', bound='Tree[Value.Base]')
"""Environment of values, an immutable list of bindings to values and/or namespaces"""

def bind(name : str, rhs : R, tree : 'Tree[R]') -> 'Tree[R]':
    """Prepend a new binding to an environment"""
    return [Binding(name, rhs)] + tree

def namespace(namespace : str, bindings : 'Tree[R]', tree : 'Tree[R]') -> 'Tree[R]':
    """Prepend a namespace to an environment"""
    return [Namespace(namespace, bindings)] + tree

def resolve(tree : 'Tree[R]', namespace : List[str], name : str) -> R:
    """Resolve a name within an environment"""
    for node in tree:
        if isinstance(node, Binding):
            if len(namespace) == 0 and node.name == name:
                return node.rhs
        elif isinstance(node, Namespace):
            if len(namespace) > 0 and namespace[0] == node.namespace:
                return resolve(node.bindings, namespace[1:], name)
        else:
            assert False
    raise KeyError()

#print(arrayize([Binding('x',T.Int())])[0].rhs)
#assert resolve([Binding('x',T.Int())], [], 'x') == T.Int()
