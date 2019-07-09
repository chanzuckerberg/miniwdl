# pyre-strict
"""
The **plan** is a directed acyclic graph (DAG) representing a WDL workflow, derived from the AST
but providing a more-convenient model of the internal dependencies. It's an intermediate
representation used to inform scheduling of workflow execution, whatever the backend.

The DAG nodes correspond to each workflow element (Decl, Call, Scatter, Conditional, Gather), and
each Node keeps a set of the Nodes on which it depends. Each Node has a human-readable ID string,
and its dependencies are represented as sets of these IDs. Abstractly, workflow execution proceeds
by "visiting" each node after all of its dependencies have been visited, if any. Each node
prescribes actions to take upon its visitation, according to its particular type.

Scatter nodes contain a "sub-plan", which is like a prototype for the sub-DAG to be instantiated
with some multiplicity determined only upon runtime evaluation of the scatter array expression.
At runtime, dependencies of Gather nodes on the sub-plan nodes should be multiplexed accordingly.
The body of a conditional section is treated similarly, but its only possible multiplicities are 0
and 1.
"""

from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Optional, Set, Iterable, Union
from .. import Error, Env, Expr, Value, StdLib, Tree, _util
from ..Error import SourceNode
from .error import *


class Node(ABC):
    id: str
    "Human-readable node ID, unique within the workflow"
    _memo_dependencies: Optional[Set[str]] = None

    def __init__(self, id: str) -> None:
        self.id = id

    def __str__(self) -> str:
        return "{} < {{{}}}".format(self.id, ", ".join(str(dep) for dep in self.dependencies))

    @property
    @abstractmethod
    def source(self) -> Union[Tree.Decl, Tree.Call, Tree.Scatter, Tree.Conditional, Tree.Gather]:
        "The ``WDL.Tree`` object represented by this node"
        ...

    @property
    def dependencies(self) -> Set[str]:
        "IDs of the nodes upon which this node depends"
        # memoize self._dependencies()
        if self._memo_dependencies is None:
            self._memo_dependencies = set(self._dependencies())
        return self._memo_dependencies

    def _dependencies(self) -> Iterable[str]:
        # subclasses override if the following isn't appropriate for the specific type of node
        return _expr_dependencies(getattr(self.source, "expr"))


class Decl(Node):
    """
    A value declared in the workflow's body or its input/output sections.

    Upon visiting this node, add to the environment a binding for the declared name to the value
    obtained either by evaluating the expression, or from the workflow inputs.
    """

    _source: Tree.Decl

    def __init__(self, source: Tree.Decl) -> None:
        super().__init__("decl:"+source.name)
        self._source = source

    @property
    def source(self) -> Tree.Decl:
        return self._source


class Call(Node):
    """
    Call a task or sub-workflow. On visiting,

    1. Evaluate call input expressions in the environment of value bindings so far accumulated
    2. Run task or sub-workflow with these inputs
    3. Bind call outputs in the appropriate environment namespace
    """

    _source: Tree.Call

    def __init__(self, source: Tree.Call) -> None:
        super().__init__("call:" + source.name)
        self._source = source

    @property
    def source(self) -> Tree.Call:
        return self._source

    def _dependencies(self) -> Iterable[str]:
        for expr in self.source.inputs.values():
            yield from _expr_dependencies(expr)


class Gather(Node):
    """
    Gather an array or optional value from a node within a scatter or conditional section.

    On visiting, bind the name of each decl, call output, or sub-gather to the corresponding
    array of values generated from the multiplexed sub-node. (For Conditional sections, the array
    has length 0 or 1, and None or the value should be bound accordingly)
    """

    _source: Tree.Gather

    def __init__(self, source: Tree.Gather) -> None:
        super().__init__("gather:" + _wrap(source.referee).id)
        self._source = source

    @property
    def source(self) -> Tree.Gather:
        return self._source

    def _dependencies(self) -> Iterable[str]:
        yield _wrap(self.source.referee).id


class Section(Node):
    """
    Common structure for scatter and conditional sections
    """

    body: List[Node]
    "Nodes in the sub-plan DAG"
    gathers: List[Gather]
    "Gather nodes exposed to other nodes in the plan"

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.body = []
        self.gathers = []


class Scatter(Section):
    """
    Scatter section:

    1. Evaluate scatter array expression
    2. For each scatter array element, schedule an instance of the body sub-plan, with an
       environment including the appropriate binding for the scatter variable.
    3. Schedule the Gather operations with the appropriate multiplexed dependencies.
    """

    _source: Tree.Scatter

    def __init__(self, source: Tree.Scatter) -> None:
        super().__init__(source.name)
        self._source = source

    @property
    def source(self) -> Tree.Scatter:
        return self._source


class Conditional(Section):
    """
    Conditional section

    1. Evaluate the boolean expression
    2. If true, schedule the body sub-plan and trivial Gather operations to propagate its results
    3. If false, schedule vacuous Gather operations to propagate None values immediately
    """

    _source: Tree.Conditional

    def __init__(self, source: Tree.Conditional) -> None:
        super().__init__(source.name)
        self._source = source

    @property
    def source(self) -> Tree.Conditional:
        return self._source


def compile(workflow: Tree.Workflow) -> List[Node]:
    """
    Compile a workflow to the top-level plan nodes. The returned order is unspecified.
    """
    nodes: Dict[str, Node] = dict()

    def visit(
        elt: Union[Tree.Decl, Tree.Call, Tree.Scatter, Tree.Conditional, Tree.Gather]
    ) -> Node:
        node = _wrap(elt)
        if isinstance(node, Section):
            assert isinstance(elt, (Tree.Scatter, Tree.Conditional))
            for ch in elt.elements:
                subnode = visit(ch)
                node.body.append(subnode)
                if isinstance(ch, (Tree.Decl, Tree.Call)):
                    g = _wrap(Tree.Gather(section=elt, referee=ch))
                    if not [g2 for g2 in node.gathers if g.id == g2.id]:
                        assert isinstance(g, Gather)
                        node.gathers.append(g)
                        nodes[g.id] = g
                elif isinstance(ch, (Tree.Scatter, Tree.Conditional)):
                    assert isinstance(subnode, Section)
                    for subgather in subnode.gathers:
                        g = _wrap(Tree.Gather(section=elt, referee=nodes[subgather.id].source))
                        if not [g2 for g2 in node.gathers if g.id == g2.id]:
                            assert isinstance(g, Gather)
                            node.gathers.append(g)
                            nodes[g.id] = g
                else:
                    assert False
        assert node.id not in nodes, node.id
        nodes[node.id] = node
        return node

    return [
        visit(elt) for elt in (workflow.inputs or []) + workflow.elements + (workflow.outputs or [])
    ]


def _wrap(elt: Union[Tree.Decl, Tree.Call, Tree.Scatter, Tree.Conditional, Tree.Gather]) -> Node:
    if isinstance(elt, Tree.Decl):
        return Decl(elt)
    if isinstance(elt, Tree.Call):
        return Call(elt)
    if isinstance(elt, Tree.Scatter):
        return Scatter(elt)
    if isinstance(elt, Tree.Conditional):
        return Conditional(elt)
    if isinstance(elt, Tree.Gather):
        return Gather(elt)
    assert False


def _expr_dependencies(expr: Optional[Expr.Base]) -> Iterable[str]:
    if isinstance(expr, Expr.Ident):
        yield _wrap(expr.referee).id
    for ch in expr.children if expr else []:
        yield from _expr_dependencies(ch)
