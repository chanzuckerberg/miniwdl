"""
The **plan** is a directed acyclic graph (DAG) representing a WDL workflow, derived from the AST
with a more explicit & uniform model of the internal dependencies. It's an intermediate
representation used to inform scheduling of workflow execution, whatever the backend.

A node in this DAG represents either:
  - binding a name to a value (obtained from input or by evaluation of a WDL expression)
  - a call to invoke a task or sub-workflow, generating namespaced outputs
  - a scatter or conditional section, containing a sub-DAG to be executed with the
    runtime-determined multiplicity
  - specialized nodes associated with scatter & conditional nodes represent the array & optional
    values, respectively, that will arise from execution of the sub-DAG.
Each node stores a list of dependencies, other nodes from which it has in-edges. Such an edge
from n1 to n2 represents the dependency of n2 on n1, usually including the flow of a
``WDL.Env.Values`` from n1 to n2.

The plan is meant to be pickled easily, with each node assigned a readable ID, and dependencies
referenced by such ID.
"""

from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Optional, Set, Iterable, Union
from .. import Error, Env, Expr, Value, StdLib, Tree, _util
from ..Error import SourceNode
from .error import *


class Node(ABC):
    id: str
    _memo_dependencies: Optional[Set[str]]

    def __init__(self, id: str):
        self.id = id
        self._memo_dependencies = None

    @abstractmethod
    @property
    def source(self) -> Union[Tree.Decl, Tree.Call, Tree.Scatter, Tree.Conditional, Tree.Gather]:
        ...

    @property
    def dependencies(self) -> Set[str]:
        if self._memo_dependencies is None:
            self._memo_dependencies = set(self._dependencies())
        return self._memo_dependencies

    def _dependencies(self) -> Iterable[str]:
        return _expr_dependencies(getattr(self.source, "expr"))


class Decl(Node):
    _source: Tree.Decl

    def __init__(self, source: Tree.Decl) -> None:
        super().__init__(source.name)
        self._source = source

    @property
    def source(self) -> Tree.Decl:
        return self._source


class WorkflowOutput(Decl):
    def __init__(self, source: Tree.Decl) -> None:
        super().__init__(source)
        self.id = "output:" + self.id


class Call(Node):
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
    _source: Tree.Gather

    def __init__(self, source: Tree.Gather) -> None:
        super().__init__("gather:" + _wrap(source.referee).id)
        self._source = source

    @property
    def source(self) -> Tree.Gather:
        return self._source

    def _dependencies(self) -> Iterable[str]:
        yield _wrap(self.source.referee).id


class Scatter(Node):
    _source: Tree.Scatter
    body: List[Node]
    gathers: List[Gather]

    def __init__(self, source: Tree.Scatter) -> None:
        super().__init__(source.name)
        self._source = source
        self.body = []
        self.gathers = []

    @property
    def source(self) -> Tree.Scatter:
        return self._source


class Conditional(Node):
    _source: Tree.Conditional
    body: List[Node]
    gathers: List[Gather]

    def __init__(self, source: Tree.Conditional) -> None:
        super().__init__(source.name)
        self._source = source
        self.body = []
        self.gathers = []

    @property
    def source(self) -> Tree.Conditional:
        return self._source


def compile(workflow: Tree.Workflow, workflow_inputs: Env.Values) -> Iterable[Node]:
    gathers: Dict[int, List[Tree.Gather]] = {}  # indexed by id(tree section object)

    def visit(
        elt: Union[Tree.Decl, Tree.Call, Tree.Scatter, Tree.Conditional, Tree.Gather]
    ) -> Iterable[Node]:
        yield _wrap(elt)
        if isinstance(elt, (Tree.Scatter, Tree.Conditional)):
            assert id(elt) not in gathers
            gathers[id(elt)] = []
            for ch in elt.elements:
                yield from visit(ch)
                if isinstance(ch, (Tree.Decl, Tree.Call)):
                    g = Tree.Gather(section=elt, referee=ch)
                    gathers[id(elt)].append(g)
                    yield _wrap(g)
                elif isinstance(ch, (Tree.Scatter, Tree.Conditional)):
                    for subgather in gathers[id(ch)]:
                        g = Tree.Gather(section=elt, referee=subgather)
                        gathers[id(elt)].append(g)
                        yield _wrap(g)
                else:
                    assert False

    for elt in workflow.inputs or []:
        yield from visit(elt)
    for elt in workflow.elements:
        yield from visit(elt)
    for elt in workflow.outputs or []:
        yield from visit(elt)


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


def _expr_dependencies(expr: Expr.Base) -> Iterable[str]:
    if isinstance(expr, Expr.Ident):
        yield _wrap(expr.referee).id
    for ch in expr.children:
        yield from _expr_dependencies(ch)
