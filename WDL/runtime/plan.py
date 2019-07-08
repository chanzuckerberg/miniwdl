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

The plan is meant to be pickled easily, with each node assigned a readable ID.
"""

from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Optional, Set, Iterable
from .. import Error, Env, Expr, Value, StdLib, Tree, _util
from Error import SourceNode
from .error import *


class Node(ABC):
    id: str
    dependencies: Set[str]

    def __init__(self, id: str):
        self.id = id
        self.dependencies = set()


class Decl(Node):
    source: Tree.Decl

    def __init__(self, source: Tree.Decl) -> None:
        super().__init__(source.name)
        self.source = source


class WorkflowOutput(Decl):
    def __init__(self, source: Tree.Decl) -> None:
        super().__init__(source)
        self.id = "output:" + self.id


class Call(Node):
    source: Tree.Call

    def __init__(self, source: Tree.Call) -> None:
        super().__init__("call:" + source.name)
        self.source = source

    # TODO: consider brining back Binding and enumerating the individual call outputs...


class Gather(Node):
    source: Tree.Gather

    def __init__(self, source: Tree.Gather, referee_id: str) -> None:
        super().__init__("gather:" + referee_id)
        self.source = source


class Scatter(Node):
    source: Tree.Scatter
    body: List[Node]
    gathers: List[Gather]

    def __init__(self, source: Tree.Scatter) -> None:
        super().__init__("scatter:" + source.variable)
        self.source = source
        self.body = []
        self.gathers = []


class Conditional(Node):
    source: Tree.Conditional
    body: List[Node]
    gathers: List[Gather]

    def __init__(self, source: Tree.Conditional) -> None:
        super().__init__("if:L{}C{}".format(source.pos.line, source.pos.column))
        self.source = source
        self.body = []
        self.gathers = []


def compile(workflow: Tree.Workflow, workflow_inputs: Env.Values) -> List[Node]:
    #    - does typechecker currently detect circularities involving scatter expression? eg
    #      scatter (i in arr) {
    #          int j = i
    #      }
    #      Array[Int] i = j
    ans = []
    def visit(elt: )
    for elt in (workflow.inputs or []):
        
    for elt in workflow.elements:
    raise NotImplementedError()


def _new_node(elt: Union[Tree.Decl, Tree.Call, Tree.Gather, Tree.Scatter, Tree.Conditional], populate_dependencies: bool=True):
    if isinstance(elt, Tree.Decl):
        ans = Decl(elt)
        ans.dependencies = _expr_dependencies(elt.expr) if populate_dependencies else set()
        return ans
    if isinstance(elt, Tree.Call):
        ans = Call(elt)
        for expr in call.inputs.values() if populate_dependencies else []:
            ans.dependencies.update(_expr_dependencies(expr))
        return ans
    if isinstance(elt, Tree.Gather):
        dep_id = _new_node(elt.referee, populate_dependencies=False).id
        ans = Gather(elt, dep_id)
        if populate_dependencies:
            ans.dependencies.add(dep_id)
        return ans
    if isinstance(elt, Tree.Scatter):
        ans = Scatter(elt)
        ans.dependencies = _expr_dependencies(elt.expr) if populate_dependencies else set()
        return ans
    if isinstance(elt, Tree.Conditional):
        ans = Conditional(elt)
        ans.dependencies = _expr_dependencies(elt.expr) if populate_dependencies else set()
        return ans
    assert False

def _expr_dependencies(expr: Expr.Base) -> Set[str]:
    if isinstance(expr, Expr.Ident):
        return set([_new_node(expr.referee, populate_dependencies=False).id])
    ans = set()
    for ch in expr.children:
        ans.update(_expr_dependencies(ch))
    return ans
