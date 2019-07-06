"""
The **plan** is a directed acyclic graph (DAG) representing a WDL workflow, compiled from the AST
with a more explicit & uniform model of the internal dependencies. It's an intermediate
representation to guide the scheduling of workflow execution, whatever the backend.

A node in this DAG represents either:
  - binding of name(s) to value(s) obtained by evaluation of WDL expression(s)
  - a call to invoke a task or sub-workflow, generating namespaced outputs
  - a scatter or conditional section, containing a sub-DAG to be executed with the respective
    semantics
An edge from n1 to n2 in the DAG represents the dependency of n2 on n1, usually including the flow
of a ``WDL.Env.Values`` from n1 to n2.
"""

from abc import ABC, abstractmethod
from typing import Tuple, List, Dict, Optional, Set, Iterable
from .. import Error, Env, Expr, Value, StdLib, Tree, _util, SourceNode
from .error import *

class Node(ABC):
    id: str
    source: SourceNode
    dependencies: "List[Node]"

    def __init__(self, id: str, source: SourceNode):
        self.id = id
        self.source = source
        self.dependencies = []

class Binding(Node):
    namespace: List[str]
    name: str

    def __init__(self, namespace: List[str], name: name, source: SourceNode) -> None:
        super().__init__(".".join(namespace+[name]), source)
        self.namespace = namespace
        self.name = name

class Decl(Node):
    source: Tree.Decl
    binding: Binding

    def __init__(self, source: Tree.Decl) -> None:
        super().__init__("decl:" + source.name, source)
        self.source = source

    def _populate_binding(self) -> None:
        self.binding = Binding([], self.source.name, self.source)
        self.binding.dependencies.append(self)

class Output(Decl):
    pass

class Call(Node):
    source: Tree.Call
    outputs: List[Binding]

    def __init__(self, source: Tree.Call) -> None:
        super().__init__("call:"+source.name, source)

    def _populate_outputs(self) -> None:
        def add_output(namespace: List[str], name: str, binding: Env.Binding) -> bool:
            b = Binding(namespace, name, self.source)
            b.dependencies.append(self)
            return True
        Env.filter(self.source.effective_outputs, add_output)

class Scatter(Node):
    source: Tree.Scatter
    body: List[Node]
    gathers: "List[Gather]"
    
    def __init__(self, source: Tree.Scatter) -> None:
        super().__init__("scatter_" + source.variable, source)
        self.body = []
        self.gathers = []

    def _populate_gathers(self) -> None:
        for n in self.body:
            if isinstance(n, Decl):
                self.gathers.append(Gather(n))
            elif isinstance(n, Call):
                for outp in n.outputs:
                    self.gathers.append(Gather(outp))
            elif isinstance(n, Scatter):
                for subgather in n.gathers:
                    self.gathers.append(Gather(subgather))
            elif isinstance(n, Conditional):
                for v in n.values:
                    self.gathers.append(Gather(v))
            else:
                assert False


class Gather(Node):
    """
    A ``Gather`` node represents the array of results arising from a node within a scatter section,
    as seen by other workflow elements outside of the scatter. It stores one dependency which is
    the said node inside the scatter body. (Note that this may itself be another ``Gather`` node,
    in the case of nested sections.)
    """

    def __init__(self, dependency: Node) -> None:
        super().__init__("gather:" + dependency.id, dependency.source)
        self.dependencies.append(dependency)

    @property
    def dependency(self) -> Node:
        return self.dependencies[0]

class Conditional(Node):
    source: Tree.Conditional
    body: List[Node]
    values: "List[ConditionalValue]"

    def __init__(self, source: Tree.Conditional) -> None:
        super().__init__("if_L" + source.pos.line, source)
        self.body = []
        self.values = []

    def _populate_values(self) -> None:
        for n in self.body:
            if isinstance(n, Decl):
                self.values.append(ConditionalValue(n))
            elif isinstance(n, Call):
                for outp in n.outputs:
                    self.values.append(ConditionalValue(outp))
            elif isinstance(n, Scatter):
                for subgather in n.gathers:
                    self.values.append(ConditionalValue(subgather))
            elif isinstance(n, Conditional):
                for v in n.values:
                    self.values.append(ConditionalValue(v))
            else:
                assert False

class ConditionalValue(Node):
    """
    A ``ConditionalValue`` node represents the result of a node within a Conditional section, which
    either takes on a value after execution, or takes on None once execution is decided against.
    Like ``Gather`` it has one dependency inside the conditional body, which may itself be an
    ``Optional`` or ``Gather``.
    """
    def __init__(self, dependency: Node) -> None:
        super().__init__(dependency.id+"?", dependency.source)
        self.dependencies.append(dependency)

    @property
    def dependency(self) -> Node:
        return self.dependencies[0]



def compile(workflow: Tree.Workflow, workflow_inputs: Env.Values) -> Dict[str,Node]:
    ...