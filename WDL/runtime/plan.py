# pyre-strict
"""
The **plan** is a directed acyclic graph representing a WDL workflow, derived from the AST but
providing a simpler, explicit model of the internal dependencies. It's an intermediate
representation used to inform job scheduling for workflow execution, whatever the backend.

The graph nodes correspond to each workflow element (Decl, Call, Scatter, Conditional, and
implicitly Gather), and each Node keeps a set of the Nodes on which it depends. To facilitate
instantiation of the graph on diverse scheduler backends, each node has a human-readable ID string,
with the dependencies encoded as sets of these IDs. Abstractly, workflow execution proceeds by
"visiting" each node after all of its dependencies have been visited. Each node prescribes a job to
do upon its visitation, according to its particular type. Named WDL values (``WDL.Env.Values``) are
transmitted along each dependency edge, and any WDL expressions in each node are evaluated in the
environment formed from the union of the node's incoming dependency edges.

Scatter nodes contain a "sub-plan", which is like a prototype for the job subgraph to be scheduled
for each element of the runtime-evaluated scatter array. They also contain prototype Gather nodes,
each dependent on a sub-plan node. Once all the job subgraphs have been scheduled, the Gather jobs
should be scheduled as well, with the sub-plan dependency multiplexed to the corresponding jobs.
Conditional sections are treated similarly but their only possible subgraph multiplicities are 0
and 1.
"""

from abc import ABC
from typing import List, Dict, Optional, Set, Iterable, Union
from .. import Expr, Tree


class Node(ABC):
    "Base class for plan node"

    id: str
    "Human-readable node ID, unique within the workflow"
    _memo_dependencies: Optional[Set[str]] = None

    def __init__(self, id: str) -> None:
        self.id = id

    def __str__(self) -> str:
        return "{} < {{{}}}".format(self.id, ", ".join(str(dep) for dep in self.dependencies))

    @property
    def dependencies(self) -> Set[str]:
        "IDs of the nodes upon which this node depends"
        # memoize self._dependencies()
        if self._memo_dependencies is None:
            self._memo_dependencies = set(self._dependencies())
        return self._memo_dependencies

    def _dependencies(self) -> Iterable[str]:
        # each node should study its AST to self-describe the IDs of the nodes it depends on.
        # the following applies to most but not all node subclasses:
        return _expr_dependencies(getattr(getattr(self, "source"), "expr"))


class Decl(Node):
    """
    A value declared in the workflow's body or its input/output sections.

    Upon visiting this node, add to the environment a binding for the declared name to the value
    obtained either (i) by evaluating the expression, or (ii) from the workflow inputs.
    """

    source: Tree.Decl

    def __init__(self, source: Tree.Decl) -> None:
        super().__init__("decl-" + source.name)
        self.source = source


class Call(Node):
    """
    Call a task or sub-workflow. On visiting,

    1. Evaluate call input expressions in the environment of value bindings so far accumulated
    2. Run task or sub-workflow with these inputs
    3. Bind call outputs in the appropriate environment namespace
    """

    source: Tree.Call

    def __init__(self, source: Tree.Call) -> None:
        super().__init__("call-" + source.name)
        self.source = source

    def _dependencies(self) -> Iterable[str]:
        for expr in self.source.inputs.values():
            yield from _expr_dependencies(expr)


class Gather(Node):
    """
    Gather an array or optional value from a node within a scatter or conditional section. This
    operation is implicit in the WDL syntax, but presented explicitly here.

    On visiting, bind the name of each decl, call output, or nested gather to the corresponding
    array of values generated from the multiplexed sub-node. (For Conditional sections, the array
    has length 0 or 1, and None or the value should be bound accordingly.)
    """

    source: Tree.Gather

    def __init__(self, source: Tree.Gather) -> None:
        super().__init__("gather-" + _wrap(source.referee).id)
        self.source = source

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
    Scatter section

    On visiting, evaluate the scatter array expression. Then for each scatter array element,
    schedule an instance of the body sub-plan with a forked environment including the appropriate
    binding for the scatter variable. Lastly, schedule the Gather operation for each sub-plan node,
    with dependencies multiplexed to the corresponding jobs.
    """

    source: Tree.Scatter

    def __init__(self, source: Tree.Scatter) -> None:
        super().__init__(source.name)
        self.source = source


class Conditional(Section):
    """
    Conditional section

    On visiting, evaluate the boolean expression. If true, schedule the body sub-plan and trivial
    Gather operations to propagate its results. Otherwise, schedule vacuous Gather operations to
    propagate None values.
    """

    source: Tree.Conditional

    def __init__(self, source: Tree.Conditional) -> None:
        super().__init__(source.name)
        self.source = source


class WorkflowOutputs(Node):
    """
    A no-op node which depends on each ``Decl`` node from the workflow output section. Or, if the
    workflow is missing the output section, depends on ``Call`` and ``Gather`` nodes for all call
    outputs.

    There is no WorkflowInputs node because all plan nodes implicitly depend on the workflow
    inputs, which have no dependencies by definition.
    """

    output_node_ids: Set[str]

    def __init__(self, output_node_ids: Iterable[str]) -> None:
        super().__init__("outputs")
        self.output_node_ids = set(output_node_ids)

    def _dependencies(self) -> Iterable[str]:
        yield from self.output_node_ids


def compile(workflow: Tree.Workflow) -> List[Node]:
    """
    Compile a workflow to the top-level plan nodes. The workflow shall have been typechecked
    already, and thus known acyclic.
    
    The returned list has no particular order.
    """

    # traverse the AST depth-first, generating the Node wrapper for each element, and the Gather
    # nodes associated with each scatter/conditional section.
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
                    assert isinstance(g, Gather)
                    node.gathers.append(g)
                elif isinstance(ch, (Tree.Scatter, Tree.Conditional)):
                    assert isinstance(subnode, Section)
                    for subgather in subnode.gathers:
                        g = _wrap(Tree.Gather(section=elt, referee=subgather.source))
                        assert isinstance(g, Gather)
                        node.gathers.append(g)
                else:
                    assert False
        return node

    ans = [visit(elt) for elt in (workflow.inputs or []) + workflow.elements]

    # tack on WorkflowOutputs
    if workflow.outputs is not None:
        output_nodes = [visit(elt) for elt in workflow.outputs]
        ans.extend(output_nodes)
        ans.append(WorkflowOutputs(n.id for n in output_nodes))
    else:
        # TODO: instantiate WorkflowOutputs on all top-level Call nodes (and all top-level Gather
        # nodes whose ultimate referee is a Call)
        pass

    # TODO: final sanity check that all dependencies are known nodes

    return ans


_classmap = {}  # pyre-ignore


def _wrap(elt: Union[Tree.Decl, Tree.Call, Tree.Scatter, Tree.Conditional, Tree.Gather]) -> Node:
    global _classmap
    if not _classmap:
        for klass in [Decl, Call, Scatter, Conditional, Gather]:
            _classmap[klass.__name__] = klass
    return _classmap[elt.__class__.__name__](elt)


def _expr_dependencies(expr: Optional[Expr.Base]) -> Iterable[str]:
    if isinstance(expr, Expr.Ident):
        yield _wrap(expr.referee).id
    for ch in expr.children if expr else []:
        yield from _expr_dependencies(ch)
