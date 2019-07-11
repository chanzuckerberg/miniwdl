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
from .. import Expr, Env, Tree
from ..Tree import WorkflowNode


class WorkflowOutputs(WorkflowNode):
    """
    A no-op node which depends on each ``Decl`` node from the workflow output section. Or, if the
    workflow is missing the output section, depends on ``Call`` and ``Gather`` nodes for all call
    outputs.

    There is no WorkflowInputs node because all plan nodes implicitly depend on the workflow
    inputs, which have no dependencies by definition.
    """

    output_node_ids: Set[str]

    def __init__(self, output_node_ids: Iterable[str], pos: Tree.SourcePosition) -> None:
        super().__init__("outputs", pos)
        self.output_node_ids = set(output_node_ids)

    def _workflow_node_dependencies(self) -> Iterable[str]:
        yield from self.output_node_ids

    def add_to_type_env(
        self, struct_typedefs: Env.StructTypeDefs, type_env: Env.Types
    ) -> Env.Types:
        raise NotImplementedError()


def compile(workflow: Tree.Workflow) -> List[WorkflowNode]:
    """
    Compile a workflow to the top-level plan nodes. The workflow shall have been typechecked
    already, and thus known acyclic.
    
    The returned list has no particular order.
    """

    ans = [node for node in (workflow.inputs or []) + workflow.body]

    # tack on WorkflowOutputs
    if workflow.outputs is not None:
        output_nodes = [node for node in workflow.outputs]
        ans.extend(output_nodes)
        ans.append(WorkflowOutputs((n.workflow_node_id for n in output_nodes), workflow.pos))
    else:
        # TODO: instantiate WorkflowOutputs on all top-level Call nodes (and all top-level Gather
        # nodes whose ultimate referee is a Call)
        pass

    # TODO: final sanity check that all dependencies are known nodes

    return ans
