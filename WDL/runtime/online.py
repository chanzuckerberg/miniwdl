# pyre-strict
"""
On-line workflow state machine, suitable for use within a singleton "driver" process with
in-memory state (or pickled between iterations).
"""

import concurrent
from typing import Optional, List, Set, Tuple, NamedTuple, Dict, Union, Iterable
from abc import ABC
from .. import Env, Value, Tree
from . import plan

CallNow = NamedTuple(
    "CallNow", [("id", str), ("callee", Union[Tree.Task, Tree.Workflow]), ("inputs", Env.Values)]
)
"""
The state machine produces a ``CallNow`` object when it's time for the driver to launch a
task/subworkflow job.

:param id: call/job ID string, unique in the workflow
:param callee: ``WDL.Call`` or ``WDL.Workflow`` to launch
:param inputs: ``WDL.Env.Values`` of call inputs
"""

_Job = NamedTuple(
    "Job",
    [
        ("id", str),
        ("node", Tree.WorkflowNode),
        ("dependencies", Set[str]),
        ("binding", Optional[Tuple[str, Value.Base]]),
    ],
)


class StateMachine(ABC):
    inputs: Env.Values
    jobs: Dict[str, _Job]
    job_outputs: Dict[str, Env.Values]
    finished: Set[str]
    running: Set[str]
    waiting: Set[str]

    def __init__(self, workflow_nodes: List[Tree.WorkflowNode], inputs: Env.Values) -> None:
        """
        Initialize the workflow state machine, given the plan and the workflow inputs
        """
        self.inputs = inputs
        self.jobs = {}
        self.job_outputs = {}
        self.finished = set()
        self.running = set()
        self.waiting = set()

        for node in workflow_nodes:
            # TODO: disregard dependencies of any decl node whose value is supplied in inputs
            self._schedule(node)
        assert "outputs" in self.jobs

    @property
    def outputs(self) -> Optional[Env.Values]:
        """
        Workflow outputs, once the workflow is completely finished. ``None`` until then.
        """
        if len(self.finished) < len(self.jobs):
            return None
        ans = self.job_outputs["outputs"]
        assert ans is not None
        return ans

    def step(self) -> Optional[CallNow]:
        """
        Advance the workflow state machine, returning the next call to initiate.

        The driver must start the specified callee task/workflow and then, later upon its
        completion, invoke ``call_finished()`` with its outputs. It is NOT necessary to await the
        call's completion before another ``step()`` for the next call; this allows the driver
        to orchestrate multiple calls at once. Indeed, the driver should launch as many calls as
        it can support concurrently (by calling ``step()`` in a loop until getting back ``None``);
        doing so after initialization and after each ``call_finished()`` invocation, until at last
        the workflow outputs are available.
        """
        # select a job whose dependencies are all finished
        job_id = next(
            (j for j in self.waiting if not (self.jobs[j].dependencies - self.finished)), None
        )
        if not job_id:
            return None
        job = self.jobs[job_id]

        # mark it 'running'
        self.running.add(job.id)
        self.waiting.remove(job.id)

        if isinstance(job.node, Tree.Gather):
            # special use of dependency outputs
            raise NotImplementedError()

        # compute job's environment by merging outputs of all dependencies
        env = _merge_environments(self.job_outputs[dep] for dep in job.dependencies)

        if isinstance(job.node, Tree.Call):
            # evaluate input expressions and issue CallNow
            call_inputs = []
            for name, expr in job.node.inputs.items():
                call_inputs = Env.bind(call_inputs, [], name, expr.eval(env))
            # TODO: check workflow inputs for optional call inputs
            assert isinstance(job.node.callee, (Tree.Task, Tree.Workflow))
            return CallNow(id=job.id, callee=job.node.callee, inputs=call_inputs)

        if isinstance(job.node, Tree.Decl):
            # bind the value obtained either (i) from the workflow inputs or (ii) by evaluating
            # the expr
            try:
                v = Env.resolve(self.inputs, [], job.node.name)
            except KeyError:
                assert job.node.expr
                v = job.node.expr.eval(env)
            self.job_outputs[job.id] = Env.bind([], [], job.node.name, v)

        elif isinstance(job.node, plan.WorkflowOutputs):
            self.job_outputs[job.id] = env  # ez ;)

        elif isinstance(job.node, Tree.WorkflowSection):
            raise NotImplementedError()

        else:
            assert False

        self.finished.add(job.id)
        self.running.remove(job.id)

        # continue stepping through simple jobs that just involve evaluating WDL expressions etc.
        return self.step()

    def call_finished(self, job_id: str, outputs: Env.Values) -> None:
        """
        Deliver notice of a job's successful completion, along with its outputs
        """
        assert job_id in self.running
        call_node = self.jobs[job_id].node
        assert isinstance(call_node, Tree.Call)
        self.job_outputs[job_id] = [Env.Namespace(call_node.name, outputs)]
        self.finished.add(job_id)
        self.running.remove(job_id)

    def _schedule(
        self,
        node: Tree.WorkflowNode,
        index: Optional[int] = None,
        binding: Optional[Tuple[str, Value.Base]] = None,
    ) -> None:
        if isinstance(node, Tree.WorkflowSection):
            raise NotImplementedError()
        job = _Job(
            id=node.workflow_node_id,
            node=node,
            dependencies=set(node.workflow_node_dependencies),
            binding=binding,
        )
        assert job.id not in self.jobs
        self.jobs[job.id] = job
        self.waiting.add(job.id)


def _merge_environments(envs: Iterable[Env.Values]) -> Env.Values:
    ans = [[]]

    def visit(namespace: List[str], binding: Env.Binding) -> None:
        try:
            Env.resolve(ans[0], namespace, binding.name)
        except KeyError:
            ans[0] = Env.bind(ans[0], namespace, binding.name, binding.rhs, binding.ctx)

    for env in envs:
        Env.map(env, visit)
    return ans[0]
