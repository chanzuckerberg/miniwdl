# pyre-strict
"""
Workflow runner building blocks & local driver

--------
Overview
--------

Workflow execution proceeds according to the AST's ``WorkflowNode`` graph, in which each Decl,
Call, Scatter, Conditional, and (implicit) Gather operation has its own node which advertises its
dependencies on the other nodes.

Abstractly, we plan to "visit" each node after visiting all of its dependencies. The node's type
prescribes some job to do upon visitation, such as evaluating a Decl's WDL expression, running a
task on some inputs, or scheduling additional jobs to scatter over an array. Named WDL values
(``WDL.Env.Values``) are transmitted along each dependency edge, and WDL expressions in each node
are evaluated in the environment formed from the union of the node's incoming dependency edges.

Scatter sections contain a body, which provides a template for the job subgraph to be scheduled for
each element of the runtime-evaluated scatter array. They also contain template Gather nodes, each
dependent on a body subgraph node. Once all the body subgraph jobs have been scheduled, the Gather
jobs can be scheduled as well, with their dependencies multiplexed to the corresponding subgraph
jobs. Nodes outside of the scatter section depend on the Gather nodes rather than reaching into the
body subgraph directly.

Conditional sections are treated similarly, but only zero or one instance of its body subgraph will
be launched. Scatter and Conditional sections may be nested, inducing a multi-level tree of Gather
operations.
"""

import concurrent
import os
import math
import itertools
from typing import Optional, List, Set, Tuple, NamedTuple, Dict, Union, Iterable
from abc import ABC
from datetime import datetime
from .. import Env, Type, Value, Tree, StdLib
from .task import run_local_task


class WorkflowOutputs(Tree.WorkflowNode):
    """
    A no-op workflow node which depends on each ``Decl`` node from the workflow output section. Or,
    if the workflow is missing the output section, depends on ``Call`` and ``Gather`` nodes for all
    call outputs.

    The workflow state machine tacks this on to the workflow graph to facilitate assembly of the
    outputs environment.
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

    @property
    def children(self) -> Iterable[Tree.SourceNode]:
        return []


_Job = NamedTuple(
    "Job",
    [
        ("id", str),
        ("node", Tree.WorkflowNode),
        ("dependencies", Set[str]),
        ("scatter_stack", List[Tuple[str, Env.Binding]]),
    ],
)


class StateMachine(ABC):
    """
    On-line workflow state machine, suitable for use within a singleton driver process managing
    in-memory state. The state machine evaluates WDL expressions locally, while instructing the
    driver when to call tasks/subworkflows. It's agnostic to how/where the driver actually executes
    each call, just requiring asynchronous notification of call completion along with the outputs.
    """

    workflow: Tree.Workflow
    inputs: Env.Values
    jobs: Dict[str, _Job]
    job_outputs: Dict[str, Env.Values]
    finished: Set[str]
    running: Set[str]
    waiting: Set[str]

    def __init__(self, workflow: Tree.Workflow, inputs: Env.Values) -> None:
        """
        Initialize the workflow state machine from the workflow AST and inputs
        """
        self.workflow = workflow
        self.inputs = inputs
        self.jobs = {}
        self.job_outputs = {}
        self.finished = set()
        self.running = set()
        self.waiting = set()

        workflow_nodes = [node for node in (workflow.inputs or []) + workflow.body]
        # tack on WorkflowOutputs
        if workflow.outputs is not None:
            output_nodes = [node for node in workflow.outputs]
            workflow_nodes.extend(output_nodes)
            workflow_nodes.append(
                WorkflowOutputs((n.workflow_node_id for n in output_nodes), workflow.pos)
            )
        else:
            # TODO: instantiate WorkflowOutputs on all top-level Call nodes (and all top-level
            # Gather nodes whose ultimate referee is a Call)
            raise NotImplementedError()

        for node in workflow_nodes:
            deps = node.workflow_node_dependencies
            if isinstance(node, Tree.Decl):
                # strike the dependencies of any decl node whose value is supplied in the inputs
                try:
                    Env.resolve(inputs, [], node.name)
                    deps = set()
                except KeyError:
                    pass
            self._schedule(node, dependencies=deps)

        # sanity check
        assert "outputs" in self.jobs
        known_jobs = set(self.waiting)
        for node in workflow_nodes:
            if isinstance(node, Tree.WorkflowSection):
                for gather in node.gathers.values():
                    known_jobs.add(gather.workflow_node_id)
        for job in self.jobs.values():
            assert not (job.dependencies - known_jobs), (
                job.id,
                (job.dependencies - known_jobs),
                known_jobs,
            )

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

    CallInstructions = NamedTuple(
        "CallInstructions",
        [("id", str), ("callee", Union[Tree.Task, Tree.Workflow]), ("inputs", Env.Values)],
    )
    """
    The state machine produces a ``CallInstructions`` object when it's time for the driver to
    launch a task/subworkflow job.

    :param id: call/job ID string, unique in the workflow
    :param callee: ``WDL.Call`` or ``WDL.Workflow`` to launch
    :param inputs: ``WDL.Env.Values`` of call inputs
    """

    def step(self) -> "Optional[StateMachine.CallInstructions]":
        """
        Advance the workflow state machine, returning the next call to initiate.

        The driver must start the specified callee task/workflow and then, later upon its
        completion, invoke ``call_finished()`` with its outputs. It is NOT necessary to await the
        call's completion before another ``step()`` for the next call; this allows the driver
        to orchestrate multiple calls at once. Indeed, the driver should launch as many calls as
        it can support concurrently, by calling ``step()`` in a loop until getting back ``None``;
        doing so after initialization and after each ``call_finished()`` invocation, until at last
        the workflow outputs are available.
        """
        while True:
            # select a job whose dependencies are all finished
            job_id = next(
                (j for j in self.waiting if not (self.jobs[j].dependencies - self.finished)), None
            )
            if not job_id:
                if self.waiting and not self.running:
                    deadlock = set(
                        itertools.chain(*(self.jobs[j].dependencies for j in self.waiting))
                    )
                    deadlock -= self.waiting
                    assert False, "deadlocked on " + str(deadlock)
                return None
            job = self.jobs[job_id]

            # mark it 'running'
            self.running.add(job.id)
            self.waiting.remove(job.id)

            # do the job
            res = self._do_job(job)

            # if it's a call, return instructions to the driver
            if isinstance(res, StateMachine.CallInstructions):
                return res

            # otherwise, record the outputs, mark the job finished, and move on to the next job
            self.job_outputs[job.id] = res
            self.running.remove(job.id)
            self.finished.add(job.id)

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

    # TODO: by topsorting all section bodies we can ensure that when we schedule an additional job,
    # all its dependencies will already have been scheduled, increasing flexibility/compatibility
    # with various backends.
    def _schedule(
        self,
        node: Tree.WorkflowNode,
        job_id: Optional[str] = None,
        dependencies: Optional[Iterable[str]] = None,
        scatter_stack: Optional[List[Tuple[str, Env.Binding]]] = None,
    ) -> None:
        job = _Job(
            id=(job_id or node.workflow_node_id),
            node=node,
            dependencies=set(
                dependencies if dependencies is not None else node.workflow_node_dependencies
            ),
            scatter_stack=list(scatter_stack or []),
        )
        assert job.id not in self.jobs
        self.jobs[job.id] = job
        self.waiting.add(job.id)

    # TODO: how much of the following helper methods can be refactored out of StateMachine if we
    # make them return Job(s) instead of calling self._schedule ?
    # if we also factor out WorkflowState ?
    def _do_job(self, job: _Job) -> "Union[StateMachine.CallInstructions, Env.Values]":
        if isinstance(job.node, Tree.Gather):
            return self._gather(
                job.node, dict((dep_id, self.job_outputs[dep_id]) for dep_id in job.dependencies)
            )

        # for all non-Gather nodes, derive the environment by merging the outputs of all the
        # dependencies (+ any current scatter variable bindings)
        env = Env.merge(
            [p[1] for p in job.scatter_stack], *(self.job_outputs[dep] for dep in job.dependencies)
        )
        stdlib = StdLib.Base()

        if isinstance(job.node, (Tree.Scatter, Tree.Conditional)):
            self._scatter(job.node, env, job.scatter_stack, stdlib)
            # the section node itself has no outputs, so return an empty env
            return []

        if isinstance(job.node, Tree.Decl):
            # bind the value obtained either (i) from the workflow inputs or (ii) by evaluating
            # the expr
            try:
                v = Env.resolve(self.inputs, [], job.node.name)
            except KeyError:
                assert job.node.expr
                v = job.node.expr.eval(env, stdlib=stdlib)
            return Env.bind([], [], job.node.name, v)

        if isinstance(job.node, WorkflowOutputs):
            return env

        if isinstance(job.node, Tree.Call):
            # evaluate input expressions
            call_inputs = []
            for name, expr in job.node.inputs.items():
                call_inputs = Env.bind(call_inputs, [], name, expr.eval(env, stdlib=stdlib))
            # check workflow inputs for additional inputs supplied to this call
            try:
                ns = Env.resolve_namespace(self.inputs, [job.node.name])
                for b in ns:
                    assert isinstance(b, Env.Binding)
                    call_inputs = Env.bind(call_inputs, [], b.name, b.rhs)
            except KeyError:
                pass
            # issue CallInstructions
            assert isinstance(job.node.callee, (Tree.Task, Tree.Workflow))
            return StateMachine.CallInstructions(
                id=job.id, callee=job.node.callee, inputs=call_inputs
            )

        raise NotImplementedError()

    def _scatter(
        self,
        section: Union[Tree.Scatter, Tree.Conditional],
        env: Env.Values,
        scatter_stack: List[Tuple[str, Env.Binding]],
        stdlib: StdLib.Base,
    ) -> None:
        # we'll be tracking, for each body node ID, the IDs of the potentially multiple
        # corresponding jobs scheduled
        multiplex = {}
        for body_node in section.body:
            multiplex[body_node.workflow_node_id] = set()
            if isinstance(body_node, Tree.WorkflowSection):
                for subgather in body_node.gathers.values():
                    multiplex[subgather.workflow_node_id] = set()

        # evaluate scatter array or boolean condition
        v = section.expr.eval(env, stdlib=stdlib)
        array = []
        if isinstance(section, Tree.Scatter):
            assert isinstance(v, Value.Array)
            array = v.value
        else:
            assert isinstance(v, Value.Boolean)
            if v.value:
                # condition is satisfied, so we'll "scatter" over a length-1 array
                array = [None]
        digits = math.ceil(math.log10(len(array) + 1))

        # compile IDs of all body nodes and their gather nodes, which we'll need below
        body_node_ids = set()
        for body_node in section.body:
            body_node_ids.add(body_node.workflow_node_id)
            if isinstance(body_node, Tree.WorkflowSection):
                for gather in body_node.gathers.values():
                    body_node_ids.add(gather.workflow_node_id)

        # for each array element, schedule an instance of the body subgraph
        for i, array_i in enumerate(array):

            # scatter bookkeeping: format the index as a left-zero-padded string so that it'll
            # sort lexicographically in the desired order; bind the scatter variable name to the
            # array value.
            scatter_stack_i = scatter_stack
            if isinstance(array_i, Value.Base):
                assert isinstance(section, Tree.Scatter)
                scatter_stack_i = scatter_stack_i + [
                    (str(i).zfill(digits), Env.Binding(section.variable, array_i))
                ]
            scatter_indices_i = [p[0] for p in scatter_stack_i]

            # schedule each body (template) node
            for body_node in section.body:
                # the job ID will be the template node ID with the current scatter index appended.
                # if we're in nested scatters, then append *each* respective index!
                body_job_id = _append_scatter_indices(body_node.workflow_node_id, scatter_indices_i)

                # furthermore, rewrite the template node's dependencies on other within-scatter
                # nodes to the corresponding jobs given the current scatter index.
                # especially tricky: in a nested scatter we can depend on a node at a higher level,
                # for which we need to append only the indices up to its level!
                dependencies = set()
                for dep_id in body_node.workflow_node_dependencies:
                    dep = self.workflow.get_node(dep_id)
                    assert dep.scatter_depth <= body_node.scatter_depth
                    dep_id = _append_scatter_indices(dep_id, scatter_indices_i[: dep.scatter_depth])
                    assert dep.scatter_depth == body_node.scatter_depth or dep_id in self.jobs
                    dependencies.add(dep_id)

                self._schedule(
                    body_node,
                    job_id=body_job_id,
                    dependencies=dependencies,
                    scatter_stack=scatter_stack_i,
                )

                # record the newly scheduled job & its expected gathers in multiplex
                multiplex[body_node.workflow_node_id].add(body_job_id)
                if isinstance(body_node, Tree.WorkflowSection):
                    for subgather in body_node.gathers.values():
                        multiplex[subgather.workflow_node_id].add(
                            _append_scatter_indices(subgather.workflow_node_id, scatter_indices_i)
                        )

        # schedule each gather op with dependencies multiplexed onto the set of jobs scheduled
        # from the corresponding body node.
        # if the scatter array was empty or the condition was false, these dependencies are
        # empty, so these jobs will become runnable immediately to "gather" empty arrays or
        # Value.Null's as appropriate.
        for body_node_id, gather in section.gathers.items():
            self._schedule(
                gather,
                job_id=_append_scatter_indices(
                    gather.workflow_node_id, [p[0] for p in scatter_stack]
                ),
                dependencies=multiplex[body_node_id],
            )

    def _gather(self, gather: Tree.Gather, dependencies: Dict[str, Env.Values]) -> Env.Values:
        # important: the dependency job IDs must sort lexicographically in the desired array order!
        dep_ids = sorted(dependencies.keys())

        # determine if we're ultimately (through nested ops) gathering from a value or a call
        leaf = gather
        while isinstance(leaf, Tree.Gather):
            leaf = leaf.referee

        # figure out names of the values to gather
        if isinstance(leaf, Tree.Decl):
            names = [leaf.name]
        elif isinstance(leaf, Tree.Call):
            names = []
            outp = leaf.effective_outputs
            if outp:
                assert len(outp) == 1
                outp = outp[0]
                assert isinstance(outp, Env.Namespace)
                for b in outp.bindings:
                    assert isinstance(b, Env.Binding)
                    names.append(b.name)
        else:
            assert False

        # for each such name,
        ans = []
        ns = [leaf.name] if isinstance(leaf, Tree.Call) else []
        for name in names:
            # gather the corresponding values
            values = [Env.resolve(dependencies[dep_id], ns, name) for dep_id in dep_ids]
            v0 = values[0] if values else None
            assert v0 is None or isinstance(v0, Value.Base)
            # bind the array, singleton value, or None as appropriate
            if isinstance(gather.section, Tree.Scatter):
                rhs = Value.Array(Type.Array(v0.type if v0 else Type.Any()), values)
            else:
                assert isinstance(gather.section, Tree.Conditional)
                assert len(values) <= 1
                rhs = v0 if v0 is not None else Value.Null()
            ans = Env.bind(ans, ns, name, rhs)

        return ans


def run_local_workflow(
    workflow: Tree.Workflow,
    posix_inputs: Env.Values,
    run_id: Optional[str] = None,
    parent_dir: Optional[str] = None,
) -> Tuple[str, Env.Values]:
    """
    Run a workflow locally.

    Inputs shall have been typechecked already.

    File inputs are presumed to be local POSIX file paths that can be mounted into containers
    """
    # TODO:
    # - error handling
    # - logging
    # - concurrency

    state = StateMachine(workflow, posix_inputs)

    parent_dir = parent_dir or os.getcwd()

    if run_id:
        run_dir = os.path.join(parent_dir, run_id)
        os.makedirs(run_dir, exist_ok=False)
    else:
        now = datetime.today()
        run_id = now.strftime("%Y%m%d_%H%M%S") + "_" + workflow.name
        try:
            run_dir = os.path.join(parent_dir, run_id)
            os.makedirs(run_dir, exist_ok=False)
        except FileExistsError:
            run_id = now.strftime("%Y%m%d_%H%M%S_") + str(now.microsecond) + "_" + workflow.name
            run_dir = os.path.join(parent_dir, run_id)
            os.makedirs(run_dir, exist_ok=False)

    while True:
        next_call = state.step()
        if next_call:
            if isinstance(next_call.callee, Tree.Task):
                _, outputs = run_local_task(
                    next_call.callee, next_call.inputs, run_id=next_call.id, parent_dir=run_dir
                )
                state.call_finished(next_call.id, outputs)
            else:
                raise NotImplementedError()
        elif state.outputs:
            return (run_dir, state.outputs)


def _append_scatter_indices(id: str, scatter_indices: List[str]) -> str:
    return "-".join([id] + scatter_indices)
