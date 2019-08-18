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
(``WDL.Env.Bindings[WDL.Value.Base]``) are transmitted along each dependency edge, and WDL
expressions in each node are evaluated in the environment formed from the union of the node's
incoming dependency edges.

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

import logging
import os
import math
import itertools
import json
import traceback
import pickle
from typing import Optional, List, Set, Tuple, NamedTuple, Dict, Union, Iterable, Callable, Any
from .. import Env, Type, Value, Tree, StdLib
from ..Error import InputError
from .._util import write_values_json, provision_run_dir, LOGGING_FORMAT, install_coloredlogs
from .task import run_local_task
from .error import TaskFailure


class WorkflowOutputs(Tree.WorkflowNode):
    """
    A no-op workflow node which depends on each ``Decl`` node from the workflow output section. Or,
    if the workflow is missing the output section, depends on ``Call`` and ``Gather`` nodes for all
    call outputs.

    The workflow state machine tacks this on to the workflow graph to facilitate assembly of the
    outputs environment.
    """

    output_node_ids: Set[str]

    def __init__(self, workflow: Tree.Workflow) -> None:
        super().__init__("outputs", workflow.pos)

        self.output_node_ids = set()
        if workflow.outputs is not None:
            for node in workflow.outputs:
                self.output_node_ids.add(node.workflow_node_id)
        else:
            # no output{} section -- use all top-level Calls and any top-level Gather whose
            # ultimate referee is a Call
            for n in workflow.body:
                if isinstance(n, Tree.Call):
                    self.output_node_ids.add(n.workflow_node_id)
                if isinstance(n, Tree.WorkflowSection):
                    for g in n.gathers.values():
                        if isinstance(g.final_referee, Tree.Call):
                            self.output_node_ids.add(g.workflow_node_id)

    def _workflow_node_dependencies(self) -> Iterable[str]:
        yield from self.output_node_ids

    def add_to_type_env(
        self, struct_typedefs: Env.Bindings[Tree.StructTypeDef], type_env: Env.Bindings[Type.Base]
    ) -> Env.Bindings[Type.Base]:
        raise NotImplementedError()

    @property
    def children(self) -> Iterable[Tree.SourceNode]:
        return []


_Job = NamedTuple(
    "_Job",
    [
        ("id", str),
        ("node", Tree.WorkflowNode),
        ("dependencies", Set[str]),
        ("scatter_stack", List[Tuple[str, Env.Binding[Value.Base]]]),
    ],
)


class StateMachine:
    """
    On-line workflow state machine, suitable for use within a singleton driver process managing
    in-memory state. The state machine evaluates WDL expressions locally, while instructing the
    driver when to call tasks/subworkflows. It's agnostic to how/where the driver actually executes
    each call, just requiring asynchronous notification of call completion along with the outputs.
    """

    _logger: Optional[logging.Logger] = None
    run_id: str
    run_dir: str
    values_to_json: Callable[[Env.Bindings[Value.Base]], Dict]
    workflow: Tree.Workflow
    inputs: Env.Bindings[Value.Base]
    jobs: Dict[str, _Job]
    job_outputs: Dict[str, Env.Bindings[Value.Base]]
    finished: Set[str]
    running: Set[str]
    waiting: Set[str]
    filename_whitelist: Set[str]
    # TODO: factor out WorkflowState interface?

    def __init__(
        self, run_id: str, run_dir: str, workflow: Tree.Workflow, inputs: Env.Bindings[Value.Base]
    ) -> None:
        """
        Initialize the workflow state machine from the workflow AST and inputs
        """
        self.run_id = run_id
        self.run_dir = run_dir
        self.workflow = workflow
        self.inputs = inputs
        self.jobs = {}
        self.job_outputs = {}
        self.finished = set()
        self.running = set()
        self.waiting = set()
        self.filename_whitelist = _filenames(inputs)

        from .. import values_to_json

        self.values_to_json = values_to_json  # pyre-ignore

        workflow_nodes = [
            node for node in (workflow.inputs or []) + workflow.body + (workflow.outputs or [])
        ]
        workflow_nodes.append(WorkflowOutputs(workflow))

        # TODO: by topsorting all section bodies we can ensure that when we schedule an additional
        # job, all its dependencies will already have been scheduled, increasing
        # flexibility/compatibility with various backends.
        for node in workflow_nodes:
            deps = node.workflow_node_dependencies
            if isinstance(node, Tree.Decl):
                # strike the dependencies of any decl node whose value is supplied in the inputs
                if inputs.has_binding(node.name):
                    deps = set()
            self._schedule(
                _Job(id=node.workflow_node_id, node=node, dependencies=deps, scatter_stack=[])
            )

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
    def outputs(self) -> Optional[Env.Bindings[Value.Base]]:
        """
        Workflow outputs, once the workflow is completely finished. ``None`` until then.

        Warning: be sure to distinguish ``None``, the workflow isn't finished, from ``[]``, the
        workflow finished with no outputs.
        """
        if len(self.finished) < len(self.jobs):
            return None
        ans = self.job_outputs["outputs"]
        assert ans is not None
        return ans

    CallInstructions = NamedTuple(
        "CallInstructions",
        [
            ("id", str),
            ("callee", Union[Tree.Task, Tree.Workflow]),
            ("inputs", Env.Bindings[Value.Base]),
        ],
    )
    """
    The state machine produces a ``CallInstructions`` object when it's time for the driver to
    launch a task/subworkflow job.

    :param id: call/job ID string, unique in the workflow
    :param callee: ``WDL.Call`` or ``WDL.Workflow`` to launch
    :param inputs: ``WDL.Env.Bindings[Value.Base]`` of call inputs
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
        runnable = []
        while True:
            # select a job whose dependencies are all finished
            if not runnable:
                runnable = sorted(
                    [j for j in self.waiting if not (self.jobs[j].dependencies - self.finished)],
                    reverse=True,
                )
            if not runnable:
                if self.waiting and not self.running:
                    self.logger.critical(
                        "deadlocked: %s",
                        str(
                            set(itertools.chain(*(self.jobs[j].dependencies for j in self.waiting)))
                            - self.finished
                        ),
                    )
                    assert False
                return None
            job_id = runnable.pop()
            job = self.jobs[job_id]

            # mark it 'running'
            self.running.add(job.id)
            self.waiting.remove(job.id)

            # do the job
            try:
                res = self._do_job(job)
            except Exception as exn:
                setattr(exn, "job_id", job.id)
                raise exn

            # if it's a call, return instructions to the driver
            if isinstance(res, StateMachine.CallInstructions):
                return res

            # otherwise, record the outputs, mark the job finished, and move on to the next job
            envlog = json.dumps(self.values_to_json(res))
            self.logger.info("visit %s -> %s", job.id, envlog if len(envlog) < 4096 else "(large)")
            self.job_outputs[job.id] = res
            self.running.remove(job.id)
            self.finished.add(job.id)

    def call_finished(self, job_id: str, outputs: Env.Bindings[Value.Base]) -> None:
        """
        Deliver notice of a job's successful completion, along with its outputs
        """
        assert job_id in self.running
        outlog = json.dumps(self.values_to_json(outputs))
        self.logger.notice("finish %s", job_id)  # pyre-fixme
        self.logger.info("output %s -> %s", job_id, outlog if len(outlog) < 4096 else "(large)")
        call_node = self.jobs[job_id].node
        assert isinstance(call_node, Tree.Call)
        self.job_outputs[job_id] = outputs.wrap_namespace(call_node.name)
        self.filename_whitelist |= _filenames(outputs)
        self.finished.add(job_id)
        self.running.remove(job_id)

    def _schedule(self, job: _Job) -> None:
        self.logger.debug("schedule %s after {%s}", job.id, ", ".join(job.dependencies))
        assert job.id not in self.jobs
        self.jobs[job.id] = job
        self.waiting.add(job.id)

    def _do_job(
        self, job: _Job
    ) -> "Union[StateMachine.CallInstructions, Env.Bindings[Value.Base]]":
        if isinstance(job.node, Tree.Gather):
            return _gather(
                job.node, dict((dep_id, self.job_outputs[dep_id]) for dep_id in job.dependencies)
            )

        # for all non-Gather nodes, derive the environment by merging the outputs of all the
        # dependencies (+ any current scatter variable bindings)
        scatter_vars = Env.Bindings()
        for p in job.scatter_stack:
            scatter_vars = Env.Bindings(p[1], scatter_vars)
        # pyre-ignore
        env = Env.merge(scatter_vars, *(self.job_outputs[dep] for dep in job.dependencies))
        envlog = json.dumps(self.values_to_json(env))
        self.logger.debug("env %s <- %s", job.id, envlog if len(envlog) < 4096 else "(large)")

        stdlib = _StdLib(self)

        if isinstance(job.node, (Tree.Scatter, Tree.Conditional)):
            for newjob in _scatter(self.workflow, job.node, env, job.scatter_stack, stdlib):
                self._schedule(newjob)
            # the section node itself has no outputs, so return an empty env
            return Env.Bindings()

        if isinstance(job.node, Tree.Decl):
            # bind the value obtained either (i) from the workflow inputs or (ii) by evaluating
            # the expr
            v = None
            try:
                v = self.inputs.resolve(job.node.name)
            except KeyError:
                pass
            if v is None:
                if job.node.expr:
                    v = job.node.expr.eval(env, stdlib=stdlib).coerce(job.node.type)
                else:
                    assert job.node.type.optional
                    v = Value.Null()
            return Env.Bindings(Env.Binding(job.node.name, v))

        if isinstance(job.node, WorkflowOutputs):
            return env

        if isinstance(job.node, Tree.Call):
            # evaluate input expressions
            call_inputs = Env.Bindings()
            for name, expr in job.node.inputs.items():
                call_inputs = call_inputs.bind(name, expr.eval(env, stdlib=stdlib))
            # check workflow inputs for additional inputs supplied to this call
            for b in self.inputs.enter_namespace(job.node.name):
                call_inputs = call_inputs.bind(b.name, b.value)
            # coerce inputs to required types
            assert isinstance(job.node.callee, (Tree.Task, Tree.Workflow))
            callee_inputs = job.node.callee.available_inputs
            call_inputs = call_inputs.map(
                lambda b: Env.Binding(b.name, b.value.coerce(callee_inputs[b.name].type))
            )
            # check input files against whitelist
            disallowed_filenames = _filenames(call_inputs) - self.filename_whitelist
            if disallowed_filenames:
                raise InputError(
                    f"call {job.node.name} inputs use unknown file: {next(iter(disallowed_filenames))}"
                )
            # issue CallInstructions
            self.logger.notice("issue %s on %s", job.id, job.node.callee.name)  # pyre-fixme
            inplog = json.dumps(self.values_to_json(call_inputs))
            self.logger.info("input %s <- %s", job.id, inplog if len(inplog) < 4096 else "(large)")

            return StateMachine.CallInstructions(
                id=job.id, callee=job.node.callee, inputs=call_inputs
            )

        raise NotImplementedError()

    @property
    def logger(self) -> logging.Logger:
        if not self._logger:
            self._logger = logging.getLogger("wdl-worfklow:" + self.run_id)
            if self.run_dir:
                fh = logging.FileHandler(os.path.join(self.run_dir, "workflow.log"))
                fh.setFormatter(logging.Formatter(LOGGING_FORMAT))
                self._logger.addHandler(fh)
            install_coloredlogs(self._logger)
        return self._logger

    def __getstate__(self) -> Dict[str, Any]:
        ans = dict(self.__dict__)
        del ans["_logger"]  # for Python pre-3.7 loggers: https://bugs.python.org/issue30520
        return ans


def _scatter(
    workflow: Tree.Workflow,
    section: Union[Tree.Scatter, Tree.Conditional],
    env: Env.Bindings[Value.Base],
    scatter_stack: List[Tuple[str, Env.Binding[Value.Base]]],
    stdlib: StdLib.Base,
) -> Iterable[_Job]:
    # we'll be tracking, for each body node ID, the IDs of the potentially multiple corresponding
    # jobs scheduled
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

    # for each array element, schedule an instance of the body subgraph
    last_scatter_indices = None
    for i, array_i in enumerate(array):

        # scatter bookkeeping: format the index as a left-zero-padded string so that it'll sort
        # lexicographically in the desired order; bind the scatter variable name to the array value
        scatter_stack_i = scatter_stack
        if isinstance(array_i, Value.Base):
            assert isinstance(section, Tree.Scatter)
            scatter_stack_i = scatter_stack_i + [
                (str(i).zfill(digits), Env.Binding(section.variable, array_i))
            ]
        scatter_indices_i = [p[0] for p in scatter_stack_i]
        assert last_scatter_indices is None or last_scatter_indices < scatter_indices_i
        last_scatter_indices = scatter_indices_i

        # schedule each body (template) node
        for body_node in section.body:
            # the job ID will be the template node ID with the current scatter index appended.
            # if we're in nested scatters, then append *each* respective index!
            assert len(scatter_indices_i) == body_node.scatter_depth
            body_job_id = _append_scatter_indices(body_node.workflow_node_id, scatter_indices_i)

            # furthermore, rewrite the template node's dependencies on other within-scatter nodes
            # to the corresponding jobs given the current scatter index.
            # especially tricky: in a nested scatter we can depend on a node at a higher level, for
            # which we need to append only the indices up to its level!
            dependencies = set()
            for dep_id in body_node.workflow_node_dependencies:
                dep = workflow.get_node(dep_id)
                assert dep.scatter_depth <= body_node.scatter_depth
                dependencies.add(
                    _append_scatter_indices(dep_id, scatter_indices_i[: dep.scatter_depth])
                )

            yield _Job(
                id=body_job_id,
                node=body_node,
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
        yield _Job(
            id=_append_scatter_indices(gather.workflow_node_id, [p[0] for p in scatter_stack]),
            node=gather,
            dependencies=multiplex[body_node_id],
            scatter_stack=scatter_stack,
        )


def _append_scatter_indices(node_id: str, scatter_indices: List[str]) -> str:
    return "-".join([node_id] + scatter_indices)


def _gather(
    gather: Tree.Gather, dependencies: Dict[str, Env.Bindings[Value.Base]]
) -> Env.Bindings[Value.Base]:
    # important: the dependency job IDs must sort lexicographically in the desired array order!
    dep_ids = sorted(dependencies.keys())

    # since it would be so awful to permute the array silently, lets verify the ID order
    if isinstance(gather.section, Tree.Scatter):
        dep_id_prefix = None
        dep_id_values = []
        for dep_id in dep_ids:
            dep_id_fields = dep_id.split("-")
            if dep_id_prefix is not None:
                assert dep_id_fields[:-1] == dep_id_prefix
            else:
                dep_id_prefix = dep_id_fields[:-1]
            dep_id_values.append(int(dep_id_fields[-1]))
        assert dep_id_values == list(range(len(dep_ids)))

    # figure out names of the values to gather, either the name if the referenced decl,
    # or each output of the referenced call.
    leaf = gather.final_referee  # follow potential linked list of Gathers for nested sections
    if isinstance(leaf, Tree.Decl):
        names = [leaf.name]
    elif isinstance(leaf, Tree.Call):
        names = []
        outp = leaf.effective_outputs.enter_namespace(leaf.name)
        assert len(outp) == len(leaf.effective_outputs)
        for b in outp:
            names.append(b.name)
    else:
        assert False

    # for each such name,
    ans = Env.Bindings()
    ns = [leaf.name] if isinstance(leaf, Tree.Call) else []
    for name in names:
        # gather the corresponding values
        values = [dependencies[dep_id].resolve(".".join(ns + [name])) for dep_id in dep_ids]
        v0 = values[0] if values else None
        assert v0 is None or isinstance(v0, Value.Base)
        # bind the array, singleton value, or None as appropriate
        if isinstance(gather.section, Tree.Scatter):
            rhs = Value.Array((v0.type if v0 else Type.Any()), values)
        else:
            assert isinstance(gather.section, Tree.Conditional)
            assert len(values) <= 1
            rhs = v0 if v0 is not None else Value.Null()
        ans = ans.bind(".".join(ns + [name]), rhs)

    return ans


class _StdLib(StdLib.Base):
    "checks against & updates the filename whitelist for the read_* and write_* functions"
    state: StateMachine

    def __init__(self, state: StateMachine) -> None:
        super().__init__(write_dir=os.path.join(state.run_dir, "write_"))
        self.state = state

    def _devirtualize_filename(self, filename: str) -> str:
        if filename in self.state.filename_whitelist:
            return filename
        raise InputError("attempted read from unknown or inaccessible file " + filename)

    def _virtualize_filename(self, filename: str) -> str:
        self.state.filename_whitelist.add(filename)
        return filename


def _filenames(env: Env.Bindings[Value.Base]) -> Set[str]:
    "Get the host filenames of all File values in the environment"
    ans = set()

    def collector(v: Value.Base) -> None:
        if isinstance(v, Value.File):
            ans.add(v.value)
        for ch in v.children:
            collector(ch)

    for b in env:
        collector(b.value)
    return ans


def run_local_workflow(
    workflow: Tree.Workflow,
    posix_inputs: Env.Bindings[Value.Base],
    run_id: Optional[str] = None,
    run_dir: Optional[str] = None,
    _test_pickle: bool = False,
) -> Tuple[str, Env.Bindings[Value.Base]]:
    """
    Run a workflow locally.

    Inputs shall have been typechecked already. File inputs are presumed to be local POSIX file
    paths that can be mounted into a container.

    :param run_id: unique ID for the run, defaults to workflow name
    :param run_dir: outputs and scratch will be stored in this directory if it doesn't already
                    exist; if it does, a timestamp-based subdirectory is created and used (defaults
                    to current working directory)
    """

    run_id = run_id or workflow.name
    run_dir = provision_run_dir(workflow.name, run_dir)
    logger = logging.getLogger("wdl-workflow:" + run_id)
    fh = logging.FileHandler(os.path.join(run_dir, "workflow.log"))
    fh.setFormatter(logging.Formatter(LOGGING_FORMAT))
    logger.addHandler(fh)
    install_coloredlogs(logger)
    logger.notice(  # pyre-fixme
        "starting workflow %s (%s Ln %d Col %d) in %s",
        workflow.name,
        workflow.pos.uri,
        workflow.pos.line,
        workflow.pos.column,
        run_dir,
    )
    write_values_json(posix_inputs, os.path.join(run_dir, "inputs.json"), namespace=workflow.name)

    state = StateMachine(run_id, run_dir, workflow, posix_inputs)

    try:
        while state.outputs is None:
            if _test_pickle:
                state = pickle.loads(pickle.dumps(state))

            next_call = state.step()
            if next_call:
                if isinstance(next_call.callee, Tree.Task):
                    run_callee = run_local_task
                elif isinstance(next_call.callee, Tree.Workflow):
                    run_callee = run_local_workflow
                else:
                    assert False
                _, outputs = run_callee(
                    next_call.callee,  # pyre-fixme
                    next_call.inputs,
                    run_id=next_call.id,
                    run_dir=os.path.join(run_dir, next_call.id),
                )
                state.call_finished(next_call.id, outputs)
    except Exception as exn:
        logger.debug(traceback.format_exc())
        if isinstance(exn, TaskFailure):
            logger.error("%s failed", getattr(exn, "run_id"))
        else:
            msg = ""
            if hasattr(exn, "job_id"):
                msg += getattr(exn, "job_id") + " "
            msg += exn.__class__.__name__
            if str(exn):
                msg += ", " + str(exn)
            logger.error(msg)
            logger.info("run directory: %s", run_dir)
        raise

    assert state.outputs is not None
    write_values_json(state.outputs, os.path.join(run_dir, "outputs.json"), namespace=workflow.name)
    logger.notice("done")  # pyre-fixme
    return (run_dir, state.outputs)
