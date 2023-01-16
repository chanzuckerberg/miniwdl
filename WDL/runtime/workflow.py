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
import multiprocessing
import os
import math
import itertools
import json
import signal
import traceback
import pickle
import threading
import regex
from concurrent import futures
from typing import Optional, List, Set, Tuple, NamedTuple, Dict, Union, Iterable, Callable, Any
from contextlib import ExitStack
from .. import Env, Type, Value, Tree, StdLib, Error
from .task import run_local_task, _fspaths, link_outputs, _add_downloadable_defaults
from .download import able as downloadable, run_cached as download
from .._util import (
    write_atomic,
    write_values_json,
    provision_run_dir,
    TerminationSignalFlag,
    LoggingFileHandler,
    compose_coroutines,
    pathsize,
    path_really_within,
)
from .._util import StructuredLogMessage as _
from . import config, _statusbar
from .cache import CallCache, new as new_call_cache
from .error import RunFailed, Terminated, error_json


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
        self, struct_types: Env.Bindings[Dict[str, Type.Base]], type_env: Env.Bindings[Type.Base]
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
    logger_id: str
    run_dir: str
    values_to_json: Callable[[Env.Bindings[Value.Base]], Dict]
    workflow: Tree.Workflow
    inputs: Env.Bindings[Value.Base]
    jobs: Dict[str, _Job]
    job_outputs: Dict[str, Env.Bindings[Value.Base]]
    finished: Set[str]
    running: Set[str]
    waiting: Set[str]

    # File/Directory paths that were either expressly supplied as workflow inputs, or were
    # generated by this workflow execution. By default, these are the only local paths the workflow
    # is allowed to access. (Unless [file_io] allow_any_input = true)
    fspath_allowlist: Set[str]
    # TODO: factor out WorkflowState interface?

    def __init__(
        self,
        logger_id: str,
        run_dir: str,
        workflow: Tree.Workflow,
        inputs: Env.Bindings[Value.Base],
    ) -> None:
        """
        Initialize the workflow state machine from the workflow AST and inputs
        """
        self.logger_id = logger_id
        self.run_dir = run_dir
        self.workflow = workflow
        self.inputs = inputs
        self.jobs = {}
        self.job_outputs = {}
        self.finished = set()
        self.running = set()
        self.waiting = set()
        self.fspath_allowlist = _fspaths(inputs)

        from .. import values_to_json

        self.values_to_json = values_to_json  # pyre-ignore

        # Preprocess inputs: if None value is supplied for an input declared with a default but
        # without the ? type quantifier, remove the binding entirely so that the default will be
        # used. In contrast, if the input declaration has an -explicitly- optional type, then we'll
        # allow the supplied None to override any default.
        input_decls = workflow.available_inputs
        self.inputs = self.inputs.filter(
            lambda b: not (
                isinstance(b.value, Value.Null)
                and b.name in input_decls
                and input_decls[b.name].expr
                and not input_decls[b.name].type.optional
            )
        )

        workflow_nodes = (workflow.inputs or []) + workflow.body + (workflow.outputs or [])
        workflow_nodes.append(WorkflowOutputs(workflow))

        for node in workflow_nodes:
            deps = node.workflow_node_dependencies
            if isinstance(node, Tree.Decl):
                # strike the dependencies of any decl node whose value is supplied in the inputs
                if self.inputs.has_binding(node.name):
                    deps = set()
            self._schedule(
                _Job(id=node.workflow_node_id, node=node, dependencies=deps, scatter_stack=[])
            )

        # TODO: by topsorting all section bodies we could ensure that when we schedule an
        # additional job, all its dependencies will already have been scheduled, increasing
        # flexibility/compatibility with various backends.

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

    def step(
        self, cfg: config.Loader, stdlib: StdLib.Base
    ) -> "Optional[StateMachine.CallInstructions]":
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
                assert self.running or not self.waiting, "deadlocked: " + str(
                    set(itertools.chain(*(self.jobs[j].dependencies for j in self.waiting)))
                    - self.finished
                )
                return None
            job_id = runnable.pop()
            job = self.jobs[job_id]

            # mark it 'running'
            self.running.add(job.id)
            self.waiting.remove(job.id)

            # do the job
            try:
                res = self._do_job(cfg, stdlib, job)
            except Exception as exn:
                setattr(exn, "job_id", job.id)
                raise exn

            # if it's a call, return instructions to the driver
            if isinstance(res, StateMachine.CallInstructions):
                return res

            # otherwise, record the outputs, mark the job finished, and move on to the next job
            envlog = self.values_to_json(res)
            self.logger.info(
                _(
                    "visit",
                    node=job.id,
                    values=envlog if len(json.dumps(envlog)) < 4096 else "(((large)))",
                )
            )
            self.job_outputs[job.id] = res
            self.running.remove(job.id)
            self.finished.add(job.id)

    def call_finished(self, job_id: str, outputs: Env.Bindings[Value.Base]) -> None:
        """
        Deliver notice of a job's successful completion, along with its outputs
        """
        assert job_id in self.running
        outlog = self.values_to_json(outputs)
        self.logger.notice(_("finish", job=job_id))  # pyre-fixme
        self.logger.info(
            _(
                "output",
                job=job_id,
                values=outlog if len(json.dumps(outlog)) < 4096 else "(((large)))",
            )
        )
        call_node = self.jobs[job_id].node
        assert isinstance(call_node, Tree.Call)
        self.job_outputs[job_id] = outputs.wrap_namespace(call_node.name)
        self.fspath_allowlist |= _fspaths(outputs)
        self.finished.add(job_id)
        self.running.remove(job_id)

    def _schedule(self, job: _Job) -> None:
        self.logger.debug(_("schedule", node=job.id, dependencies=list(job.dependencies)))
        assert job.id not in self.jobs
        self.jobs[job.id] = job
        self.waiting.add(job.id)

    def _do_job(
        self, cfg: config.Loader, stdlib: StdLib.Base, job: _Job
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
        envlog = self.values_to_json(env)
        self.logger.debug(
            _(
                "env",
                node=job.id,
                values=envlog if len(json.dumps(envlog)) < 4096 else "(((large)))",
            )
        )

        if isinstance(job.node, (Tree.Scatter, Tree.Conditional)):
            for newjob in _scatter(
                self.workflow,
                job.node,
                env,
                job.scatter_stack,
                stdlib,
                cfg.get_int("scheduler", "scatter_tag_max"),
            ):
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
            return Value.rewrite_env_paths(
                env, lambda v: _check_path_allowed(cfg, self.fspath_allowlist, "workflow output", v)
            )

        if isinstance(job.node, Tree.Call):
            # evaluate input expressions
            call_inputs = Env.Bindings()
            for name, expr in job.node.inputs.items():
                call_inputs = call_inputs.bind(name, expr.eval(env, stdlib=stdlib))
            # check workflow inputs for additional inputs supplied to this call
            for b in self.inputs.enter_namespace(job.node.name):
                call_inputs = call_inputs.bind(b.name, b.value)

            # coerce inputs to required types (treating inputs with defaults as optional even if
            # they don't have the ? type quantifier)
            assert isinstance(job.node.callee, (Tree.Task, Tree.Workflow))
            callee_inputs = job.node.callee.available_inputs
            call_inputs = call_inputs.map(
                lambda b: Env.Binding(
                    b.name,
                    (
                        b.value.coerce(
                            (
                                callee_inputs[b.name].type.copy(optional=True)
                                if callee_inputs[b.name].expr
                                else callee_inputs[b.name].type
                            )
                        )
                        if b.name in callee_inputs
                        else b.value
                    ),
                )
            )
            call_inputs = Value.rewrite_env_paths(
                call_inputs,
                lambda v: _check_path_allowed(
                    cfg, self.fspath_allowlist, f"call {job.node.name} input", v
                ),
            )
            # issue CallInstructions
            self.logger.notice(_("ready", job=job.id, callee=job.node.callee.name))  # pyre-fixme
            inplog = self.values_to_json(call_inputs)
            self.logger.info(
                _(
                    "input",
                    job=job.id,
                    values=inplog if len(json.dumps(inplog)) < 4096 else "(((large)))",
                )
            )

            return StateMachine.CallInstructions(
                id=job.id, callee=job.node.callee, inputs=call_inputs
            )

        raise NotImplementedError()

    @property
    def logger(self) -> logging.Logger:
        if not self._logger:
            self._logger = logging.getLogger(self.logger_id)
            # TODO: if we were truly unpickling in a new process, we'd need to add a new
            # LoggingFileHandler to self._logger
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
    max_tag: int,
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
    digits = math.ceil(math.log10(len(array))) if len(array) > 1 else 1

    # for each array element, schedule an instance of the body subgraph
    last_scatter_indices = None
    scatter_tags = _scatter_tags(array, max_tag)
    for i, array_i in enumerate(array):

        # scatter bookkeeping: format the index as a left-zero-padded string so that it'll sort
        # lexicographically in the desired order; bind the scatter variable name to the array value
        scatter_stack_i = scatter_stack
        if isinstance(array_i, Value.Base):
            assert isinstance(section, Tree.Scatter)
            str_i = str(i).zfill(digits)
            assert len(str_i) <= digits
            if scatter_tags[i]:
                str_i += "-" + scatter_tags[i]
            scatter_stack_i = scatter_stack_i + [(str_i, Env.Binding(section.variable, array_i))]
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


def _scatter_tags(array: List[Optional[Value.Base]], max_tag: int) -> List[str]:
    # Given an array of values, compute an array of names for each item that strives for useful
    # human-readability, and can be embedded in the run id/directory safely. This is to help the
    # operator navigate the run logs & directory tree, looking for specific items.

    # stringify each item and split each string into a list of identifier-like components
    any = False
    delimiters = regex.compile("[^0-9a-zA-Z_]+")
    items = []
    for i, array_i in enumerate(array):
        if (
            isinstance(array_i, Value.Base)
            and not isinstance(array_i, Value.Null)
            and not (isinstance(array_i, Value.Int) and array_i.value == i)
        ):
            items.append(delimiters.split(json.dumps(array_i.json)))
            any = True
        else:
            items.append([])
    if not any or max_tag <= 0:
        return [""] * len(array)

    # compute & remove those lists' longest common prefix & suffix
    lcp = _longest_common_prefix(items)
    if lcp:
        items = [item[lcp:] for item in items]
    lcs = _longest_common_prefix([list(reversed(item)) for item in items])
    if lcs:
        items = [item[:-lcs] for item in items]

    # concatenate remaining components
    tags = ["".join(item) for item in items]

    # truncate to first max_tag characters
    tags_pfx = [tag[:max_tag].rstrip("-") for tag in tags]
    if sum(1 for tag in tags_pfx if len(tag)) == sum(1 for tag in set(tags_pfx) if len(tag)):
        return tags_pfx
    # if those weren't unique, then try suffix; if those aren't unique either, then give up to
    # avoid generating misleading tags.
    tags_sfx = [tag[-max_tag:].lstrip("-") for tag in tags]
    return (
        tags_sfx
        if sum(1 for tag in tags_sfx if len(tag)) == sum(1 for tag in set(tags_sfx) if len(tag))
        else ([""] * len(items))
    )


def _longest_common_prefix(items: List[List[str]]) -> int:
    ans = 0
    for i, item in enumerate(items):
        if i == 0:
            ans = len(item)
        else:
            ans = min(ans, len(item))
            prev_item = items[i - 1]
            j = 0
            while j < ans and item[j] == prev_item[j]:
                j += 1
            ans = j
        if ans == 0:
            return 0
    return ans


def _gather(
    gather: Tree.Gather, dependencies: Dict[str, Env.Bindings[Value.Base]]
) -> Env.Bindings[Value.Base]:
    # important: the dependency job IDs must sort lexicographically in the desired array order!
    dep_ids = sorted(dependencies.keys())

    # since it would be so awful to permute the array silently, lets verify the ID order
    if len(dep_ids) > 1:
        assert isinstance(gather.section, Tree.Scatter)
        dep_ids_split = [dep_id.split("-") for dep_id in dep_ids]
        dep_id_lcp = _longest_common_prefix(dep_ids_split)
        for i, dep_id_i in enumerate(dep_ids_split):
            assert i == int(dep_id_i[dep_id_lcp])

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
    "checks against & updates the file/directory allowlist for the read_* and write_* functions"
    cfg: config.Loader
    state: StateMachine
    cache: CallCache

    def __init__(
        self, wdl_version: str, cfg: config.Loader, state: StateMachine, cache: CallCache
    ) -> None:
        super().__init__(wdl_version, write_dir=os.path.join(state.run_dir, "write_"))
        self.cfg = cfg
        self.state = state
        self.cache = cache

    def _devirtualize_filename(self, filename: str) -> str:
        if downloadable(self.cfg, filename):
            cached = self.cache.get_download(filename)
            if cached:
                return cached
        return _check_path_allowed(
            self.cfg, self.state.fspath_allowlist, "read_*() argument", Value.File(filename)
        )

    def _virtualize_filename(self, filename: str) -> str:
        self.state.fspath_allowlist.add(filename)
        return filename


def _check_path_allowed(
    cfg: config.Loader, allowlist: Set[str], desc: str, v: Union[Value.File, Value.Directory]
) -> str:
    isdir = isinstance(v, Value.Directory)
    fspath = v.value.rstrip("/") + ("/" if isdir else "")
    if fspath in allowlist or downloadable(cfg, fspath, directory=isdir):
        return v.value
    if not cfg.get_bool("file_io", "allow_any_input"):
        raise Error.InputError(
            desc + " uses file/directory not expressly supplied with workflow inputs"
            " (to allow, set [file_io] allow_any_input = true): " + fspath
        )
    # allow_any_input: checks that normally happen in CLI.validate_input_path
    if not (os.path.isdir(fspath) if isdir else os.path.isfile(fspath)):
        raise Error.InputError(f"{desc} uses nonexistent file/directory: {fspath}")
    fspath = os.path.abspath(fspath).rstrip("/")
    if not path_really_within(fspath, cfg["file_io"]["root"]):
        raise Error.InputError(
            f"{desc} {v.value} must reside within [file_io] root " + cfg["file_io"]["root"]
        )
    return fspath


class _ThreadPools:
    # Singleton managing the thread pools for concurrent task and subworkflow execution
    #
    # All tasks run on one thread pool.
    #
    # Each subworkflow call runs on a thread pool reserved for its nested call depth level.
    # (If we kept just one subworkflow thread pool, then nested calls could deadlock when no thread
    # is available to run a subworkflow whilst the caller blocks its own thread.)
    _lock: threading.Lock
    _cleanup: ExitStack
    _task_pool: futures.ThreadPoolExecutor
    _subworkflow_pools: List[futures.ThreadPoolExecutor]
    _subworkflow_concurrency: int

    def __init__(self, cfg: config.Loader, cleanup: ExitStack) -> None:
        self._lock = threading.Lock()
        self._cleanup = cleanup.enter_context(ExitStack())

        task_concurrency = (
            cfg["scheduler"].get_int("task_concurrency")
            or (
                cfg.get_int("scheduler", "call_concurrency")  # pre-v1.5.4 legacy
                if cfg.has_option("scheduler", "call_concurrency")
                else 0
            )
            or multiprocessing.cpu_count()
        )

        self._task_pool = futures.ThreadPoolExecutor(max_workers=task_concurrency)
        self._cleanup.callback(futures.ThreadPoolExecutor.shutdown, self._task_pool)

        self._subworkflow_concurrency = cfg.get_int("scheduler", "subworkflow_concurrency") or max(
            task_concurrency, multiprocessing.cpu_count()
        )
        self._subworkflow_pools = []

    def submit_task(self, *args, **kwargs):  # pyre-ignore
        with self._lock:
            return self._task_pool.submit(*args, **kwargs)

    def submit_subworkflow(self, call_depth: int, *args, **kwargs):  # pyre-ignore
        with self._lock:
            if call_depth >= len(self._subworkflow_pools):
                # First time at this call depth -- initialize a thread pool for it
                assert call_depth == len(self._subworkflow_pools)
                pool = futures.ThreadPoolExecutor(self._subworkflow_concurrency)
                self._cleanup.callback(futures.ThreadPoolExecutor.shutdown, pool)
                self._subworkflow_pools.append(pool)
            return self._subworkflow_pools[call_depth].submit(*args, **kwargs)


def run_local_workflow(
    cfg: config.Loader,
    workflow: Tree.Workflow,
    inputs: Env.Bindings[Value.Base],
    run_id: Optional[str] = None,
    run_dir: Optional[str] = None,
    logger_prefix: Optional[List[str]] = None,
    _thread_pools: Optional[_ThreadPools] = None,
    _cache: Optional[CallCache] = None,
    _test_pickle: bool = False,
    _run_id_stack: Optional[List[str]] = None,
) -> Tuple[str, Env.Bindings[Value.Base]]:
    """
    Run a workflow locally.

    Inputs shall have been typechecked already. File inputs are presumed to be local POSIX file
    paths that can be mounted into a container.

    :param run_id: unique ID for the run, defaults to workflow name
    :param run_dir: directory under which to create a timestamp-named subdirectory for this run
                    (defaults to current working directory).
                    If the final path component is ".", then operate in run_dir directly.
    """

    # provision run directory and log file
    run_id = run_id or workflow.name
    _run_id_stack = _run_id_stack or []
    run_dir = provision_run_dir(workflow.name, run_dir, last_link=not _run_id_stack)

    logger_prefix = logger_prefix or ["wdl"]
    logger_id = logger_prefix + ["w:" + run_id]
    logger = logging.getLogger(".".join(logger_id))
    logfile = os.path.join(run_dir, "workflow.log")
    with ExitStack() as cleanup:
        cleanup.enter_context(
            LoggingFileHandler(
                logger,
                logfile,
            )
        )
        if cfg.has_option("logging", "json") and cfg["logging"].get_bool("json"):
            cleanup.enter_context(
                LoggingFileHandler(
                    logger,
                    logfile + ".json",
                    json=True,
                )
            )
        logger.notice(  # pyre-fixme
            _(
                "workflow start",
                name=workflow.name,
                source=workflow.pos.uri,
                line=workflow.pos.line,
                column=workflow.pos.column,
                dir=run_dir,
            )
        )
        logger.debug(_("thread", ident=threading.get_ident()))
        terminating = cleanup.enter_context(TerminationSignalFlag(logger))
        cache = _cache
        if not cache:
            cache = cleanup.enter_context(new_call_cache(cfg, logger))
            assert _thread_pools is None
        if not _thread_pools:
            cache.flock(logfile, exclusive=True)  # flock top-level workflow.log
        write_values_json(inputs, os.path.join(run_dir, "inputs.json"), namespace=workflow.name)

        # query call cache
        cache_key = f"{workflow.name}/{workflow.digest}/{Value.digest_env(inputs)}"
        cached = cache.get(cache_key, inputs, workflow.effective_outputs)
        if cached is not None:
            for outp in workflow.effective_outputs:
                v = cached[outp.name]
                vj = json.dumps(v.json)
                logger.info(
                    _(
                        "cached output",
                        name=outp.name,
                        value=(v.json if len(vj) < 4096 else "(((large)))"),
                    )
                )
            _outputs = link_outputs(
                cache,
                cached,
                run_dir,
                hardlinks=cfg["file_io"].get_bool("output_hardlinks"),
                use_relative_output_paths=cfg["file_io"].get_bool("use_relative_output_paths"),
            )
            write_values_json(
                cached, os.path.join(run_dir, "outputs.json"), namespace=workflow.name
            )
            logger.notice("done (cached)")  # pyre-fixme
            # returning `cached`, not the rewritten `_outputs`, to retain opportunity to find
            # cached downstream inputs
            return (run_dir, cached)

        # if we're the top-level workflow, provision thread pools
        if not _thread_pools:
            # delayed heavy imports -- load .task_container now to work around python issue41567
            import importlib_metadata
            from .task_container import new as _new_task_container

            assert not _run_id_stack
            try:
                # log version into workflow.log
                version = "v" + importlib_metadata.version("miniwdl")
            except importlib_metadata.PackageNotFoundError:
                version = "UNKNOWN"
            logger.notice(_("miniwdl", version=version, uname=" ".join(os.uname())))  # pyre-fixme

            thread_pools = _ThreadPools(cfg, cleanup)
        else:
            assert _run_id_stack and _cache
            thread_pools = _thread_pools

        try:
            # run workflow state machine
            outputs = _workflow_main_loop(
                cfg,
                workflow,
                inputs,
                _run_id_stack + [run_id],
                run_dir,
                logger,
                logger_id,
                thread_pools,
                cache,
                terminating,
                _test_pickle,
            )
        except:
            _statusbar.abort()
            if not _run_id_stack and cfg["scheduler"].get_bool("fail_fast"):
                # if we're the top-level worfklow, signal abort to anything still running
                # concurrently on the thread pools (SIGUSR1 will be picked up by
                # TerminationSignalFlag)
                os.kill(os.getpid(), signal.SIGUSR1)
            raise

        cache.put(cache_key, outputs)

    return (run_dir, outputs)


def _workflow_main_loop(
    cfg: config.Loader,
    workflow: Tree.Workflow,
    inputs: Env.Bindings[Value.Base],
    run_id_stack: List[str],
    run_dir: str,
    logger: logging.Logger,
    logger_id: List[str],
    thread_pools: _ThreadPools,
    cache: CallCache,
    terminating: Callable[[], bool],
    _test_pickle: bool,
) -> Env.Bindings[Value.Base]:
    assert isinstance(cfg, config.Loader)
    call_futures = {}
    try:
        # start plugin coroutines and process inputs through them
        with compose_coroutines(
            [
                (
                    lambda kwargs, cor=cor: cor(
                        cfg, logger, run_id_stack, run_dir, workflow, **kwargs
                    )
                )
                for cor in [cor2 for _, cor2 in sorted(config.load_plugins(cfg, "workflow"))]
            ],
            {"inputs": inputs},
        ) as plugins:
            recv = next(plugins)
            inputs = recv["inputs"]

            # download input files, if needed
            _download_input_files(
                cfg,
                logger,
                logger_id,
                run_dir,
                _add_downloadable_defaults(cfg, workflow.available_inputs, inputs),
                thread_pools,
                cache,
            )

            # run workflow state machine to completion
            state = StateMachine(".".join(logger_id), run_dir, workflow, inputs)
            while state.outputs is None:
                if _test_pickle:
                    state = pickle.loads(pickle.dumps(state))
                if terminating():
                    raise Terminated()
                # schedule all runnable calls
                stdlib = _StdLib(workflow.effective_wdl_version, cfg, state, cache)
                next_call = state.step(cfg, stdlib)
                while next_call:
                    call_dir = os.path.join(run_dir, next_call.id)
                    if os.path.exists(call_dir):
                        logger.warning(
                            _("call subdirectory already exists, conflict likely", dir=call_dir)
                        )
                    sub_args = (cfg, next_call.callee, next_call.inputs)
                    sub_kwargs = {
                        "run_id": next_call.id,
                        "run_dir": os.path.join(call_dir, "."),
                        "logger_prefix": logger_id,
                        "_cache": cache,
                        "_run_id_stack": run_id_stack,
                    }
                    # submit to appropriate thread pool
                    if isinstance(next_call.callee, Tree.Task):
                        _statusbar.task_backlogged()
                        future = thread_pools.submit_task(run_local_task, *sub_args, **sub_kwargs)
                    elif isinstance(next_call.callee, Tree.Workflow):
                        future = thread_pools.submit_subworkflow(
                            len(run_id_stack) - 1,
                            run_local_workflow,
                            *sub_args,
                            **sub_kwargs,
                            _thread_pools=thread_pools,
                        )
                    else:
                        assert False
                    call_futures[future] = next_call.id
                    next_call = state.step(cfg, stdlib)
                # no more calls to launch right now; wait for an outstanding call to finish
                future = next(futures.as_completed(call_futures), None)
                if future:
                    __, outputs = future.result()
                    call_id = call_futures[future]
                    state.call_finished(call_id, outputs)
                    call_futures.pop(future)
                else:
                    assert state.outputs is not None

            # create output_links
            outputs = link_outputs(
                cache,
                state.outputs,
                run_dir,
                hardlinks=cfg["file_io"].get_bool("output_hardlinks"),
                # Relative output paths only make sense at the top level, and hence is only used here.
                use_relative_output_paths=cfg["file_io"].get_bool("use_relative_output_paths"),
            )

            # process outputs through plugins
            recv = plugins.send({"outputs": outputs})
            outputs = recv["outputs"]

            # write outputs.json
            write_values_json(
                outputs, os.path.join(run_dir, "outputs.json"), namespace=workflow.name
            )
            logger.notice("done")
            return outputs
    except Exception as exn:
        tbtxt = traceback.format_exc()
        logger.debug(tbtxt)
        cause = exn
        while isinstance(cause, RunFailed) and cause.__cause__:
            cause = cause.__cause__
        wrapper = RunFailed(workflow, run_id_stack[-1], run_dir)
        try:
            write_atomic(
                json.dumps(
                    error_json(
                        wrapper,
                        cause=exn,
                        traceback=tbtxt if not isinstance(exn, Error.RuntimeError) else None,
                    ),
                    indent=2,
                ),
                os.path.join(run_dir, "error.json"),
            )
        except Exception as exn2:
            logger.debug(traceback.format_exc())
            logger.critical(_("failed to write error.json", dir=run_dir, message=str(exn2)))
        if not isinstance(exn, RunFailed):
            logger.error(
                _(
                    str(wrapper),
                    dir=run_dir,
                    **error_json(
                        exn, traceback=tbtxt if not isinstance(exn, Error.RuntimeError) else None
                    ),
                )
            )
        elif not isinstance(exn.__cause__, Terminated):
            logger.error(
                _("call failure propagating", **{"from": getattr(exn, "run_id"), "dir": run_dir})
            )
        # Cancel all future tasks that havent started
        for key in call_futures:
            key.cancel()
        raise wrapper from exn


def _download_input_files(
    cfg: config.Loader,
    logger: logging.Logger,
    logger_prefix: List[str],
    run_dir: str,
    inputs: Env.Bindings[Value.Base],
    thread_pools: _ThreadPools,
    cache: CallCache,
) -> None:
    """
    Find all File & Directory input values that are downloadable URIs (including any nested within
    compound values), and ensure the cache is "primed" with them, performing any needed download
    tasks on thread_pool. The inputs are not modified, but the CallCache will be ready to quickly
    produce a local filename corresponding to any URI therein, because it's either stored in the
    persistent download cache (if enabled), or downloaded to the current/parent run directory and
    transiently memoized.
    """

    # scan inputs for URIs
    uris = set()

    def scan_uri(v: Union[Value.File, Value.Directory]) -> str:
        nonlocal uris
        directory = isinstance(v, Value.Directory)
        uri = v.value
        if uri not in uris and downloadable(cfg, uri, directory=directory):
            uris.add((uri, directory))
        return uri

    Value.rewrite_env_paths(inputs, scan_uri)
    if not uris:
        return
    logger.notice(_("downloading input URIs", count=len(uris)))  # pyre-fixme

    # download them on the thread pool (but possibly further limiting concurrency)
    download_concurrency = cfg.get_int("scheduler", "download_concurrency")
    if download_concurrency <= 0:
        download_concurrency = 999999
    ops = {}
    incomplete = len(uris)
    outstanding = set()
    downloaded_bytes = 0
    cached_hits = 0
    exn = None

    while incomplete and not exn:
        assert len(outstanding) <= incomplete

        # top up thread pool's queue (up to download_concurrency)
        while uris and len(outstanding) < download_concurrency:
            (uri, directory) = uris.pop()
            logger.info(
                _(f"schedule input {'directory' if directory else 'file'} download", uri=uri)
            )
            future = thread_pools.submit_task(
                download,
                cfg,
                logger,
                cache,
                uri,
                directory=directory,
                run_dir=os.path.join(run_dir, "download", str(len(ops)), "."),
                logger_prefix=logger_prefix + [f"download{len(ops)}"],
            )
            ops[future] = uri
            outstanding.add(future)
        assert outstanding

        # wait for one or more oustanding downloads to finish
        just_finished, still_outstanding = futures.wait(
            outstanding, return_when=futures.FIRST_COMPLETED
        )
        outstanding = still_outstanding
        for future in just_finished:
            # check results
            try:
                future_exn = future.exception()
            except futures.CancelledError:
                future_exn = Terminated()
            if not future_exn:
                uri = ops[future]
                cached, filename = future.result()
                if cached:
                    cached_hits += 1
                else:
                    sz = pathsize(filename)
                    logger.info(_("downloaded input", uri=uri, path=filename, bytes=sz))
                    downloaded_bytes += sz
            elif not exn:
                # cancel pending ops and signal running ones to abort
                for outsfut in outstanding:
                    outsfut.cancel()
                os.kill(os.getpid(), signal.SIGUSR1)
                exn = future_exn
            incomplete -= 1

    if exn:
        raise exn
    logger.notice(  # pyre-fixme
        _(
            "processed input URIs",
            cached=cached_hits,
            downloaded=len(ops) - cached_hits,
            downloaded_bytes=downloaded_bytes,
        )
    )
