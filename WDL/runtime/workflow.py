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
import json
import signal
import traceback
import pickle
import threading
from concurrent import futures
from typing import Optional, List, Callable, Tuple, Dict, NamedTuple, Set, Union
from contextlib import ExitStack
from .. import Env, Value, Tree, Error
from .task import run_local_task
from ._io_helpers import (
    _add_downloadable_defaults,
    _warn_output_basename_collisions,
    link_outputs,
)
from .download import able as downloadable, run_cached as download
from ._stdlib import WorkflowStdLib
from ._workflow_state import StateMachine
from .._util import (
    write_atomic,
    write_values_json,
    provision_run_dir,
    TerminationSignalFlag,
    LoggingFileHandler,
    compose_coroutines,
    pathsize,
)
from .._util import StructuredLogMessage as _
from . import config, _statusbar
from .cache import CallCache, CallCacheAddPaths, call_cache_key, new as new_call_cache
from .error import RunFailed, Terminated, error_json


class WorkflowMainLoopResult(NamedTuple):
    outputs: Env.Bindings[Value.Base]
    add_paths: CallCacheAddPaths


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
    _logger: logging.Logger

    def __init__(self, cfg: config.Loader, cleanup: ExitStack, logger: logging.Logger) -> None:
        self._logger = logger
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
        self._logger.info(_("task thread pool initialized", task_concurrency=task_concurrency))

        self._subworkflow_concurrency = cfg.get_int("scheduler", "subworkflow_concurrency") or max(
            task_concurrency, multiprocessing.cpu_count()
        )
        self._subworkflow_pools = []

    def submit_task(self, *args, **kwargs):
        with self._lock:
            return self._task_pool.submit(*args, **kwargs)

    def submit_subworkflow(self, call_depth: int, *args, **kwargs):
        with self._lock:
            if call_depth >= len(self._subworkflow_pools):
                # First time at this call depth -- initialize a thread pool for it
                assert call_depth == len(self._subworkflow_pools)
                pool = futures.ThreadPoolExecutor(self._subworkflow_concurrency)
                self._cleanup.callback(futures.ThreadPoolExecutor.shutdown, pool)
                self._subworkflow_pools.append(pool)
                self._logger.info(
                    _(
                        "subworkflow thread pool initialized",
                        subworkflow_concurrency=self._subworkflow_concurrency,
                        call_depth=call_depth,
                    )
                )
            return self._subworkflow_pools[call_depth].submit(*args, **kwargs)


def _download_workflow_input_files(
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
    compound values), and ensure the cache is "primed" with them. Workflow inputs are not modified:
    future task input localization will resolve each URI from the cache.
    """

    # scan inputs for URIs
    uris: Set[Tuple[str, bool]] = set()

    def scan_uri(v: Union[Value.File, Value.Directory]) -> str:
        nonlocal uris
        directory = isinstance(v, Value.Directory)
        uri = v.value
        if (uri, directory) not in uris and downloadable(cfg, uri, directory=directory):
            uris.add((uri, directory))
        return uri

    Value.rewrite_env_paths(inputs, scan_uri)
    if not uris:
        return
    logger.notice(_("downloading input URIs", count=len(uris)))

    # download them on the thread pool (but possibly further limiting concurrency)
    download_concurrency = cfg.get_int("scheduler", "download_concurrency")
    if download_concurrency <= 0:
        download_concurrency = 999999
    ops: Dict[futures.Future[Tuple[bool, str]], str] = {}
    incomplete = len(uris)
    outstanding: Set[futures.Future[Tuple[bool, str]]] = set()
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
    logger.notice(
        _(
            "processed input URIs",
            cached=cached_hits,
            downloaded=len(ops) - cached_hits,
            downloaded_bytes=downloaded_bytes,
        )
    )


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
        logger.notice(
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
            assert cache and _thread_pools is None
        if not _thread_pools:
            cache.flock(logfile, exclusive=True)  # flock top-level workflow.log
        write_values_json(inputs, os.path.join(run_dir, "inputs.json"), namespace=workflow.name)

        # query call cache
        cache_inputs = inputs
        cache_key = call_cache_key(workflow.name, workflow.digest, cache_inputs)
        cached = cache.get(cache_key, cache_inputs, workflow.effective_outputs)
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
            logger.notice("done (cached)")
            # returning `cached`, not the rewritten `_outputs`, to retain opportunity to find
            # cached downstream inputs
            return (run_dir, cached)

        # if we're the top-level workflow, provision thread pools
        if not _thread_pools:
            # delayed heavy imports -- load .task_container now to work around python issue41567
            import importlib_metadata
            from .task_container import new as _new_task_container  # noqa: F401

            assert not _run_id_stack
            try:
                # log version into workflow.log
                version = "v" + importlib_metadata.version("miniwdl")
            except importlib_metadata.PackageNotFoundError:
                version = "UNKNOWN"
            logger.notice(_("miniwdl", version=version, uname=" ".join(os.uname())))

            thread_pools = _ThreadPools(cfg, cleanup, logger)
        else:
            assert _run_id_stack and _cache
            thread_pools = _thread_pools

        try:
            # run workflow state machine
            main_loop_result = _workflow_main_loop(
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
            outputs = main_loop_result.outputs
            add_paths = main_loop_result.add_paths
        except:
            _statusbar.abort()
            if not _run_id_stack and cfg["scheduler"].get_bool("fail_fast"):
                # if we're the top-level worfklow, signal abort to anything still running
                # concurrently on the thread pools (SIGUSR1 will be picked up by
                # TerminationSignalFlag)
                os.kill(os.getpid(), signal.SIGUSR1)
            raise

        cache.put(
            cache_key,
            outputs,
            run_dir=run_dir,
            inputs=cache_inputs,
            add_paths=add_paths,
        )

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
) -> WorkflowMainLoopResult:
    assert isinstance(cfg, config.Loader)
    call_futures = {}
    try:
        # start plugin coroutines and process inputs through them
        with compose_coroutines(
            [
                (
                    lambda kwargs, cor=cor: cor(  # type: ignore
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
            _download_workflow_input_files(
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
                stdlib = WorkflowStdLib(cfg, workflow.effective_wdl_version, state, cache)
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
                    child_key = call_cache_key(
                        next_call.callee.name, next_call.callee.digest, next_call.inputs
                    )
                    call_futures[future] = (next_call.id, child_key)
                    next_call = state.step(cfg, stdlib)
                # no more calls to launch right now; wait for an outstanding call to finish
                future = next(futures.as_completed(call_futures), None)
                if future:
                    __, outputs = future.result()
                    call_id, child_key = call_futures[future]
                    # Fold child task/subworkflow manifests into the parent workflow manifest.
                    # This makes a workflow cache hit sensitive to source-relative paths used
                    # inside its calls, even when the workflow itself doesn't read those files.
                    state.add_paths.update(cache.get_add_paths(child_key))
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
            _warn_output_basename_collisions(logger, outputs)

            # write outputs.json
            write_values_json(
                outputs, os.path.join(run_dir, "outputs.json"), namespace=workflow.name
            )
            logger.notice("done")
            return WorkflowMainLoopResult(outputs, state.add_paths)
    except Exception as exn:
        tbtxt = traceback.format_exc()
        logger.debug(tbtxt)
        cause: BaseException = exn
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
