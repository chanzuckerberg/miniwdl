"""
Local task runner
"""

import logging
import os
import json
import traceback
import threading
import regex
from typing import (
    Tuple,
    List,
    Dict,
    Optional,
    Callable,
    Set,
    Any,
    Union,
    TYPE_CHECKING,
    Generator,
    Iterable,
)
from contextlib import ExitStack, suppress
from collections import Counter

from .. import Error, Type, Env, Value, StdLib, Tree, Expr, _util
from .._util import (
    WDLVersion,
    write_atomic,
    write_values_json,
    provision_run_dir,
    TerminationSignalFlag,
    chmod_R_plus,
    LoggingFileHandler,
    compose_coroutines,
    path_really_within,
    pathsize,
    rmtree_atomic,
    wdl_version_geq,
)
from .._util import StructuredLogMessage as _
from . import config, _statusbar
from .cache import CallCache, CallCacheAddPaths, call_cache_key, new as new_call_cache
from ._io_helpers import (
    _add_downloadable_defaults,
    _warn_struct_extra,
    _warn_output_basename_collisions,
    link_outputs,
)
from ._io_helpers import (
    _fspaths,
    _resolve_source_relative_path,
    _source_relative_cache_add_path,
)
from .download import able as downloadable, run_cached as download
from ._stdlib import TaskInputStdLib, TaskOutputStdLib
from .error import OutputError, Interrupted, Terminated, RunFailed, error_json

if TYPE_CHECKING:  # otherwise-delayed heavy imports
    from .task_container import TaskContainer

TaskPluginCoroutine = Generator[Dict[str, Any], Dict[str, Any], None]


def run_local_task(  # type: ignore[return]
    cfg: config.Loader,
    task: Tree.Task,
    inputs: Env.Bindings[Value.Base],
    run_id: Optional[str] = None,
    run_dir: Optional[str] = None,
    logger_prefix: Optional[List[str]] = None,
    _run_id_stack: Optional[List[str]] = None,
    _cache: Optional[CallCache] = None,
    _plugins: Optional[List[Callable[..., Any]]] = None,
) -> Tuple[str, Env.Bindings[Value.Base]]:
    """
    Run a task locally.

    Inputs shall have been typechecked already. File inputs are presumed to be local POSIX file
    paths that can be mounted into a container.

    :param run_id: unique ID for the run, defaults to workflow name
    :param run_dir: directory under which to create a timestamp-named subdirectory for this run
                    (defaults to current working directory).
                    If the final path component is ".", then operate in run_dir directly.
    """
    from .task_container import new as new_task_container  # delay heavy import

    _run_id_stack = _run_id_stack or []
    run_id = run_id or task.name
    logger_prefix = (logger_prefix or ["wdl"]) + ["t:" + run_id]
    logger = logging.getLogger(".".join(logger_prefix))
    with ExitStack() as cleanup:
        terminating = cleanup.enter_context(TerminationSignalFlag(logger))
        if terminating():
            raise Terminated(quiet=True)

        # provision run directory and log file
        run_dir = provision_run_dir(task.name, run_dir, last_link=not _run_id_stack)
        logfile = os.path.join(run_dir, "task.log")
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
                "task setup",
                name=task.name,
                source=task.pos.uri,
                line=task.pos.line,
                column=task.pos.column,
                dir=run_dir,
                thread=threading.get_ident(),
            )
        )
        write_values_json(inputs, os.path.join(run_dir, "inputs.json"))

        if not _run_id_stack:
            cache = _cache or cleanup.enter_context(new_call_cache(cfg, logger))
            cache.flock(logfile, exclusive=True)  # no containing workflow; flock task.log
        else:
            cache = _cache
        assert cache

        cleanup.enter_context(_statusbar.task_slotted())
        maybe_container = None
        try:
            cache_inputs = inputs
            cache_key = call_cache_key(task.name, task.digest, cache_inputs)
            cached = cache.get(cache_key, cache_inputs, task.effective_outputs)
            if cached is not None:
                for decl in task.outputs:
                    v = cached[decl.name]
                    vj = json.dumps(v.json)
                    logger.info(
                        _(
                            "cached output",
                            name=decl.name,
                            value=(v.json if len(vj) < 4096 else "(((large)))"),
                        )
                    )
                # create out/ and outputs.json
                _outputs = link_outputs(
                    cache,
                    cached,
                    run_dir,
                    hardlinks=cfg["file_io"].get_bool("output_hardlinks"),
                    use_relative_output_paths=cfg["file_io"].get_bool("use_relative_output_paths"),
                )
                write_values_json(
                    cached, os.path.join(run_dir, "outputs.json"), namespace=task.name
                )
                logger.notice("done (cached)")
                # returning `cached`, not the rewritten `_outputs`, to retain opportunity to find
                # cached downstream inputs
                return (run_dir, cached)
            # start plugin coroutines and process inputs through them
            with compose_coroutines(
                [
                    (
                        lambda kwargs, cor=cor: cor(  # type: ignore
                            cfg, logger, _run_id_stack + [run_id], run_dir, task, **kwargs
                        )
                    )
                    for cor in (
                        [cor2 for _, cor2 in sorted(config.load_plugins(cfg, "task"))]
                        + (_plugins or [])
                    )
                ],
                {"inputs": inputs},
            ) as plugins:
                recv = next(plugins)
                inputs = recv["inputs"]

                # download input files, if needed
                posix_inputs = _download_task_input_files(
                    cfg,
                    logger,
                    logger_prefix,
                    run_dir,
                    _add_downloadable_defaults(cfg, task.available_inputs, inputs),
                    cache,
                )

                # create TaskContainer according to configuration
                container = new_task_container(cfg, logger, run_id, run_dir)
                maybe_container = container
                # Manifest source-relative paths observed while evaluating task declarations,
                # runtime/requirements expressions, and command placeholders. This is separate
                # from TaskContainer's localized input map; it is persisted to the call cache.
                cache_add_paths = CallCacheAddPaths()

                # evaluate input/postinput declarations, including mapping from host to
                # in-container file paths
                container_env = _eval_task_inputs(
                    logger, task, posix_inputs, container, cache_add_paths
                )

                # evaluate runtime fields
                stdlib = TaskInputStdLib(
                    task.effective_wdl_version,
                    logger,
                    container,
                    source_dir=task.source_dir,
                    cache_add_paths=cache_add_paths,
                )
                _eval_task_runtime(
                    cfg, logger, run_id, task, posix_inputs, container, container_env, stdlib
                )
                if wdl_version_geq(task.effective_wdl_version, WDLVersion.V1_2):
                    container.build_task_runtime_info_struct(logger, run_id, task)
                    assert container.task_runtime_info_struct is not None
                    container_env = container_env.bind("task", container.task_runtime_info_struct)

                # start container & run command (and retry if needed)
                container = _try_task(
                    cfg,
                    task,
                    logger,
                    plugins,
                    container,
                    container_env,
                    terminating,
                    cache_add_paths,
                )

                # bind output declarations to task runtime info with the final return code
                if wdl_version_geq(task.effective_wdl_version, WDLVersion.V1_2):
                    assert container.try_counter >= 1
                    container.update_task_runtime_info_struct(
                        attempt=Value.Int(container.try_counter - 1),
                        return_code=(
                            Value.Int(container.last_exit_code)
                            if container.last_exit_code is not None
                            else Value.Null()
                        ),
                    )
                    assert container.task_runtime_info_struct is not None
                    container_env = container_env.bind("task", container.task_runtime_info_struct)

                # evaluate output declarations
                outputs = _eval_task_outputs(logger, run_id, task, container_env, container)

                # create output_links
                outputs = link_outputs(
                    cache,
                    outputs,
                    run_dir,
                    hardlinks=cfg["file_io"].get_bool("output_hardlinks"),
                    use_relative_output_paths=cfg["file_io"].get_bool("use_relative_output_paths"),
                )

                # process outputs through plugins
                recv = plugins.send({"outputs": outputs})
                outputs = recv["outputs"]

                # clean up, if so configured, and make sure output files will be accessible to
                # downstream tasks
                _delete_work(cfg, logger, container, True)
                chmod_R_plus(run_dir, file_bits=0o660, dir_bits=0o770)
                _warn_output_basename_collisions(logger, outputs)

                # write outputs.json
                write_values_json(
                    outputs, os.path.join(run_dir, "outputs.json"), namespace=task.name
                )
                logger.notice("done")
                if not run_id.startswith("download-"):
                    cache.put(
                        cache_key,
                        outputs,
                        run_dir=run_dir,
                        inputs=cache_inputs,
                        add_paths=cache_add_paths,
                    )
                return (run_dir, outputs)
        except Exception as exn:
            tbtxt = traceback.format_exc()
            logger.debug(tbtxt)
            wrapper = RunFailed(task, run_id, run_dir)
            logmsg = _(
                str(wrapper),
                dir=run_dir,
                **error_json(
                    exn, traceback=tbtxt if not isinstance(exn, Error.RuntimeError) else None
                ),
            )
            if isinstance(exn, Terminated) and getattr(exn, "quiet", False):
                logger.debug(logmsg)
            else:
                logger.error(logmsg)
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
            try:
                if maybe_container:
                    _delete_work(cfg, logger, maybe_container, False)
            except Exception as exn2:
                logger.debug(traceback.format_exc())
                logger.error(_("delete_work also failed", exception=str(exn2)))
            raise wrapper from exn


def _eval_task_inputs(
    logger: logging.Logger,
    task: Tree.Task,
    posix_inputs: Env.Bindings[Value.Base],
    container: "TaskContainer",
    cache_add_paths: CallCacheAddPaths,
) -> Env.Bindings[Value.Base]:
    # Preprocess inputs: if None value is supplied for an input declared with a default but without
    # the ? type quantifier, remove the binding entirely so that the default will be used. In
    # contrast, if the input declaration has an -explicitly- optional type, then we'll allow the
    # supplied None to override any default.
    input_decls = task.available_inputs
    posix_inputs = posix_inputs.filter(
        lambda b: (
            not (
                isinstance(b.value, Value.Null)
                and b.name in input_decls
                and input_decls[b.name].expr
                and not input_decls[b.name].type.optional
            )
        )
    )

    # Map all the provided input File & Directory paths to in-container paths
    container.add_paths(_fspaths(posix_inputs))
    _warn_input_basename_collisions(logger, container)

    # copy posix_inputs with all File & Directory values mapped to their in-container paths
    def map_paths(fn: Union[Value.File, Value.Directory]) -> str:
        p = fn.value.rstrip("/")
        if isinstance(fn, Value.Directory):
            p += "/"
        return container.input_path_map[p]

    container_inputs = Value.rewrite_env_paths(posix_inputs, map_paths)

    # initialize value environment with the inputs
    container_env: Env.Bindings[Value.Base] = Env.Bindings()
    for b in container_inputs:
        assert isinstance(b, Env.Binding)
        v = b.value
        assert isinstance(v, Value.Base)
        container_env = container_env.bind(b.name, v)
        vj = json.dumps(v.json)
        logger.info(_("input", name=b.name, value=(v.json if len(vj) < 4096 else "(((large)))")))

    # collect remaining declarations requiring evaluation.
    decls_to_eval = _task_decl_eval_order(
        decl
        for decl in (task.inputs or []) + task.postinputs
        if not container_env.has_binding(decl.name)
    )

    # evaluate each declaration in that order
    # note: the write_* functions call container.add_paths as a side-effect
    stdlib = TaskInputStdLib(
        task.effective_wdl_version,
        logger,
        container,
        source_dir=task.source_dir,
        cache_add_paths=cache_add_paths,
    )
    for decl in decls_to_eval:
        assert isinstance(decl, Tree.Decl)
        v = _eval_task_decl(
            logger,
            decl,
            container_env,
            stdlib,
            lambda value: _postprocess_task_decl_paths(
                decl,
                value,
                lambda w: _resolve_task_decl_path_into_container(
                    task, decl.name, w, container, cache_add_paths
                ),
                lambda name: Error.InputError(
                    f"File/Directory path not found in task declaration {name}"
                ),
            ),
        )
        vj = json.dumps(v.json)
        logger.info(_("eval", name=decl.name, value=(v.json if len(vj) < 4096 else "(((large)))")))
        container_env = container_env.bind(decl.name, v)

    return container_env


def _download_task_input_files(
    cfg: config.Loader,
    logger: logging.Logger,
    logger_prefix: List[str],
    run_dir: str,
    inputs: Env.Bindings[Value.Base],
    cache: CallCache,
) -> Env.Bindings[Value.Base]:
    """
    Find all File & Directory input values that are downloadable URIs (including any nested within
    compound values). Download them to some location under run_dir and return a copy of the inputs
    with the URI values replaced by the downloaded paths.
    """

    downloads = 0
    download_bytes = 0
    cached_hits = 0

    def rewriter(v: Union[Value.Directory, Value.File]) -> str:
        nonlocal downloads, download_bytes, cached_hits
        directory = isinstance(v, Value.Directory)
        uri = v.value
        if downloadable(cfg, uri, directory=directory):
            logger.info(_(f"download input {'directory' if directory else 'file'}", uri=uri))
            cached, filename = download(
                cfg,
                logger,
                cache,
                uri,
                directory=directory,
                run_dir=os.path.join(run_dir, "download", str(downloads), "."),
                logger_prefix=logger_prefix + [f"download{downloads}"],
            )
            if cached:
                cached_hits += 1
            else:
                sz = pathsize(filename)
                logger.info(_("downloaded input", uri=uri, path=filename, bytes=sz))
                downloads += 1
                download_bytes += sz
            return filename
        return uri

    ans = Value.rewrite_env_paths(inputs, rewriter)
    if downloads or cached_hits:
        logger.notice(
            _(
                "processed input URIs",
                downloaded=downloads,
                downloaded_bytes=download_bytes,
                cached=cached_hits,
            )
        )
    return ans


def _task_decl_eval_order(decls: Iterable[Tree.Decl]) -> List[Tree.Decl]:
    """
    Topologically sort task declarations for evaluation.
    """
    decls_by_id, decls_adj = Tree._decl_dependency_matrix(list(decls))
    ans = [decls_by_id[did] for did in _util.topsort(decls_adj)]
    # NOTE: _util.topsort() throws on cycles, but those should have been rejected in static
    # typechecking prior to this.
    assert len(decls_by_id) == len(ans)
    return ans


def _eval_task_decl(
    logger: logging.Logger,
    decl: Tree.Decl,
    env: Env.Bindings[Value.Base],
    stdlib: StdLib.Base,
    postprocess_paths: Callable[[Value.Base], Value.Base],
) -> Value.Base:
    """
    Evaluate one task declaration and apply File/Directory path rewriting logic (which differs
    between input/private and output declarations).
    """
    try:
        value = decl.expr.eval(env, stdlib=stdlib).coerce(decl.type) if decl.expr else Value.Null()
        _warn_struct_extra(logger, decl.name, value)
        return postprocess_paths(value)
    except Error.RuntimeError as exn:
        setattr(exn, "job_id", decl.workflow_node_id)
        raise exn
    except Exception as exn:
        exn2 = Error.EvalError(decl, str(exn))
        setattr(exn2, "job_id", decl.workflow_node_id)
        raise exn2 from exn


def _postprocess_task_decl_paths(
    decl: Tree.Decl,
    value: Value.Base,
    missing_path: Callable[[Union[Value.File, Value.Directory]], Optional[str]],
    missing_error: Callable[[str], Error.RuntimeError],
) -> Value.Base:
    """
    Replace non-existent File/Directory paths with None (Value.Null), then coerce to the
    declaration type (which may raise if the declaration is non-optional).
    """
    value = Value.rewrite_paths(value, missing_path)
    try:
        return value.coerce(decl.type)
    except FileNotFoundError:  # from Value.Null.coerce(File|Directory)
        err = missing_error(decl.name)
        setattr(err, "job_id", decl.workflow_node_id)
        raise err


def _resolve_task_decl_path_into_container(
    task: Tree.Task,
    decl_name: str,
    v: Union[Value.File, Value.Directory],
    container: "TaskContainer",
    cache_add_paths: CallCacheAddPaths,
) -> Optional[str]:
    """
    Resolve a task input/private declaration File/Directory path into the task container.

    Paths built from already-localized input Directories are checked against their host backing
    directories. Other relative paths are WDL 1.2 source-relative paths: resolve them under the WDL
    source directory, record them in ``cache_add_paths``, mount them into the task container, and
    return the container path.
    """
    ans = _task_decl_input_directory_child_path(decl_name, v, container)
    if ans is None or ans != v.value:
        return ans

    if not wdl_version_geq(task.effective_wdl_version, WDLVersion.V1_2):
        return v.value

    source_paths: Set[str] = set()
    ans = _resolve_source_relative_path(
        container.cfg, task.source_dir, f"task declaration {decl_name}", v
    )
    if ans is None:
        cache_add_paths.add(_source_relative_cache_add_path(task.source_dir, v), absent=True)
        return None
    if ans == v.value:
        return ans

    source_paths.add(ans + ("/" if isinstance(v, Value.Directory) else ""))
    assert len(source_paths) == 1
    source_path = next(iter(source_paths))
    # A declaration default/private value can affect task behavior even though it isn't an explicit
    # input. Remember it in the call-cache manifest so mtime changes invalidate hits.
    cache_add_paths.add(source_path)
    container.add_paths(source_paths)
    return container.input_path_map[source_path]


def _task_decl_input_directory_child_path(
    decl_name: str, v: Union[Value.File, Value.Directory], container: "TaskContainer"
) -> Optional[str]:
    """
    Check a task File/Directory path formulated by path logic from another input Directory.

    input {
        Directory d
    }
    File f = join_paths(d, "file.txt")
    Directory? maybe = join_paths(d, "maybe/")

    Existing children must match the declared File/Directory kind. Directory children are returned
    with the trailing slash expected by TaskContainer. Missing children return None so optional
    declarations can become Null.
    """
    isdir = isinstance(v, Value.Directory)
    container_path = v.value.rstrip("/") + ("/" if isdir else "")
    found_input, host_path = container._input_host_path(container_path)
    if not found_input:
        return v.value
    assert host_path is not None
    if not os.path.exists(host_path.rstrip("/")):
        return None  # induces to Value.Null()
    if os.path.isdir(host_path) if isdir else os.path.isfile(host_path):
        return container_path if isdir else v.value
    raise Error.InputError(
        f"task declaration {decl_name} uses file/directory with the wrong type: " + v.value
    )


def _warn_input_basename_collisions(logger: logging.Logger, container: "TaskContainer") -> None:
    basenames = Counter(
        [os.path.basename((p[:-1] if p.endswith("/") else p)) for p in container.input_path_map_rev]
    )
    collisions = [nm for nm, n in basenames.items() if n > 1]
    if collisions:
        logger.warning(
            _(
                "mounting input files with colliding basenames in separate container directories",
                basenames=collisions,
            )
        )


def _eval_task_runtime(
    cfg: config.Loader,
    logger: logging.Logger,
    run_id: str,
    task: Tree.Task,
    inputs: Env.Bindings[Value.Base],
    container: "TaskContainer",
    env: Env.Bindings[Value.Base],
    stdlib: StdLib.Base,
) -> None:
    # evaluate runtime{} expressions (merged with any configured defaults)
    runtime_defaults = cfg.get_dict("task_runtime", "defaults")
    if run_id.startswith("download-"):
        runtime_defaults.update(cfg.get_dict("task_runtime", "download_defaults"))
    runtime_values = {}
    for key, v in runtime_defaults.items():
        runtime_values[key] = Value.from_json(Type.Any(), v)
    for key, expr in task.runtime.items():  # evaluate expressions in source code
        runtime_values[key] = expr.eval(env, stdlib)
    for b in inputs.enter_namespace("runtime"):
        runtime_values[b.name] = b.value  # input overrides
    for b in inputs.enter_namespace("requirements"):
        runtime_values[b.name] = b.value
    if "return_codes" in runtime_values and wdl_version_geq(
        task.effective_wdl_version, WDLVersion.V1_2
    ):
        runtime_values["returnCodes"] = runtime_values.pop("return_codes")
    logger.debug(_("runtime values", **dict((key, str(v)) for key, v in runtime_values.items())))

    # have container implementation validate & postprocess into container.runtime_values
    container.process_runtime(logger, runtime_values)

    if container.runtime_values:
        logger.info(_("effective runtime", **container.runtime_values))

    # add any configured overrides for in-container environment variables
    container.runtime_values.setdefault("env", {})
    env_vars_override = {}
    env_vars_skipped = []
    for ev_name, ev_value in cfg["task_runtime"].get_dict("env").items():
        if ev_value is None:
            try:
                env_vars_override[ev_name] = os.environ[ev_name]
            except KeyError:
                env_vars_skipped.append(ev_name)
        else:
            env_vars_override[ev_name] = str(ev_value)
    if env_vars_skipped:
        logger.warning(
            _("skipping pass-through of undefined environment variable(s)", names=env_vars_skipped)
        )
    if cfg.get_bool("file_io", "mount_tmpdir") or task.name in cfg.get_list(
        "file_io", "mount_tmpdir_for"
    ):
        env_vars_override["TMPDIR"] = os.path.join(
            container.container_dir, "work", "_miniwdl_tmpdir"
        )
    if env_vars_override:
        # usually don't dump values into log, as they may often be auth tokens
        logger.notice(
            _(
                "overriding environment variables (portability warning)",
                names=list(env_vars_override.keys()),
            )
        )
        logger.debug(
            _("overriding environment variables (portability warning)", **env_vars_override)
        )
        container.runtime_values["env"].update(env_vars_override)

    # process decls with "env" decorator
    env_decls: Dict[str, Value.Base] = {}
    for decl in (task.inputs or []) + task.postinputs:
        if decl.decor.get("env", False) is True:
            v = env[decl.name]
            if isinstance(v, (Value.String, Value.File, Value.Directory)):
                v = v.value
            else:
                v = json.dumps(v.json)
            env_decls[decl.name] = v
    container.runtime_values["env"].update(env_decls)

    unused_keys = list(
        key
        for key in runtime_values
        if key not in ("memory", "docker", "container") and key not in container.runtime_values
    )
    if unused_keys:
        logger.warning(_("ignored runtime settings", keys=unused_keys))


def _try_task(
    cfg: config.Loader,
    task: Tree.Task,
    logger: logging.Logger,
    plugins: TaskPluginCoroutine,
    container: "TaskContainer",
    container_env: Env.Bindings[Value.Base],
    terminating: Callable[[], bool],
    cache_add_paths: CallCacheAddPaths,
) -> "TaskContainer":
    """
    Run the task command in the container, retrying up to runtime.preemptible occurrences of
    Interrupted errors, plus up to runtime.maxRetries occurrences of any error.
    """
    from docker.errors import BuildError as DockerBuildError  # delay heavy import

    max_retries = container.runtime_values.get("maxRetries", 0)
    max_interruptions = container.runtime_values.get("preemptible", 0)
    retries = 0
    interruptions = 0

    command = None
    plugin_changed_command = False
    assert isinstance(task.command, Expr.TaskCommand)
    command_uses_task_attempt = _task_command_uses_task_attempt(task.command)

    while True:
        if terminating():
            raise Terminated()

        if command is None or command_uses_task_attempt:
            command = _eval_task_command(
                cfg,
                task,
                logger,
                container,
                container_env,
                attempt=container.try_counter - 1,
                cache_add_paths=cache_add_paths,
            )
            if container.try_counter == 1:
                assert retries == 0 and interruptions == 0 and not plugin_changed_command
                # let plugin(s) process command & container
                recv = plugins.send({"command": command, "container": container})
                plugin_command, container = (recv[k] for k in ("command", "container"))
                if plugin_command != command:
                    plugin_changed_command = True
                    command = plugin_command
        assert isinstance(command, str)
        logger.debug(_("command", command=command.strip()))

        if cfg.get_bool("file_io", "copy_input_files") or task.name in cfg.get_list(
            "file_io", "copy_input_files_for"
        ):
            # must follow command interpolation, which can add new input files via write_*
            container.copy_input_files(logger)
        host_tmpdir = (
            os.path.join(container.host_work_dir(), "_miniwdl_tmpdir")
            if cfg.get_bool("file_io", "mount_tmpdir")
            or task.name in cfg.get_list("file_io", "mount_tmpdir_for")
            else None
        )

        try:
            # start container & run command
            if host_tmpdir:
                logger.debug(_("creating task temp directory", TMPDIR=host_tmpdir))
                os.mkdir(host_tmpdir, mode=0o770)
            try:
                container.run(logger, command)
                return container
            finally:
                if host_tmpdir:
                    logger.info(_("deleting task temp directory", TMPDIR=host_tmpdir))
                    rmtree_atomic(host_tmpdir)
                if (
                    "preemptible" in container.runtime_values
                    and cfg.has_option("task_runtime", "_mock_interruptions")
                    and interruptions < cfg["task_runtime"].get_int("_mock_interruptions")
                ):
                    raise Interrupted("mock interruption") from None
        except Exception as exn:
            if isinstance(exn, Interrupted) and interruptions < max_interruptions:
                logger.error(
                    _(
                        "interrupted task will be retried",
                        error=exn.__class__.__name__,
                        message=str(exn),
                        prev_interruptions=interruptions,
                        max_interruptions=max_interruptions,
                    )
                )
                interruptions += 1
            elif (
                not isinstance(exn, (Terminated, DockerBuildError))
                and retries < max_retries
                and not terminating()
            ):
                logger.error(
                    _(
                        "failed task will be retried",
                        error=exn.__class__.__name__,
                        message=str(exn),
                        prev_retries=retries,
                        max_retries=max_retries,
                    )
                )
                retries += 1
            else:
                raise
            if command_uses_task_attempt and plugin_changed_command:
                # Our plugin API, designed well before the addition of `task.attempt` in WDL 1.2,
                # doesn't allow for reprocessing the command after a retry; to be safe, we fail if
                # the command uses `task.attempt` and the plugin changed the (first-try) command.
                raise Error.RuntimeError(
                    "task command uses task.attempt, but a task plugin changed the command; "
                    "cannot retry with an updated task.attempt value"
                ) from exn
            _delete_work(cfg, logger, container, False)
            container.reset(logger)


def _eval_task_command(
    cfg: config.Loader,
    task: Tree.Task,
    logger: logging.Logger,
    container: "TaskContainer",
    container_env: Env.Bindings[Value.Base],
    attempt: int,
    cache_add_paths: CallCacheAddPaths,
) -> str:
    """
    Evaluate the task command expression. In WDL 1.2, this may occur multiple times if retrying and
    the command uses `task.attempt`.
    """
    assert attempt >= 0
    command_env = container_env
    if wdl_version_geq(task.effective_wdl_version, WDLVersion.V1_2):
        container.update_task_runtime_info_struct(
            attempt=Value.Int(attempt),
            return_code=Value.Null(),
        )
        assert container.task_runtime_info_struct is not None
        command_env = command_env.bind("task", container.task_runtime_info_struct)
    old_command_dedent = cfg["task_runtime"].get_bool("old_command_dedent")
    # pylint: disable=E1101
    placeholder_re = regex.compile(cfg["task_runtime"]["placeholder_regex"], flags=regex.POSIX)
    command_stdlib = TaskInputStdLib(
        task.effective_wdl_version,
        logger,
        container,
        source_dir=task.source_dir,
        cache_add_paths=cache_add_paths,
        eval_context=StdLib.EvalContext(placeholder_regex=placeholder_re),
    )
    assert isinstance(task.command, Expr.TaskCommand)
    ans = task.command.eval(command_env, command_stdlib, dedent=not old_command_dedent).value
    if old_command_dedent:  # see issue #674
        ans = _util.strip_leading_whitespace(ans)[1]
    return ans


def _task_command_uses_task_attempt(command: Expr.TaskCommand) -> bool:
    """
    Test whether the command uses WDL 1.2's `task.attempt` (which necessitates re-evaluating the
    command on retry).
    """
    exprs = [part.expr for part in command.parts if isinstance(part, Expr.Placeholder)]
    while exprs:
        expr = exprs.pop()
        if (
            isinstance(expr, Expr.Get)
            and expr.member == "attempt"
            and isinstance(expr.expr, Expr.Get)
            and expr.expr.member is None
            and isinstance(expr.expr.expr, Expr.Ident)
            and expr.expr.expr.name == "task"
        ) or (isinstance(expr, Expr.Ident) and expr.name == "task.attempt"):
            return True
        exprs.extend(child for child in expr.children if isinstance(child, Expr.Base))
    return False


def _eval_task_outputs(
    logger: logging.Logger,
    run_id: str,
    task: Tree.Task,
    env: Env.Bindings[Value.Base],
    container: "TaskContainer",
) -> Env.Bindings[Value.Base]:
    stdout_file = os.path.join(container.host_dir, "stdout.txt")
    with suppress(FileNotFoundError):
        if os.path.getsize(stdout_file) > 0 and not run_id.startswith("download-"):
            # If the task produced nonempty stdout that isn't used in the WDL outputs, generate a
            # courtesy log message directing user where to find it
            stdout_used = False
            expr_stack = [outp.expr for outp in task.outputs]
            while expr_stack:
                expr = expr_stack.pop()
                assert isinstance(expr, Expr.Base)
                if isinstance(expr, Expr.Apply) and expr.function_name == "stdout":
                    stdout_used = True
                else:
                    expr_stack.extend(expr.children)  # type: ignore[arg-type]
            if not stdout_used:
                logger.info(
                    _(
                        "command stdout unused; consider output `File cmd_out = stdout()`"
                        " or redirect command to stderr log >&2",
                        stdout_file=stdout_file,
                    )
                )

    stdlib = TaskOutputStdLib(task.effective_wdl_version, logger, container)
    outputs: Env.Bindings[Value.Base] = Env.Bindings()

    # evaluate output declarations in dependency order
    for decl in _task_decl_eval_order(task.outputs):
        assert decl.expr
        # evaluate and check existence of in-container File/Directory output paths (tolerating
        # non-existence for optional outputs); bind to env for subsequent decls
        v = _eval_task_decl(
            logger,
            decl,
            env,
            stdlib,
            lambda value: _postprocess_task_output_decl_paths(logger, decl, value, container),
        )
        env = env.bind(decl.name, v)

        # rewrite in-container File/Directory paths to host paths, bind in outputs env
        try:
            v = Value.rewrite_paths(
                v, lambda w: _task_output_host_path(logger, decl.name, w, container)
            )
        except Error.RuntimeError as exn:
            setattr(exn, "job_id", decl.workflow_node_id)
            raise exn
        outputs = outputs.bind(decl.name, v)

    return outputs


def _postprocess_task_output_decl_paths(
    logger: logging.Logger,
    decl: Tree.Decl,
    value: Value.Base,
    container: "TaskContainer",
) -> Value.Base:
    """
    Log a task output value, then resolve missing File/Directory paths.
    """
    vj = json.dumps(value.json)
    logger.info(
        _("output", name=decl.name, value=(value.json if len(vj) < 4096 else "(((large)))"))
    )
    return _postprocess_task_decl_paths(
        decl,
        value,
        lambda v: _task_output_missing_path(v, container),
        lambda name: OutputError("File/Directory path not found in task output " + name),
    )


def _task_output_missing_path(
    v: Union[Value.File, Value.Directory],
    container: "TaskContainer",
) -> Optional[str]:
    """
    Return None for a task output File/Directory path missing from the container.
    """
    container_path = v.value
    if isinstance(v, Value.Directory) and not container_path.endswith("/"):
        container_path += "/"
    if container.host_path(container_path) is None:
        return None
    return v.value


def _task_output_host_path(
    logger: logging.Logger,
    output_name: str,
    v: Union[Value.File, Value.Directory],
    container: "TaskContainer",
) -> Optional[str]:
    """
    Rewrite an existing task output File/Directory path from container path to host path.
    """
    container_path = v.value
    if isinstance(v, Value.Directory) and not container_path.endswith("/"):
        container_path += "/"
    host_path = container.host_path(container_path)
    assert host_path is not None
    if isinstance(v, Value.Directory):
        if host_path.endswith("/"):
            host_path = host_path[:-1]
        _check_directory(host_path, output_name)
        logger.debug(_("output dir", container=container_path, host=host_path))
    else:
        logger.debug(_("output file", container=container_path, host=host_path))
    return host_path


def _check_directory(host_path: str, output_name: str) -> None:
    """
    Traverse an output directory to check that all symlinks are relative & resolve inside the dir.
    """

    def raiser(exc: OSError):
        raise exc

    for root, subdirs, files in os.walk(host_path, onerror=raiser, followlinks=False):
        for fn in files:
            fn = os.path.join(root, fn)
            if os.path.islink(fn) and (
                not os.path.exists(fn)
                or os.path.isabs(os.readlink(fn))
                or not path_really_within(fn, host_path)
            ):
                raise OutputError(f"Directory in output {output_name} contains unusable symlink")


def _delete_work(
    cfg: config.Loader,
    logger: logging.Logger,
    container: "Optional[TaskContainer]",
    success: bool,
) -> None:
    opt = cfg["file_io"]["delete_work"].strip().lower()
    if container and (
        opt == "always" or (success and opt == "success") or (not success and opt == "failure")
    ):
        if success and not cfg["file_io"].get_bool("output_hardlinks"):
            logger.warning(
                "ignoring configuration [file_io] delete_work because it requires also output_hardlinks = true"
            )
            return
        container.delete_work(logger, delete_streams=not success)
