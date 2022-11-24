# pyre-strict
"""
Local task runner
"""
import logging
import math
import os
import json
import traceback
import glob
import threading
import shutil
import regex
from typing import Tuple, List, Dict, Optional, Callable, Set, Any, Union
from contextlib import ExitStack, suppress

from .. import Error, Type, Env, Value, StdLib, Tree, Expr, _util
from .._util import (
    write_atomic,
    write_values_json,
    provision_run_dir,
    TerminationSignalFlag,
    chmod_R_plus,
    path_really_within,
    LoggingFileHandler,
    compose_coroutines,
    pathsize,
    link_force,
    symlink_force,
    rmtree_atomic,
)
from .._util import StructuredLogMessage as _
from . import config, _statusbar
from .download import able as downloadable, run_cached as download
from .cache import CallCache, new as new_call_cache
from .error import OutputError, Interrupted, Terminated, RunFailed, error_json


def run_local_task(
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
        logger.notice(  # pyre-fixme
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
        container = None
        try:
            cache_key = f"{task.name}/{task.digest}/{Value.digest_env(inputs)}"
            cached = cache.get(cache_key, inputs, task.effective_outputs)
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
                logger.notice("done (cached)")  # pyre-fixme
                # returning `cached`, not the rewritten `_outputs`, to retain opportunity to find
                # cached downstream inputs
                return (run_dir, cached)
            # start plugin coroutines and process inputs through them
            with compose_coroutines(
                [
                    (
                        lambda kwargs, cor=cor: cor(
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
                posix_inputs = _download_input_files(
                    cfg,
                    logger,
                    logger_prefix,
                    run_dir,
                    _add_downloadable_defaults(cfg, task.available_inputs, inputs),
                    cache,
                )

                # create TaskContainer according to configuration
                container = new_task_container(cfg, logger, run_id, run_dir)

                # evaluate input/postinput declarations, including mapping from host to
                # in-container file paths
                container_env = _eval_task_inputs(logger, task, posix_inputs, container)

                # evaluate runtime fields
                stdlib = InputStdLib(task.effective_wdl_version, logger, container)
                _eval_task_runtime(
                    cfg, logger, run_id, task, posix_inputs, container, container_env, stdlib
                )

                # interpolate command
                # pylint: disable=E1101
                placeholder_re = regex.compile(
                    cfg["task_runtime"]["placeholder_regex"], flags=regex.POSIX
                )
                setattr(
                    stdlib,
                    "_placeholder_regex",
                    placeholder_re,
                )  # hack to pass regex to WDL.Expr.Placeholder._eval
                command = _util.strip_leading_whitespace(
                    task.command.eval(container_env, stdlib).value
                )[1]
                delattr(stdlib, "_placeholder_regex")
                logger.debug(_("command", command=command.strip()))

                # process command & container through plugins
                recv = plugins.send({"command": command, "container": container})
                command, container = (recv[k] for k in ("command", "container"))

                # start container & run command (and retry if needed)
                _try_task(cfg, task, logger, container, command, terminating)

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

                # write outputs.json
                write_values_json(
                    outputs, os.path.join(run_dir, "outputs.json"), namespace=task.name
                )
                logger.notice("done")  # pyre-fixme
                if not run_id.startswith("download-"):
                    cache.put(cache_key, outputs)
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
                _delete_work(cfg, logger, container, False)
            except Exception as exn2:
                logger.debug(traceback.format_exc())
                logger.error(_("delete_work also failed", exception=str(exn2)))
            raise wrapper from exn


def _download_input_files(
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
        logger.notice(  # pyre-fixme
            _(
                "processed input URIs",
                downloaded=downloads,
                downloaded_bytes=download_bytes,
                cached=cached_hits,
            )
        )
    return ans


def _add_downloadable_defaults(
    cfg: config.Loader, available_inputs: Env.Bindings[Tree.Decl], inputs: Env.Bindings[Value.Base]
) -> Env.Bindings[Value.Base]:
    """
    Look for available File/Directory inputs that default to a string constant appearing to be a
    downloadable URI. For each one, add a binding for that default to the user-supplied inputs (if
    not already overridden in them).

    This is to trigger download of the default URIs even though we otherwise don't evaluate input
    declarations until after processing downloads.
    """
    ans = inputs
    for b in available_inputs:
        if (
            isinstance(b.value.type, (Type.File, Type.Directory))
            and b.name not in ans
            and isinstance(b.value.expr, Expr.String)
        ):
            directory = isinstance(b.value.type, Type.Directory)
            maybe_uri = b.value.expr.literal
            if maybe_uri and downloadable(cfg, maybe_uri.value, directory=directory):
                v = (
                    Value.Directory(maybe_uri.value, b.value.expr)
                    if directory
                    else Value.File(maybe_uri.value, b.value.expr)
                )
                ans = ans.bind(b.name, v)
    return ans


def _eval_task_inputs(
    logger: logging.Logger,
    task: Tree.Task,
    posix_inputs: Env.Bindings[Value.Base],
    container: "runtime.task_container.TaskContainer",
) -> Env.Bindings[Value.Base]:
    # Preprocess inputs: if None value is supplied for an input declared with a default but without
    # the ? type quantifier, remove the binding entirely so that the default will be used. In
    # contrast, if the input declaration has an -explicitly- optional type, then we'll allow the
    # supplied None to override any default.
    input_decls = task.available_inputs
    posix_inputs = posix_inputs.filter(
        lambda b: not (
            isinstance(b.value, Value.Null)
            and b.name in input_decls
            and input_decls[b.name].expr
            and not input_decls[b.name].type.optional
        )
    )

    # Map all the provided input File & Directory paths to in-container paths
    container.add_paths(_fspaths(posix_inputs))

    # copy posix_inputs with all File & Directory values mapped to their in-container paths
    def map_paths(fn: Union[Value.File, Value.Directory]) -> str:
        p = fn.value.rstrip("/")
        if isinstance(fn, Value.Directory):
            p += "/"
        return container.input_path_map[p]

    container_inputs = Value.rewrite_env_paths(posix_inputs, map_paths)

    # initialize value environment with the inputs
    container_env = Env.Bindings()
    for b in container_inputs:
        assert isinstance(b, Env.Binding)
        v = b.value
        assert isinstance(v, Value.Base)
        container_env = container_env.bind(b.name, v)
        vj = json.dumps(v.json)
        logger.info(_("input", name=b.name, value=(v.json if len(vj) < 4096 else "(((large)))")))

    # collect remaining declarations requiring evaluation.
    decls_to_eval = []
    for decl in (task.inputs or []) + (task.postinputs or []):
        if not container_env.has_binding(decl.name):
            decls_to_eval.append(decl)

    # topsort them according to internal dependencies. prior static validation
    # should have ensured they're acyclic.
    decls_by_id, decls_adj = Tree._decl_dependency_matrix(decls_to_eval)
    decls_to_eval = [decls_by_id[did] for did in _util.topsort(decls_adj)]
    assert len(decls_by_id) == len(decls_to_eval)

    # evaluate each declaration in that order
    # note: the write_* functions call container.add_paths as a side-effect
    stdlib = InputStdLib(task.effective_wdl_version, logger, container)
    for decl in decls_to_eval:
        assert isinstance(decl, Tree.Decl)
        v = Value.Null()
        if decl.expr:
            try:
                v = decl.expr.eval(container_env, stdlib=stdlib).coerce(decl.type)
            except Error.RuntimeError as exn:
                setattr(exn, "job_id", decl.workflow_node_id)
                raise exn
            except Exception as exn:
                exn2 = Error.EvalError(decl, str(exn))
                setattr(exn2, "job_id", decl.workflow_node_id)
                raise exn2 from exn
        else:
            assert decl.type.optional
        vj = json.dumps(v.json)
        logger.info(_("eval", name=decl.name, value=(v.json if len(vj) < 4096 else "(((large)))")))
        container_env = container_env.bind(decl.name, v)

    return container_env


def _fspaths(env: Env.Bindings[Value.Base]) -> Set[str]:
    """
    Get the unique paths of all File & Directory values in the environment. Directory paths will
    have a trailing '/'.
    """
    ans = set()

    def collector(v: Value.Base) -> None:
        if isinstance(v, Value.File):
            assert not v.value.endswith("/")
            ans.add(v.value)
        elif isinstance(v, Value.Directory):
            ans.add(v.value.rstrip("/") + "/")
        for ch in v.children:
            collector(ch)

    for b in env:
        collector(b.value)
    return ans


def _eval_task_runtime(
    cfg: config.Loader,
    logger: logging.Logger,
    run_id: str,
    task: Tree.Task,
    inputs: Env.Bindings[Value.Base],
    container: "runtime.task_container.TaskContainer",
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
        logger.notice(  # pyre-ignore
            _(
                "overriding environment variables (portability warning)",
                names=list(env_vars_override.keys()),
            )
        )
        logger.debug(
            _("overriding environment variables (portability warning)", **env_vars_override)
        )
        container.runtime_values["env"].update(env_vars_override)

    # process decls with "env" decorator (EXPERIMENTAL)
    env_decls = {}
    for decl in (task.inputs or []) + task.postinputs:
        if decl.decor.get("env", False) is True:
            if not env_decls:
                logger.warning(
                    "task env declarations are an experimental feature, subject to change"
                )
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
    container: "runtime.task_container.TaskContainer",
    command: str,
    terminating: Callable[[], bool],
) -> None:
    """
    Run the task command in the container, retrying up to runtime.preemptible occurrences of
    Interrupted errors, plus up to runtime.maxRetries occurrences of any error.
    """
    from docker.errors import BuildError as DockerBuildError  # delay heavy import

    max_retries = container.runtime_values.get("maxRetries", 0)
    max_interruptions = container.runtime_values.get("preemptible", 0)
    retries = 0
    interruptions = 0

    while True:
        if terminating():
            raise Terminated()
        # copy input files, if needed
        if cfg.get_bool("file_io", "copy_input_files") or task.name in cfg.get_list(
            "file_io", "copy_input_files_for"
        ):
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
                return container.run(logger, command)
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
            _delete_work(cfg, logger, container, False)
            container.reset(logger)


def _eval_task_outputs(
    logger: logging.Logger,
    run_id: str,
    task: Tree.Task,
    env: Env.Bindings[Value.Base],
    container: "runtime.task_container.TaskContainer",
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
                    expr_stack.extend(expr.children)
            if not stdout_used:
                logger.info(
                    _(
                        "command stdout unused; consider output `File cmd_out = stdout()`"
                        " or redirect command to stderr log >&2",
                        stdout_file=stdout_file,
                    )
                )

    # Helpers to rewrite File/Directory from in-container paths to host paths
    # First pass -- convert nonexistent output paths to None/Null
    def rewriter1(v: Union[Value.File, Value.Directory], output_name: str) -> Optional[str]:
        container_path = v.value
        if isinstance(v, Value.Directory) and not container_path.endswith("/"):
            container_path += "/"
        if container.host_path(container_path) is None:
            logger.warning(
                _(
                    "output path not found in container (error unless declared type is optional)",
                    output=output_name,
                    path=container_path,
                )
            )
            return None
        return v.value

    # Second pass -- convert in-container paths to host paths
    def rewriter2(v: Union[Value.File, Value.Directory], output_name: str) -> Optional[str]:
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

    stdlib = OutputStdLib(task.effective_wdl_version, logger, container)
    outputs = Env.Bindings()
    for decl in task.outputs:
        assert decl.expr
        try:
            v = decl.expr.eval(env, stdlib=stdlib).coerce(decl.type)
        except Error.RuntimeError as exn:
            setattr(exn, "job_id", decl.workflow_node_id)
            raise exn
        except Exception as exn:
            exn2 = Error.EvalError(decl, str(exn))
            setattr(exn2, "job_id", decl.workflow_node_id)
            raise exn2 from exn
        vj = json.dumps(v.json)
        logger.info(
            _("output", name=decl.name, value=(v.json if len(vj) < 4096 else "(((large)))"))
        )

        # Now, a delicate sequence for postprocessing File outputs (including Files nested within
        # compound values)

        # First convert nonexistent paths to None/Null, and bind this in the environment for
        # evaluating subsequent output expressions.
        v = Value.rewrite_paths(v, lambda w: rewriter1(w, decl.name))
        env = env.bind(decl.name, v)
        # check if any nonexistent paths were provided for (non-optional) File/Directory types
        # Value.Null.coerce has a special behavior for us to raise FileNotFoundError for a
        # non-optional File/Directory type.
        try:
            v = v.coerce(decl.type)
        except FileNotFoundError:
            exn = OutputError("File/Directory path not found in task output " + decl.name)
            setattr(exn, "job_id", decl.workflow_node_id)
            raise exn
        # Rewrite in-container paths to host paths
        v = Value.rewrite_paths(v, lambda w: rewriter2(w, decl.name))
        outputs = outputs.bind(decl.name, v)

    return outputs


def _check_directory(host_path: str, output_name: str) -> None:
    """
    traverse output directory to check that all symlinks are relative & resolve inside the dir
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


def link_outputs(
    cache: CallCache,
    outputs: Env.Bindings[Value.Base],
    run_dir: str,
    hardlinks: bool = False,
    use_relative_output_paths: bool = False,
) -> Env.Bindings[Value.Base]:
    """
    Following a successful run, the output files may be scattered throughout a complex directory
    tree used for execution. To help navigating this, generate a subdirectory of the run directory
    containing nicely organized symlinks to the output files, and rewrite File values in the
    outputs env to use these symlinks.
    """

    def link1(target: str, link: str, directory: bool) -> None:
        if hardlinks:
            # TODO: what if target is an input from a different filesystem?
            if directory:
                shutil.copytree(target, link, symlinks=True, copy_function=link_force)
            else:
                link_force(target, link)
        else:
            symlink_force(target, link)

    def map_paths(v: Value.Base, dn: str) -> Value.Base:
        if isinstance(v, (Value.File, Value.Directory)):
            target = (
                v.value
                if os.path.exists(v.value)
                else cache.get_download(v.value, isinstance(v, Value.Directory))
            )
            if target:
                target = os.path.realpath(target)
                assert os.path.exists(target)
                if not hardlinks and path_really_within(target, os.path.dirname(run_dir)):
                    # make symlink relative
                    target = os.path.relpath(target, start=os.path.realpath(dn))
                link = os.path.join(dn, os.path.basename(v.value.rstrip("/")))
                os.makedirs(dn, exist_ok=False)
                link1(target, link, isinstance(v, Value.Directory))
                # Drop a dotfile alongside Directory outputs, to inform a program crawling the out/
                # directory without reference to the output types or JSON for whatever reason. It
                # might otherwise have trouble distinguishing Directory outputs among the
                # structured subdirectories we create for compound types.
                if isinstance(v, Value.Directory):
                    with open(os.path.join(dn, ".WDL_Directory"), "w") as dotfile:
                        pass
                v.value = link
        # recurse into compound values
        elif isinstance(v, Value.Array) and v.value:
            d = int(math.ceil(math.log10(len(v.value))))  # how many digits needed
            for i in range(len(v.value)):
                v.value[i] = map_paths(v.value[i], os.path.join(dn, str(i).rjust(d, "0")))
        elif isinstance(v, Value.Map):
            # create a subdirectory for each key, as long as the key names seem to make reasonable
            # path components; otherwise, treat the dict as a list of its values
            keys_ok = (
                sum(
                    1
                    for b in v.value
                    if regex.fullmatch("[-_a-zA-Z0-9][-_a-zA-Z0-9.]*", str(b[0]).strip("'\""))
                    is None
                )
                == 0
            )
            d = int(math.ceil(math.log10(len(v.value))))
            for i, b in enumerate(v.value):
                v.value[i] = (
                    b[0],
                    map_paths(
                        b[1],
                        os.path.join(
                            dn, str(b[0]).strip("'\"") if keys_ok else str(i).rjust(d, "0")
                        ),
                    ),
                )
        elif isinstance(v, Value.Pair):
            v.value = (
                map_paths(v.value[0], os.path.join(dn, "left")),
                map_paths(v.value[1], os.path.join(dn, "right")),
            )
        elif isinstance(v, Value.Struct):
            for key in v.value:
                v.value[key] = map_paths(v.value[key], os.path.join(dn, key))
        return v

    os.makedirs(os.path.join(run_dir, "out"), exist_ok=False)

    if use_relative_output_paths:
        return link_outputs_relative(link1, cache, outputs, run_dir, hardlinks=hardlinks)

    return outputs.map(
        lambda binding: Env.Binding(
            binding.name,
            map_paths(
                Value.rewrite_paths(binding.value, lambda v: v.value),  # nop to deep copy
                os.path.join(run_dir, "out", binding.name),
            ),
        )
    )


def link_outputs_relative(
    link1: Callable[[str, str, bool], None],
    cache: CallCache,
    outputs: Env.Bindings[Value.Base],
    run_dir: str,
    hardlinks: bool = False,
) -> Env.Bindings[Value.Base]:
    """
    link_outputs with [file_io] use_relative_output_paths = true. We organize the links to reflect
    the generated files' paths relative to their task working directory.
    """
    link_destinations = dict()

    def map_path_relative(v: Union[Value.File, Value.Directory]) -> str:
        target = (
            v.value
            if os.path.exists(v.value)
            else cache.get_download(v.value, isinstance(v, Value.Directory))
        )
        if target:
            real_target = os.path.realpath(target)
            rel_link = None
            if path_really_within(target, os.path.join(run_dir, "work")):
                # target was generated by current task; use its path relative to the task work dir
                if not os.path.basename(run_dir).startswith("download-"):  # except download tasks
                    rel_link = os.path.relpath(real_target, os.path.join(run_dir, "work"))
            else:
                # target is an out/ link generated by a call in the current workflow OR a cached
                # run; use the link's path relative to that out/ dir, which by induction should
                # equal its path relative to the original work/ dir.
                # we need heuristic to find the out/ dir in a task/workflow run directory, since the
                # user's cwd or the task-generated relative path might coincidentally have
                # something named 'out'.
                p = None
                for p in reversed([m.span()[0] for m in regex.finditer("/out(?=/)", target)]):
                    if p and (
                        os.path.isfile(os.path.join(target[:p], "task.log"))
                        or os.path.isfile(os.path.join(target[:p], "workflow.log"))
                    ):
                        break
                    p = None
                if p and p + 5 < len(target):
                    rel_link = os.path.relpath(target, target[: p + 5])
            # if neither of the above cases applies, then fall back to just the target basename
            rel_link = rel_link or os.path.basename(target)
            abs_link = os.path.join(os.path.join(run_dir, "out"), rel_link)
            if link_destinations.get(abs_link, real_target) != real_target:
                raise FileExistsError(
                    "Output filename collision; to allow this, set"
                    " [file_io] use_relative_output_paths = false. Affected path: " + abs_link
                )
            os.makedirs(os.path.dirname(abs_link), exist_ok=True)
            link1(real_target, abs_link, isinstance(v, Value.Directory))
            link_destinations[abs_link] = real_target
            return abs_link
        return v.value

    return Value.rewrite_env_paths(outputs, map_path_relative)


def _delete_work(
    cfg: config.Loader,
    logger: logging.Logger,
    container: "Optional[runtime.task_container.TaskContainer]",
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


class _StdLib(StdLib.Base):
    logger: logging.Logger
    container: "runtime.task_container.TaskContainer"
    inputs_only: bool  # if True then only permit access to input files

    def __init__(
        self,
        wdl_version: str,
        logger: logging.Logger,
        container: "runtime.task_container.TaskContainer",
        inputs_only: bool,
    ) -> None:
        super().__init__(wdl_version, write_dir=os.path.join(container.host_dir, "write_"))
        self.logger = logger
        self.container = container
        self.inputs_only = inputs_only

    def _devirtualize_filename(self, filename: str) -> str:
        # check allowability of reading this file, & map from in-container to host
        ans = self.container.host_path(filename, inputs_only=self.inputs_only)
        if ans is None:
            raise OutputError("function was passed non-existent file " + filename)
        self.logger.debug(_("read_", container=filename, host=ans))
        return ans

    def _virtualize_filename(self, filename: str) -> str:
        # register new file with container input_path_map
        self.container.add_paths([filename])
        self.logger.debug(
            _("write_", host=filename, container=self.container.input_path_map[filename])
        )
        self.logger.info(_("wrote", file=self.container.input_path_map[filename]))
        return self.container.input_path_map[filename]


class InputStdLib(_StdLib):
    # StdLib for evaluation of task inputs and command
    def __init__(
        self,
        wdl_version: str,
        logger: logging.Logger,
        container: "runtime.task_container.TaskContainer",
    ) -> None:
        super().__init__(wdl_version, logger, container, True)


class OutputStdLib(_StdLib):
    # StdLib for evaluation of task outputs
    def __init__(
        self,
        wdl_version: str,
        logger: logging.Logger,
        container: "runtime.task_container.TaskContainer",
    ) -> None:
        super().__init__(wdl_version, logger, container, False)

        setattr(
            self,
            "stdout",
            StdLib.StaticFunction(
                "stdout",
                [],
                Type.File(),
                lambda: Value.File(os.path.join(self.container.container_dir, "stdout.txt")),
            ),
        )
        setattr(
            self,
            "stderr",
            StdLib.StaticFunction(
                "stderr",
                [],
                Type.File(),
                lambda: Value.File(os.path.join(self.container.container_dir, "stderr.txt")),
            ),
        )

        def _glob(pattern: Value.String, lib: OutputStdLib = self) -> Value.Array:
            pat = pattern.coerce(Type.String()).value
            if not pat:
                raise OutputError("empty glob() pattern")
            assert isinstance(pat, str)
            if pat[0] == "/":
                raise OutputError("glob() pattern must be relative to task working directory")
            if pat.startswith("..") or "/.." in pat:
                raise OutputError("glob() pattern must not use .. uplevels")
            if pat.startswith("./"):
                pat = pat[2:]
            # glob the host directory
            pat = os.path.join(lib.container.host_work_dir(), pat)
            host_files = sorted(fn for fn in glob.glob(pat) if os.path.isfile(fn))
            # convert the host filenames to in-container filenames
            container_files = []
            for hf in host_files:
                dstrip = lib.container.host_dir
                dstrip += "" if dstrip.endswith("/") else "/"
                assert hf.startswith(dstrip)
                container_files.append(os.path.join(lib.container.container_dir, hf[len(dstrip) :]))
            return Value.Array(Type.File(), [Value.File(fn) for fn in container_files])

        setattr(
            self,
            "glob",
            StdLib.StaticFunction("glob", [Type.String()], Type.Array(Type.File()), _glob),
        )
