# pyre-strict
"""
Local task runner
"""
import logging
import math
import os
import json
import copy
import traceback
import glob
import threading
import shutil
import re
from typing import Tuple, List, Dict, Optional, Callable, Set, Any
from contextlib import ExitStack

from .. import Error, Type, Env, Value, StdLib, Tree, _util
from .._util import (
    write_atomic,
    write_values_json,
    provision_run_dir,
    TerminationSignalFlag,
    parse_byte_size,
    chmod_R_plus,
    path_really_within,
    LoggingFileHandler,
    compose_coroutines,
)
from .._util import StructuredLogMessage as _
from . import config, _statusbar
from .download import able as downloadable, run_cached as download
from .cache import CallCache
from .error import *
from .task_container import TaskContainer
from .task_container import new as new_task_container


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

    _run_id_stack = _run_id_stack or []
    run_id = run_id or task.name
    logger_prefix = (logger_prefix or ["wdl"]) + ["t:" + run_id]
    logger = logging.getLogger(".".join(logger_prefix))
    with ExitStack() as cleanup:
        cleanup.enter_context(_statusbar.task_slotted())
        terminating = cleanup.enter_context(TerminationSignalFlag(logger))
        if terminating():
            raise Terminated(quiet=True)

        # provision run directory and log file
        run_dir = provision_run_dir(task.name, run_dir, last_link=not _run_id_stack)
        logfile = os.path.join(run_dir, "task.log")
        fh = cleanup.enter_context(
            LoggingFileHandler(
                logger,
                logfile,
                json=(
                    cfg["logging"].get_bool("json") if cfg.has_option("logging", "json") else False
                ),
            )
        )
        logger.notice(  # pyre-fixme
            _(
                "task start",
                name=task.name,
                source=task.pos.uri,
                line=task.pos.line,
                column=task.pos.column,
                dir=run_dir,
            )
        )
        logger.info(_("thread", ident=threading.get_ident()))
        write_values_json(inputs, os.path.join(run_dir, "inputs.json"))

        if not _run_id_stack:
            wdl_doc = getattr(task, "parent")
            cache = _cache or cleanup.enter_context(CallCache(cfg, logger))
            cache.flock(logfile, exclusive=True)  # no containing workflow; flock task.log

        else:
            cache = _cache
        assert cache

        try:
            input_digest = cache.get_digest_for_inputs(inputs)
            task_digest = cache.get_digest_for_task(task)
            cached = cache.get(
                key=f"{task.name}_{task_digest}/{input_digest}", output_types=task.effective_outputs
            )
            if cached:
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
                outputs = link_outputs(
                    cached, run_dir, hardlinks=cfg["file_io"].get_bool("output_hardlinks")
                )
                write_values_json(
                    cached, os.path.join(run_dir, "outputs.json"), namespace=task.name
                )
                logger.notice("done (cached)")  # pyre-fixme
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
                    cfg, logger, logger_prefix, run_dir, inputs, cache
                )

                # create TaskContainer according to configuration
                container = new_task_container(cfg, logger, run_id, run_dir)

                # evaluate input/postinput declarations, including mapping from host to
                # in-container file paths
                container_env = _eval_task_inputs(logger, task, posix_inputs, container)

                # evaluate runtime fields
                container.runtime_values = _eval_task_runtime(
                    cfg, logger, task, container, container_env
                )

                # interpolate command
                command = _util.strip_leading_whitespace(
                    task.command.eval(container_env, stdlib=InputStdLib(logger, container)).value
                )[1]
                logger.debug(_("command", command=command.strip()))

                # process command & container through plugins
                recv = plugins.send({"command": command, "container": container})
                command, container = (recv[k] for k in ("command", "container"))

                # start container & run command (and retry if needed)
                _try_task(cfg, logger, container, command, terminating)

                # evaluate output declarations
                outputs = _eval_task_outputs(logger, task, container_env, container)

                # create output_links
                outputs = link_outputs(
                    outputs, run_dir, hardlinks=cfg["file_io"].get_bool("output_hardlinks")
                )

                # process outputs through plugins
                recv = plugins.send({"outputs": outputs})
                outputs = recv["outputs"]

                # clean up, if so configured, and make sure output files will be accessible to
                # downstream tasks
                _delete_work(cfg, logger, run_dir, True)
                chmod_R_plus(run_dir, file_bits=0o660, dir_bits=0o770)

                # write outputs.json
                write_values_json(
                    outputs, os.path.join(run_dir, "outputs.json"), namespace=task.name
                )
                logger.notice("done")  # pyre-fixme
                cache.put(f"{task.name}_{task_digest}", input_digest, outputs)
                return (run_dir, outputs)
        except Exception as exn:
            logger.debug(traceback.format_exc())
            wrapper = RunFailed(task, run_id, run_dir)
            logmsg = _(str(wrapper), dir=run_dir, **error_json(exn))
            if isinstance(exn, Terminated) and getattr(exn, "quiet", False):
                logger.debug(logmsg)
            else:
                logger.error(logmsg)
            try:
                write_atomic(
                    json.dumps(error_json(wrapper, cause=exn), indent=2),
                    os.path.join(run_dir, "error.json"),
                )
            except Exception as exn2:
                logger.debug(traceback.format_exc())
                logger.critical(_("failed to write error.json", dir=run_dir, message=str(exn2)))
            try:
                _delete_work(cfg, logger, run_dir, False)
            except:
                logger.exception("delete_work failed")
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
    Find all File values in the inputs (including any nested within compound values) that need
    to / can be downloaded. Download them to some location under run_dir and return a copy of the
    inputs with the URI values replaced by the downloaded filenames.
    """

    downloads = 0
    download_bytes = 0
    cached_hits = 0
    cached_bytes = 0

    def rewriter(uri: str) -> str:
        nonlocal downloads, download_bytes, cached_hits, cached_bytes
        if downloadable(cfg, uri):
            logger.info(_("download input file", uri=uri))
            cached, filename = download(
                cfg,
                logger,
                cache,
                uri,
                run_dir=os.path.join(run_dir, "download", str(downloads), "."),
                logger_prefix=logger_prefix + [f"download{downloads}"],
            )
            sz = os.path.getsize(filename)
            if cached:
                cached_hits += 1
                cached_bytes += sz
            else:
                logger.info(_("downloaded input file", uri=uri, file=filename, bytes=sz))
                downloads += 1
                download_bytes += sz
            return filename
        return uri

    ans = Value.rewrite_env_files(inputs, rewriter)
    if downloads or cached_hits:
        logger.notice(  # pyre-fixme
            _(
                "downloaded input files",
                downloaded=downloads,
                downloaded_bytes=download_bytes,
                cached=cached_hits,
                cached_bytes=cached_bytes,
            )
        )
    return ans


def _eval_task_inputs(
    logger: logging.Logger,
    task: Tree.Task,
    posix_inputs: Env.Bindings[Value.Base],
    container: TaskContainer,
) -> Env.Bindings[Value.Base]:

    # Map all the provided input Files to in-container paths
    container.add_files(_filenames(posix_inputs))

    # copy posix_inputs with all Files mapped to their in-container paths
    def map_files(fn: str) -> str:
        return container.input_file_map[fn]

    container_inputs = Value.rewrite_env_files(posix_inputs, map_files)

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
    # note: the write_* functions call container.add_files as a side-effect
    stdlib = InputStdLib(logger, container)
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


def _filenames(env: Env.Bindings[Value.Base]) -> Set[str]:
    "Get the filenames of all File values in the environment"
    ans = set()

    def collector(v: Value.Base) -> None:
        if isinstance(v, Value.File):
            ans.add(v.value)
        for ch in v.children:
            collector(ch)

    for b in env:
        collector(b.value)
    return ans


def _eval_task_runtime(
    cfg: config.Loader,
    logger: logging.Logger,
    task: Tree.Task,
    container: TaskContainer,
    env: Env.Bindings[Value.Base],
) -> Dict[str, Union[int, str]]:
    runtime_values = {}
    for key, v in cfg["task_runtime"].get_dict("defaults").items():
        if isinstance(v, str):
            runtime_values[key] = Value.String(v)
        elif isinstance(v, int):
            runtime_values[key] = Value.Int(v)
        else:
            raise Error.InputError(f"invalid default runtime setting {key} = {v}")
    for key, expr in task.runtime.items():
        runtime_values[key] = expr.eval(env)
    logger.debug(_("runtime values", **dict((key, str(v)) for key, v in runtime_values.items())))
    ans = {}

    if "docker" in runtime_values:
        ans["docker"] = runtime_values["docker"].coerce(Type.String()).value

    host_limits = container.__class__.detect_resource_limits(cfg, logger)
    if "cpu" in runtime_values:
        cpu_value = runtime_values["cpu"].coerce(Type.Int()).value
        assert isinstance(cpu_value, int)
        cpu_max = cfg["task_runtime"].get_int("cpu_max")
        if cpu_max == 0:
            cpu_max = host_limits["cpu"]
        cpu = max(1, cpu_value if cpu_value <= cpu_max or cpu_max < 0 else cpu_max,)
        if cpu != cpu_value:
            logger.warning(
                _("runtime.cpu adjusted to host limit", original=cpu_value, adjusted=cpu)
            )
        ans["cpu"] = cpu

    if "memory" in runtime_values:
        memory_str = runtime_values["memory"].coerce(Type.String()).value
        assert isinstance(memory_str, str)
        try:
            memory_bytes = parse_byte_size(memory_str)
        except ValueError:
            raise Error.EvalError(
                task.runtime["memory"], "invalid setting of runtime.memory, " + memory_str
            )

        memory_max = cfg["task_runtime"]["memory_max"].strip()
        memory_max = -1 if memory_max == "-1" else parse_byte_size(memory_max)
        if memory_max == 0:
            memory_max = host_limits["mem_bytes"]
        if memory_max > 0 and memory_bytes > memory_max:
            logger.warning(
                _(
                    "runtime.memory adjusted to host limit",
                    original=memory_bytes,
                    adjusted=memory_max,
                )
            )
            memory_bytes = memory_max
        ans["memory_reservation"] = memory_bytes

        memory_limit_multiplier = cfg["task_runtime"].get_float("memory_limit_multiplier")
        if memory_limit_multiplier > 0.0:
            ans["memory_limit"] = int(memory_limit_multiplier * memory_bytes)

    if "maxRetries" in runtime_values:
        ans["maxRetries"] = max(0, runtime_values["maxRetries"].coerce(Type.Int()).value)
    if "preemptible" in runtime_values:
        ans["preemptible"] = max(0, runtime_values["preemptible"].coerce(Type.Int()).value)

    if ans:
        logger.info(_("effective runtime", **ans))
    unused_keys = list(
        key for key in runtime_values if key not in ("cpu", "memory") and key not in ans
    )
    if unused_keys:
        logger.warning(_("ignored runtime settings", keys=unused_keys))

    return ans


def _try_task(
    cfg: config.Loader,
    logger: logging.Logger,
    container: TaskContainer,
    command: str,
    terminating: Callable[[], bool],
) -> None:
    """
    Run the task command in the container, retrying up to runtime.preemptible occurrences of
    Interrupted errors, plus up to runtime.maxRetries occurrences of any error.
    """
    max_retries = container.runtime_values.get("maxRetries", 0)
    max_interruptions = container.runtime_values.get("preemptible", 0)
    retries = 0
    interruptions = 0

    while True:
        if terminating():
            raise Terminated()
        # copy input files, if needed
        if cfg["file_io"].get_bool("copy_input_files"):
            container.copy_input_files(logger)

        try:
            # start container & run command
            try:
                return container.run(logger, command)
            finally:
                if (
                    "preemptible" in container.runtime_values
                    and cfg.has_option("task_runtime", "_mock_interruptions")
                    and interruptions < cfg["task_runtime"].get_int("_mock_interruptions")
                ):
                    raise Interrupted("mock interruption") from None
                if terminating():
                    raise Terminated()
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
            elif not isinstance(exn, Terminated) and retries < max_retries:
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
            container.reset(
                logger,
                interruptions + retries - 1,
                delete_work=(
                    cfg["file_io"]["delete_work"].strip().lower() in ["always", "failure"]
                ),
            )


def _eval_task_outputs(
    logger: logging.Logger, task: Tree.Task, env: Env.Bindings[Value.Base], container: TaskContainer
) -> Env.Bindings[Value.Base]:

    # helper to rewrite Files from in-container paths to host paths
    def rewriter(fn: str, output_name: str) -> str:
        host_file = container.host_file(fn)
        if host_file is None:
            logger.warning(
                _(
                    "output file not found in container (error unless declared type is optional)",
                    name=output_name,
                    file=fn,
                )
            )
        else:
            logger.debug(_("output file", container=fn, host=host_file))
        # We may overwrite File.value with None, which is an invalid state, then we'll fix it
        # up (or abort) below. This trickery is because we don't, at this point, know whether
        # the 'desired' output type is File or File?.
        return host_file  # pyre-fixme

    stdlib = OutputStdLib(logger, container)
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

        # First bind the value as-is in the environment, so that subsequent output expressions will
        # "see" the in-container path(s) if they use this binding.
        env = env.bind(decl.name, v)
        # Rewrite each File.value to either a host path, or None if the file doesn't exist.
        v = Value.rewrite_files(v, lambda fn: rewriter(fn, decl.name))
        # File.coerce has a special behavior for us so that, if the value is None:
        #   - produces Value.Null() if the desired type is File?
        #   - raises FileNotFoundError otherwise.
        try:
            v = v.coerce(decl.type)
        except FileNotFoundError:
            exn = OutputError("File not found in task output " + decl.name)
            setattr(exn, "job_id", decl.workflow_node_id)
            raise exn
        outputs = outputs.bind(decl.name, v)

    return outputs


def link_outputs(
    outputs: Env.Bindings[Value.Base], run_dir: str, hardlinks: bool = False
) -> Env.Bindings[Value.Base]:
    """
    Following a successful run, the output files may be scattered throughout a complex directory
    tree used for execution. To help navigating this, generate a subdirectory of the run directory
    containing nicely organized symlinks to the output files, and rewrite File values in the
    outputs env to use these symlinks.
    """

    def map_files(v: Value.Base, dn: str) -> Value.Base:
        if isinstance(v, Value.File):
            if os.path.isfile(v.value):
                hardlink = os.path.realpath(v.value)
                assert os.path.isfile(hardlink)
                newlink = os.path.join(dn, os.path.basename(v.value))
                os.makedirs(dn, exist_ok=False)
                if not hardlinks and path_really_within(hardlink, os.path.dirname(run_dir)):
                    # make symlink relative
                    hardlink = os.path.relpath(hardlink, start=os.path.realpath(dn))
                (os.link if hardlinks else os.symlink)(hardlink, newlink)
                v.value = newlink
        # recurse into compound values
        elif isinstance(v, Value.Array) and v.value:
            d = int(math.ceil(math.log10(len(v.value))))  # how many digits needed
            for i in range(len(v.value)):
                v.value[i] = map_files(v.value[i], os.path.join(dn, str(i).rjust(d, "0")))
        elif isinstance(v, Value.Map):
            # create a subdirectory for each key, as long as the key names seem to make reasonable
            # path components; otherwise, treat the dict as a list of its values
            keys_ok = (
                sum(
                    1
                    for b in v.value
                    if re.fullmatch("[-_a-zA-Z0-9][-_a-zA-Z0-9.]*", str(b[0])) is None
                )
                == 0
            )
            d = int(math.ceil(math.log10(len(v.value))))
            for i, b in enumerate(v.value):
                v.value[i] = (
                    b[0],
                    map_files(
                        b[1], os.path.join(dn, str(b[0]) if keys_ok else str(i).rjust(d, "0"))
                    ),
                )
        elif isinstance(v, Value.Pair):
            v.value = (
                map_files(v.value[0], os.path.join(dn, "left")),
                map_files(v.value[1], os.path.join(dn, "right")),
            )
        elif isinstance(v, Value.Struct):
            for key in v.value:
                v.value[key] = map_files(v.value[key], os.path.join(dn, key))
        return v

    os.makedirs(os.path.join(run_dir, "out"), exist_ok=False)
    # out/ used to be called output_links/ -- symlink this name to ease transition
    os.symlink("out", os.path.join(run_dir, "output_links"))
    return outputs.map(
        lambda binding: Env.Binding(
            binding.name,
            map_files(copy.deepcopy(binding.value), os.path.join(run_dir, "out", binding.name),),
        )
    )


def _delete_work(cfg: config.Loader, logger: logging.Logger, run_dir: str, success: bool) -> None:
    opt = cfg["file_io"]["delete_work"].strip().lower()
    if opt == "always" or (success and opt == "success") or (not success and opt == "failure"):
        if success and not cfg["file_io"].get_bool("output_hardlinks"):
            logger.warning(
                "ignoring configuration [file_io] delete_work because it requires also output_hardlinks = true"
            )
            return
        for dn in ["write_", "work", "failed_tries"]:
            dn = os.path.join(run_dir, dn)
            if os.path.isdir(dn):
                shutil.rmtree(dn)
                logger.info(_("deleted working directory", dir=dn))


class _StdLib(StdLib.Base):
    logger: logging.Logger
    container: TaskContainer
    inputs_only: bool  # if True then only permit access to input files

    def __init__(self, logger: logging.Logger, container: TaskContainer, inputs_only: bool) -> None:
        super().__init__(write_dir=os.path.join(container.host_dir, "write_"))
        self.logger = logger
        self.container = container
        self.inputs_only = inputs_only

    def _devirtualize_filename(self, filename: str) -> str:
        # check allowability of reading this file, & map from in-container to host
        ans = self.container.host_file(filename, inputs_only=self.inputs_only)
        if ans is None:
            raise OutputError("function was passed non-existent file " + filename)
        self.logger.debug(_("read_", container=filename, host=ans))
        return ans

    def _virtualize_filename(self, filename: str) -> str:
        # register new file with container input_file_map
        self.container.add_files([filename])
        self.logger.debug(
            _("write_", host=filename, container=self.container.input_file_map[filename])
        )
        self.logger.info(_("wrote", file=self.container.input_file_map[filename]))
        return self.container.input_file_map[filename]


class InputStdLib(_StdLib):
    # StdLib for evaluation of task inputs and command
    def __init__(self, logger: logging.Logger, container: TaskContainer) -> None:
        super().__init__(logger, container, True)


class OutputStdLib(_StdLib):
    # StdLib for evaluation of task outputs
    def __init__(self, logger: logging.Logger, container: TaskContainer) -> None:
        super().__init__(logger, container, False)

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
            pat = os.path.join(lib.container.host_dir, "work", pat)
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
