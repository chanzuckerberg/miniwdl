"""
miniwdl command-line interface
"""
# PYTHON_ARGCOMPLETE_OK
import sys
import os
import platform
import tempfile
import json
import logging
import asyncio
import atexit
import textwrap
from shlex import quote as shellquote
from argparse import ArgumentParser, Action, SUPPRESS, RawDescriptionHelpFormatter
from contextlib import ExitStack
import argcomplete
from . import (
    load,
    Error,
    Value,
    Type,
    Expr,
    Document,
    Workflow,
    Task,
    Env,
    Decl,
    Call,
    Scatter,
    Conditional,
    SourcePosition,
    parse_document,
    copy_source,
    values_from_json,
    values_to_json,
    read_source_default,
    ReadSourceResult,
)
from ._util import (
    VERBOSE_LEVEL,
    NOTICE_LEVEL,
    configure_logger,
    parse_byte_size,
    path_really_within,
    ANSI,
    currently_in_container,
    LoggingFileHandler,
)
from ._util import StructuredLogMessage as _

quant_warning = False


def main(args=None):
    sys.setrecursionlimit(1_000_000)  # permit as much call stack depth as OS can give us

    parser = create_arg_parser()
    argcomplete.autocomplete(parser)

    replace_COLUMNS = os.environ.get("COLUMNS", None)
    os.environ["COLUMNS"] = "100"  # make help descriptions wider
    args = parser.parse_args(args if args is not None else sys.argv[1:])
    if replace_COLUMNS is not None:
        os.environ["COLUMNS"] = replace_COLUMNS
    else:
        del os.environ["COLUMNS"]

    try:
        if args.command == "check":
            check(**vars(args))
        elif args.command == "run":
            runner(**vars(args))
        elif args.command == "run_self_test":
            run_self_test(**vars(args))
        elif args.command == "localize":
            localize(**vars(args))
        elif args.command == "configure":
            configure(**vars(args))
        else:
            assert False
    except (
        Error.SyntaxError,
        Error.ImportError,
        Error.ValidationError,
        Error.MultipleValidationErrors,
    ) as exn:
        global quant_warning
        print_error(exn)
        if args.check_quant and quant_warning:
            print(
                "* Hint: for compatibility with older existing WDL code, try setting --no-quant-check to relax "
                "quantifier validation rules.",
                file=sys.stderr,
            )
        if args.debug:
            raise exn
        sys.exit(2)
    sys.exit(0)


def create_arg_parser():
    parser = ArgumentParser("miniwdl")
    parser.add_argument(
        "--version",
        nargs=0,
        action=PipVersionAction,
        help="show miniwdl package version information",
    )
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = "command"
    fill_common(fill_check_subparser(subparsers))
    fill_configure_subparser(subparsers)
    fill_common(fill_run_subparser(subparsers))
    fill_common(fill_run_self_test_subparser(subparsers))
    fill_common(fill_localize_subparser(subparsers))
    return parser


class PipVersionAction(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        from . import runtime

        miniwdl_version = pkg_version()
        if miniwdl_version:
            print(f"miniwdl v{miniwdl_version}")
        else:
            print("miniwdl version unknown")

        # show plugin versions
        # importlib_metadata doesn't seem to provide EntryPoint.dist to get from an entry point to
        # the metadata of the package providing it; continuing to use pkg_resources for this. Risk
        # that they give inconsistent results?
        import pkg_resources

        for group in runtime.config.default_plugins().keys():
            group = f"miniwdl.plugin.{group}"
            for plugin in pkg_resources.iter_entry_points(group=group):
                print(f"{group}\t{plugin}\t{plugin.dist}")
        sys.exit(0)


def fill_common(subparser):
    group = subparser.add_argument_group("language")
    group.add_argument(
        "-p",
        "--path",
        metavar="DIR",
        type=str,
        action="append",
        help="local directory to search for imports (can supply multiple times)",
    )
    group.add_argument(
        "--no-outside-imports",
        action="store_true",
        help="deny local imports from outside directory of main WDL file (or --path)",
    )
    group.add_argument(
        "--no-quant-check",
        dest="check_quant",
        action="store_false",
        help=(
            "relax static typechecking of optional types, and permit coercion of T to Array[T] (discouraged; for "
            "backwards compatibility with older WDL)"
        ),
    )
    group = subparser.add_argument_group("debugging")
    group.add_argument(
        "--debug", action="store_true", help="maximally verbose logging & exception tracebacks"
    )


def fill_check_subparser(subparsers):
    check_parser = subparsers.add_parser(
        "check",
        help="Validate a WDL document; show an outline with lint warnings",
        description="Load and typecheck a WDL document, showing an outline with lint warnings.\n\n"
        "Individual lint warnings can be suppressed by a WDL comment containing !WarningName on the\n"
        "same line or the following line, e.g.:\n"
        "    Int? foo = 42  # !UnnecessaryQuantifier\n"
        "    Int bar = foo + 1\n"
        "    # Lorem ipsum dolor sit (!OptionalCoercion)\n",
        formatter_class=RawDescriptionHelpFormatter,
    )
    check_parser.add_argument(
        "uri", metavar="WDL_URI", type=str, nargs="+", help="WDL document filename/URI"
    )
    check_parser.add_argument(
        "--strict",
        action="store_true",
        help="exit with nonzero status code if any lint warnings are shown (in addition to syntax and type errors)",
    )
    check_parser.add_argument(
        "--suppress",
        metavar="Warning1,Warning2",
        type=str,
        help="comma-separated set of warnings to disable globally e.g. StringCoercion,NonemptyCoercion",
    )
    check_parser.add_argument(
        "--no-suppress",
        dest="show_all",
        action="store_true",
        help="show warnings even if they have inline suppression comments",
    )
    check_parser.add_argument(
        # old option maintained for backwards-compatibility
        "--no-shellcheck",
        dest="shellcheck",
        action="store_false",
        help=SUPPRESS,
    )
    return check_parser


def check(
    uri=None,
    path=None,
    check_quant=True,
    strict=False,
    show_all=False,
    suppress=None,
    shellcheck=True,
    no_outside_imports=False,
    **kwargs,
):
    from . import Lint

    suppress = set(suppress.split(",")) if suppress else set()
    if not shellcheck:
        suppress.add("CommandShellCheck")

    # Load the document (read, parse, and typecheck)
    if "CommandShellCheck" in suppress:
        Lint._shellcheck_available = False

    shown = [0]
    for uri1 in uri or []:
        try:
            doc = load(
                uri1,
                path or [],
                check_quant=check_quant,
                read_source=make_read_source(no_outside_imports),
            )
        except (Error.SyntaxError, Error.ValidationError, Error.MultipleValidationErrors) as exn:
            if not getattr(exn, "declared_wdl_version", None):
                atexit.register(
                    lambda: print(
                        "* Hint: document should begin with WDL version declaration",
                        file=sys.stderr,
                    )
                )
            raise exn

        Lint.lint(doc)

        # Print an outline
        print(os.path.basename(uri1))
        outline(
            doc,
            0,
            show_called=(doc.workflow is not None),
            suppress=suppress,
            show_all=show_all,
            shown=shown,
        )

    if "CommandShellCheck" not in suppress and Lint._shellcheck_available is False:
        print(
            "* Suggestion: install shellcheck (www.shellcheck.net) to check task commands. (--suppress "
            "CommandShellCheck suppresses this message)",
            file=sys.stderr,
        )

    if strict and shown[0]:
        sys.exit(2)


def outline(
    obj, level, file=sys.stdout, show_called=True, suppress=None, show_all=False, shown=None
):
    # recursively pretty-print a brief outline of the workflow
    s = "".join(" " for i in range(level * 4))

    first_descent = []

    def descend(dobj=None, first_descent=first_descent):
        # show lint for the node just prior to first descent beneath it
        if not first_descent and hasattr(obj, "lint"):
            for (pos, cls, msg, suppressed) in sorted(obj.lint, key=lambda t: t[0]):
                if not (suppress and str(cls) in suppress) and (show_all or not suppressed):
                    print(
                        f"{s}    (Ln {pos.line}, Col {pos.column}) {cls}{' (suppressed)' if suppressed else ''}, {msg}",
                        file=file,
                    )
                    if shown:
                        shown[0] += 1
        first_descent.append(False)
        if dobj:
            outline(
                dobj,
                level + (1 if not isinstance(dobj, Decl) else 0),
                file=file,
                show_called=show_called,
                suppress=suppress,
                show_all=show_all,
                shown=shown,
            )

    # document
    if isinstance(obj, Document):
        # workflow
        if obj.workflow:
            descend(obj.workflow)
        # tasks
        for task in sorted(obj.tasks, key=lambda task: (not task.called, task.name)):
            descend(task)
        # imports
        for imp in sorted(obj.imports, key=lambda t: t.namespace):
            print("    {}{} : {}".format(s, imp.namespace, os.path.basename(imp.uri)), file=file)
            descend(imp.doc)
    # workflow
    elif isinstance(obj, Workflow):
        print(
            "{}workflow {}{}".format(
                s, obj.name, " (not called)" if show_called and not obj.called else ""
            ),
            file=file,
        )
        for elt in (obj.inputs or []) + obj.body + (obj.outputs or []):
            descend(elt)
    # task
    elif isinstance(obj, Task):
        print(
            "{}task {}{}".format(
                s, obj.name, " (not called)" if show_called and not obj.called else ""
            ),
            file=file,
        )
        for decl in (obj.inputs or []) + obj.postinputs + obj.outputs:
            descend(decl)
    # call
    elif isinstance(obj, Call):
        if obj.name != obj.callee_id[-1]:
            print("{}call {} as {}".format(s, ".".join(obj.callee_id), obj.name), file=file)
        else:
            print("{}call {}".format(s, ".".join(obj.callee_id)), file=file)
    # scatter
    elif isinstance(obj, Scatter):
        print("{}scatter {}".format(s, obj.variable), file=file)
        for elt in obj.body:
            descend(elt)
    # if
    elif isinstance(obj, Conditional):
        print("{}if".format(s), file=file)
        for elt in obj.body:
            descend(elt)
    # decl
    elif isinstance(obj, Decl):
        pass

    descend()


def print_error(exn):
    global quant_warning
    if isinstance(exn, Error.MultipleValidationErrors):
        for exn1 in exn.exceptions:
            print_error(exn1)
    else:
        if sys.stderr.isatty():
            sys.stderr.write(ANSI.BHRED)
        if isinstance(getattr(exn, "pos", None), SourcePosition):
            print(f"({exn.pos.uri} Ln {exn.pos.line} Col {exn.pos.column}) {exn}", file=sys.stderr)
        else:
            print(str(exn), file=sys.stderr)
        if sys.stderr.isatty():
            sys.stderr.write(ANSI.RESET)
        if isinstance(exn, Error.ImportError) and hasattr(exn, "__cause__"):
            print_error(exn.__cause__)
        if isinstance(exn, Error.ValidationError) and exn.source_text:
            # show source excerpt
            lines = exn.source_text.split("\n")
            error_line = lines[exn.pos.line - 1].replace("\t", " ")
            print("    " + error_line, file=sys.stderr)
            end_line = exn.pos.end_line
            end_column = exn.pos.end_column
            if end_line > exn.pos.line:
                end_line = exn.pos.line
                end_column = len(error_line) + 1
            while end_column > exn.pos.column + 1 and error_line[end_column - 2] == " ":
                end_column = end_column - 1
            print(
                "    " + " " * (exn.pos.column - 1) + "^" * (end_column - exn.pos.column),
                file=sys.stderr,
            )
            if isinstance(exn, Error.StaticTypeMismatch) and exn.actual.coerces(
                exn.expected, check_quant=False
            ):
                quant_warning = True


def make_read_source(no_outside_imports):
    top_dir = None

    async def read_source(uri, path, importer):
        from urllib import parse, request

        if uri.startswith("http:") or uri.startswith("https:"):
            with tempfile.TemporaryDirectory(prefix="miniwdl_import_uri_") as tmpdir:
                assert isinstance(tmpdir, str) and os.path.isdir(tmpdir)
                fn = os.path.join(
                    tmpdir,
                    os.path.basename(parse.urlsplit(uri).path),
                )
                request.urlretrieve(uri, filename=fn)
                with open(fn, "r") as infile:
                    return ReadSourceResult(infile.read(), uri)
        elif importer and (
            importer.pos.abspath.startswith("http:") or importer.pos.abspath.startswith("https:")
        ):
            assert not os.path.isabs(uri), "absolute import from downloaded WDL"
            return await read_source(parse.urljoin(importer.pos.abspath, uri), [], importer)
        ans = await read_source_default(uri, path, importer)
        if no_outside_imports:
            # Require all imported local WDL files to be in/under the directory of the main WDL
            # file (the first loaded), or one of the --path directoires.
            nonlocal top_dir
            if not top_dir:
                top_dir = os.path.dirname(ans.abspath)
            if not next(
                (p for p in ([top_dir] + path) if path_really_within(ans.abspath, p)), False
            ):
                raise PermissionError(
                    "denied import from outside main WDL file's directory; "
                    "strike --no-outside-imports or add to --path: " + os.path.dirname(ans.abspath)
                )
        return ans

    return read_source


def fill_run_subparser(subparsers):
    run_parser = subparsers.add_parser(
        "run",
        help="Run workflow/task locally with built-in runtime",
        description="For details & configuration see:\n"
        "https://miniwdl.readthedocs.io/en/latest/runner_reference.html",
    )
    run_parser.add_argument("uri", metavar="URI", type=str, help="WDL document filename/URI")
    run_parser.add_argument(
        "inputs",
        metavar="input_key=value",
        type=str,
        nargs="*",
        help="Workflow inputs. Optional space between = and value."
        " For arrays repeat, key=value1 key=value2 ...",
    ).completer = runner_input_completer
    group = run_parser.add_argument_group("input")
    group.add_argument(
        "-i",
        "--input",
        metavar="INPUT.json",
        dest="input_file",
        help="Cromwell-style input JSON object, filename, or -; command-line inputs will be merged in",
    )
    group.add_argument(
        "--empty",
        metavar="input_key",
        action="append",
        help="explicitly set a string input to the empty string OR an array input to the empty array",
    )
    group.add_argument(
        "--none",
        metavar="input_key",
        action="append",
        help="explicitly set an optional input to None (to override a default)",
    )
    group.add_argument(
        "--task",
        metavar="TASK_NAME",
        help="name of task to run (for WDL documents with multiple tasks & no workflow)",
    )
    group.add_argument(
        "-j",
        "--json",
        dest="json_only",
        action="store_true",
        help="just print Cromwell-style input JSON to standard output, then exit",
    )
    group = run_parser.add_argument_group("output")
    group.add_argument(
        "-d",
        "--dir",
        metavar="DIR",
        dest="run_dir",
        help=(
            "directory under which to create a timestamp-named subdirectory for this run (defaults to current "
            " working directory); supply '.' or 'some/dir/.' to instead run in this directory exactly"
        ),
    )
    group.add_argument(
        "--error-json",
        action="store_true",
        help="upon failure, print error information JSON to standard output (in addition to standard error logging)",
    )
    group.add_argument(
        "-o",
        metavar="OUT.json",
        dest="stdout_file",
        help="write JSON output/error to specified file instead of standard output (implies --error-json)",
    )
    group = run_parser.add_argument_group("logging")
    group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="increase logging detail & stream tasks' stderr",
    )
    group.add_argument(
        "--no-color",
        action="store_true",
        help="disable colored logging and status bar on terminal (also set by NO_COLOR environment variable)",
    )
    group.add_argument("--log-json", action="store_true", help="write all logs in JSON")
    group.add_argument("-e", metavar="ERR.json", dest="stderr_file", help=SUPPRESS)
    group = run_parser.add_argument_group("configuration")
    group.add_argument(
        "--cfg",
        metavar="FILE",
        type=str,
        default=None,
        help=(
            "configuration file to load (in preference to file named by MINIWDL_CFG environment, or "
            "XDG_CONFIG_{HOME,DIRS}/miniwdl.cfg)"
        ),
    )
    group.add_argument("-@", metavar="N", dest="max_tasks", type=int, default=None, help=SUPPRESS)
    group.add_argument("--runtime-cpu-max", metavar="N", type=int, default=None, help=SUPPRESS)
    group.add_argument("--runtime-memory-max", metavar="N", type=str, default=None, help=SUPPRESS)
    group.add_argument(
        "--runtime-defaults",
        metavar="JSON",
        type=str,
        default=None,
        help="""default runtime settings for all tasks (JSON filename or literal object e.g. '{"maxRetries":2}')""",
    )
    group.add_argument(
        "--no-cache",
        action="store_true",
        help="override any configuration enabling cache lookup for call outputs & downloaded files",
    )
    group.add_argument(
        "--env",
        action="append",
        metavar="VARNAME[=VALUE]",
        type=str,
        help="Environment variable to pass through to [or set outright in]"
        " all task environments (can supply multiple times; warning, non-portable side channel)",
    )
    group.add_argument(
        "--copy-input-files",
        action="store_true",
        help="copy input files for each task and mount them read/write (unblocks task commands that mv/rm/write them)",
    )
    group.add_argument(
        "--copy-input-files-for",
        action="append",
        metavar="TASK_NAME",
        type=str,
        help="copy input files only for specifically named task (can supply multiple times)",
    )
    group.add_argument(
        "--as-me",
        action="store_true",
        help=(
            "run all containers as the invoking user uid:gid (more secure, but potentially blocks task commands e.g. "
            "apt-get)"
        ),
    )
    # TODO:
    # way to specify None for an optional value (that has a default)
    return run_parser


def runner(
    uri,
    task=None,
    inputs=[],
    input_file=None,
    empty=[],
    none=[],
    json_only=False,
    run_dir=None,
    path=None,
    check_quant=True,
    cfg=None,
    runtime_cpu_max=None,
    runtime_memory_max=None,
    env=[],
    runtime_defaults=None,
    max_tasks=None,
    copy_input_files=False,
    copy_input_files_for=[],
    as_me=False,
    no_cache=False,
    error_json=False,
    log_json=False,
    stdout_file=None,
    stderr_file=None,
    no_outside_imports=False,
    **kwargs,
):
    # set up logging
    level = NOTICE_LEVEL
    if kwargs["verbose"]:
        level = VERBOSE_LEVEL
    if kwargs["debug"]:
        level = logging.DEBUG
    else:
        logging.raiseExceptions = False
    if kwargs["no_color"]:
        # picked up by _util.configure_logger()
        os.environ["NO_COLOR"] = os.environ.get("NO_COLOR", "")
    # log_json setting only comes from command line or environment (not cfg file), because we
    # need it before loading configuration
    log_json = log_json or (
        os.environ.get("MINIWDL__LOGGING__JSON", "").lower().strip()
        in ("t", "y", "1", "true", "yes")
    )
    logging.basicConfig(level=level)
    logger = logging.getLogger("miniwdl-run")

    with ExitStack() as cleanup:
        if stderr_file:
            cleanup.enter_context(
                LoggingFileHandler(logging.getLogger(), stderr_file, json=log_json)
            )
        set_status = cleanup.enter_context(configure_logger(json=log_json))

        # load configuration & apply command-line overrides
        from . import runtime

        cfg_arg = None
        if cfg:
            assert os.path.isfile(cfg), "--cfg file not found"
            cfg_arg = [cfg]
        cfg = runtime.config.Loader(logger, filenames=cfg_arg)
        cfg_overrides = {
            "scheduler": {},
            "file_io": {},
            "task_runtime": {},
            "download_cache": {},
            "call_cache": {},
            "logging": {"json": log_json},
        }
        if max_tasks is not None:
            cfg_overrides["scheduler"]["call_concurrency"] = max_tasks
        if copy_input_files:
            cfg_overrides["file_io"]["copy_input_files"] = copy_input_files
        if copy_input_files_for:
            cfg_overrides["file_io"]["copy_input_files_for"] = copy_input_files_for
        if as_me:
            cfg_overrides["task_runtime"]["as_user"] = as_me
        if runtime_defaults:
            if runtime_defaults.lstrip()[0] == "{":
                json.loads(runtime_defaults)
                cfg_overrides["task_runtime"]["defaults"] = runtime_defaults
            else:
                with open(runtime_defaults, "r") as infile:
                    cfg_overrides["task_runtime"]["defaults"] = infile.read()
        if runtime_cpu_max is not None:
            cfg_overrides["task_runtime"]["cpu_max"] = runtime_cpu_max
        if env:
            cfg_overrides["task_runtime"]["env"] = runner_env_override(cfg, env)
        if runtime_memory_max is not None:
            runtime_memory_max = (
                -1 if runtime_memory_max.strip() == "-1" else parse_byte_size(runtime_memory_max)
            )
            cfg_overrides["task_runtime"]["memory_max"] = runtime_memory_max
        if no_cache:
            cfg_overrides["download_cache"]["get"] = False
            cfg_overrides["call_cache"]["get"] = False

        cfg.override(cfg_overrides)
        cfg.log_all()
        if os.geteuid() == 0 and not currently_in_container():
            logger.warning("running miniwdl as root is usually avoidable (see docs)")
        if cfg["task_runtime"].get_dict("env"):
            logger.warning(
                "--env is a non-standard side channel; relying on it is probably not portable"
            )

        # check root
        if not path_really_within((run_dir or os.getcwd()), cfg["file_io"]["root"]):
            logger.error(
                _(
                    "working directory or --dir must be within the configured `file_io.root' directory",
                    dir=(run_dir or os.getcwd()),
                    root=cfg["file_io"]["root"],
                )
            )
            sys.exit(2)
        if (
            cfg["download_cache"].get_bool("get") or cfg["download_cache"].get_bool("put")
        ) and not path_really_within(cfg["download_cache"]["dir"], cfg["file_io"]["root"]):
            logger.error(
                _(
                    "configuration error: 'download_cache.dir' must be within the `file_io.root' directory",
                    dir=cfg["download_cache"]["dir"],
                    root=cfg["file_io"]["root"],
                )
            )
            sys.exit(2)

        try:
            # load WDL document
            doc = load(
                uri,
                path or [],
                check_quant=check_quant,
                read_source=make_read_source(no_outside_imports),
            )

            # parse and validate the provided inputs
            eff_root = (
                cfg["file_io"]["root"] if not cfg["file_io"].get_bool("copy_input_files") else "/"
            )

            target, input_env, input_json = runner_input(
                doc,
                inputs,
                input_file,
                empty,
                none,
                task=task,
                downloadable=lambda fn, is_dir: runtime.download.able(cfg, fn, directory=is_dir),
                root=eff_root,  # if copy_input_files is set, then input files need not reside under the configured root
            )
        except Error.InputError as exn:
            runner_standard_output(runtime.error_json(exn), stdout_file, error_json, log_json)
            die(exn.args[0])
        except Exception as exn:
            runner_standard_output(runtime.error_json(exn), stdout_file, error_json, log_json)
            raise

        if json_only:
            print(json.dumps(input_json, indent=(None if log_json else 2)))
            sys.exit(0)

        # debug logging
        versionlog = {"python": sys.version, "uname": " ".join(os.uname())}
        for pkg in ["miniwdl", "docker", "lark", "argcomplete", "pygtail"]:
            pkver = pkg_version(pkg)
            versionlog[pkg] = str(pkver) if pkver else "UNKNOWN"
        logger.debug(_("package versions", **versionlog))

        envlog = {}
        for k in os.environ:
            if k.upper().startswith("MINIWDL") or k in [
                "LANG",
                "SHELL",
                "USER",
                "HOME",
                "PWD",
                "TMPDIR",
            ]:
                envlog[k] = os.environ[k]
        logger.debug(_("environment", **envlog))

        enabled_plugins = []
        disabled_plugins = []
        for group in runtime.config.default_plugins().keys():
            for enabled, plugin in runtime.config.load_all_plugins(cfg, group):
                (enabled_plugins if enabled else disabled_plugins).append(
                    f"{plugin.name} = {plugin.value}"
                )
        if enabled_plugins or disabled_plugins:
            logger.debug(
                _("plugin configuration", enabled=enabled_plugins, disabled=disabled_plugins)
            )

        rerun_sh = f"pushd {shellquote(os.getcwd())} && miniwdl {' '.join(shellquote(t) for t in sys.argv[1:])}; popd"

        # run & log any errors
        cleanup.enter_context(runtime._statusbar.enable(set_status))
        cache = cleanup.enter_context(runtime.cache.new(cfg, logger))
        rundir = None
        try:
            rundir, output_env = runtime.run(cfg, target, input_env, run_dir=run_dir, _cache=cache)
        except Exception as exn:
            runner_standard_output(runtime.error_json(exn), stdout_file, error_json, log_json)
            exit_status = 2
            from_rundir = None
            while isinstance(exn, runtime.RunFailed):
                exn_rundir = getattr(exn, "run_dir")
                rundir = rundir or exn_rundir
                from_rundir = exn_rundir
                exn = exn.__cause__
            if isinstance(exn, runtime.CommandFailed):
                exit_status = (lambda v: v if v else exit_status)(getattr(exn, "exit_status", 0))
                if not (kwargs["verbose"] or kwargs["debug"]):
                    logger.notice(
                        "run with --verbose to include task standard error streams in this log"
                    )
            info = runtime.error_json(exn)
            if rundir:
                info["dir"] = rundir
            if from_rundir and from_rundir != rundir:
                info["from_dir"] = from_rundir
            msg = str(exn)
            if "message" in info:
                msg = info["message"]
                del info["message"]
            logger.error(_(msg, **info))
            if isinstance(exn, AssertionError) or kwargs["debug"]:
                raise
            sys.exit(exit_status)
        finally:
            if rundir:
                # whether success or fail, leave some artifacts in the run directory.
                # this should be done under the flock held open within the cache context so that
                # other waiting processes know when we're really finished with the run directory.
                with open(os.path.join(rundir, "rerun"), "w") as rerunfile:
                    print(rerun_sh, file=rerunfile)
                copy_source(doc, os.path.join(rundir, "wdl"))
            cfg.log_unused_options()

    # report
    outputs_json = {"outputs": values_to_json(output_env, namespace=target.name), "dir": rundir}
    runner_standard_output(outputs_json, stdout_file, error_json, log_json)
    return outputs_json


def runner_input_completer(prefix, parsed_args, **kwargs):
    # argcomplete completer for `miniwdl run` and `miniwdl cromwell`
    if "uri" in parsed_args:
        # load document. in the completer setting, we need to substitute the home directory
        # and environment variables
        uri = os.path.expandvars(os.path.expanduser(parsed_args.uri))
        if not (uri.startswith("http:") or uri.startswith("https:") or os.path.exists(uri)):
            argcomplete.warn("file not found: " + uri)
            return []
        try:
            doc = load(
                uri,
                path=(parsed_args.path if hasattr(parsed_args, "path") else []),
                check_quant=parsed_args.check_quant,
                read_source=make_read_source(
                    parsed_args.no_outside_imports
                    if hasattr(parsed_args, "no_outside_imports")
                    else False
                ),
            )
        except Exception as exn:
            argcomplete.warn(
                "unable to load {}; try 'miniwdl check' on it ({})".format(uri, str(exn))
            )
            return []
        # resolve target
        if parsed_args.task:
            target = next((t for t in doc.tasks if t.name == parsed_args.task), None)
            if not target:
                argcomplete.warn(f"no such task {parsed_args.task} in document")
                return []
        elif doc.workflow:
            target = doc.workflow
        elif len(doc.tasks) == 1:
            target = doc.tasks[0]
        elif len(doc.tasks) > 1:
            argcomplete.warn("specify --task for WDL document with multiple tasks and no workflow")
            return []
        else:
            argcomplete.warn("Empty WDL document")
            return []
        assert target
        # figure the available input names (starting with prefix, if any)
        completed_input_names = [nm + "=" for nm in values_to_json(target.required_inputs)]
        if prefix and prefix.find("=") == -1:
            completed_input_names = [nm for nm in completed_input_names if nm.startswith(prefix)]
            if not completed_input_names:
                # suggest optional inputs only if nothing else matches prefix
                completed_input_names = [
                    nm + "="
                    for nm in values_to_json(target.available_inputs)
                    if nm.startswith(prefix)
                ]
        return completed_input_names


def runner_input(
    doc,
    inputs,
    input_file,
    empty,
    none,
    task=None,
    check_required=True,
    downloadable=None,
    root="/",
):
    """
    - Determine the target workflow/task
    - Check types of supplied inputs
    - Check all required inputs are supplied
    - Return inputs as Env.Bindings[Value.Base]
    """

    # resolve target
    target = None
    if task:
        target = next((t for t in doc.tasks if t.name == task), None)
        if not target:
            raise Error.InputError(f"no such task {task} in document")
    elif doc.workflow:
        target = doc.workflow
    elif len(doc.tasks) == 1:
        target = doc.tasks[0]
    elif len(doc.tasks) > 1:
        raise Error.InputError(
            "specify --task for WDL document with multiple tasks and no workflow"
        )
    else:
        raise Error.InputError("Empty WDL document")
    assert target

    # build up an values env of the provided inputs
    available_inputs = target.available_inputs
    input_env = runner_input_json_file(
        available_inputs,
        (target.name if isinstance(target, Workflow) else ""),
        input_file,
        downloadable,
        root,
    )

    # set explicitly empty arrays or strings
    for empty_name in empty or []:
        try:
            decl = available_inputs[empty_name]
        except KeyError:
            runner_input_help(target)
            raise Error.InputError(f"No such input to {target.name}: {empty_name}")
        if isinstance(decl.type, Type.Array):
            if decl.type.nonempty:
                raise Error.InputError(
                    f"Cannot set input {str(decl.type)} {decl.name} to empty array"
                )
            input_env = input_env.bind(empty_name, Value.Array(decl.type.item_type, []), decl)
        elif isinstance(decl.type, Type.String):
            input_env = input_env.bind(empty_name, Value.String(""), decl)
        else:
            msg = f"Cannot set {str(decl.type)} {decl.name} to empty array or string"
            if decl.type.optional:
                msg += "; perhaps you want --none " + decl.name
            raise Error.InputError(msg)

    # set explicitly None values
    for none_name in none or []:
        try:
            decl = available_inputs[none_name]
        except KeyError:
            runner_input_help(target)
            raise Error.InputError(f"No such input to {target.name}: {none_name}")
        if not decl.type.optional:
            raise Error.InputError(
                f"Cannot set non-optional input {str(decl.type)} {decl.name} to None"
            )
        input_env = input_env.bind(none_name, Value.Null(), decl)

    # preprocess command-line inputs: merge adjacent elements ("x=", "y") into ("x=y"), allowing
    # shell filename completion on y
    inputs = list(inputs)
    i = 0
    while i < len(inputs):
        len_i = len(inputs[i])
        if len_i > 1 and inputs[i].find("=") == len_i - 1 and i + 1 < len(inputs):
            inputs[i] = inputs[i] + inputs[i + 1]
            del inputs[i + 1]
        i += 1

    # add in command-line inputs
    for one_input in inputs:
        # parse [namespace], name, and value
        buf = one_input.split("=", 1)
        if not one_input or not one_input[0].isalpha() or len(buf) != 2 or not buf[0]:
            runner_input_help(target)
            raise Error.InputError("Invalid input name=value pair: " + one_input)
        name, s_value = buf

        # find corresponding input declaration
        decl = available_inputs.get(name)

        if not decl:
            # allow arbitrary runtime overrides
            nmparts = name.split(".")
            runtime_idx = next((i for i, term in enumerate(nmparts) if term in ("runtime",)), -1)
            if runtime_idx >= 0 and len(nmparts) > (runtime_idx + 1):
                decl = available_inputs.get(".".join(nmparts[:runtime_idx] + ["_runtime"]))

        if not decl:
            runner_input_help(target)
            raise Error.InputError(f"No such input to {target.name}: {buf[0]}")

        # create a Value based on the expected type
        v = runner_input_value(s_value, decl.type, downloadable, root)

        # insert value into input_env
        existing = input_env.get(name)
        if existing:
            if isinstance(v, Value.Array):
                assert isinstance(existing, Value.Array) and v.type.coerces(existing.type)
                existing.value.extend(v.value)
            else:
                runner_input_help(target)
                raise Error.InputError(f"non-array input {buf[0]} duplicated")
        else:
            input_env = input_env.bind(name, v, decl)

    # check for missing inputs
    if check_required:
        missing_inputs = values_to_json(target.required_inputs.subtract(input_env))
        if missing_inputs:
            runner_input_help(target)
            raise Error.InputError(
                f"missing required inputs for {target.name}: {', '.join(missing_inputs.keys())}"
            )

    # make a pass over the Env to create a dict for Cromwell-style input JSON
    return (
        target,
        input_env,
        values_to_json(input_env, namespace=(target.name if isinstance(target, Workflow) else "")),
    )


def runner_input_json_file(available_inputs, namespace, input_file, downloadable, root):
    """
    Load user-supplied inputs JSON file, if any
    """
    ans = Env.Bindings()

    if input_file:
        input_file = input_file.strip()
    if input_file:
        import yaml  # delayed heavy import

        input_json = None
        if input_file[0] == "{":
            input_json = input_file
        elif input_file == "-":
            input_json = sys.stdin.read()
        else:
            input_json = (
                asyncio.get_event_loop()
                .run_until_complete(make_read_source(False)(input_file, [], None))
                .source_text
            )
        input_json = yaml.safe_load(input_json)
        if not isinstance(input_json, dict):
            raise Error.InputError("check JSON input; expected top-level object")
        try:
            ans = values_from_json(input_json, available_inputs, namespace=namespace)
        except Error.InputError as exn:
            raise Error.InputError("check JSON input; " + exn.args[0])

        ans = Value.rewrite_env_paths(
            ans,
            lambda v: validate_input_path(
                v.value, isinstance(v, Value.Directory), downloadable, root
            ),
        )

    return ans


def runner_input_help(target):
    def bold(line):
        if sys.stderr.isatty():
            return f"{ANSI.BOLD}{line}{ANSI.RESET}"
        return line

    ans = [
        "",
        bold(f"{target.name} ({target.pos.uri})"),
        bold(f"{'-'*(len(target.name)+len(target.pos.uri)+3)}"),
    ]
    required_inputs = target.required_inputs
    ans.append(bold("\nrequired inputs:"))
    for b in required_inputs:
        ans.append(bold(f"  {str(b.value.type)} {b.name}"))
        add_wrapped_parameter_meta(target, b.name, ans)
    optional_inputs = target.available_inputs.subtract(target.required_inputs)
    optional_inputs = optional_inputs.filter(lambda b: not b.value.name.startswith("_"))
    if target.inputs is None:
        # if the target doesn't have an input{} section (pre WDL 1.0), exclude
        # declarations bound to a non-constant expression (heuristic)
        optional_inputs = optional_inputs.filter(
            lambda b: b.value.expr is None or is_constant_expr(b.value.expr)
        )
    if optional_inputs:
        ans.append(bold("\noptional inputs:"))
        for b in optional_inputs:
            d = bold(f"  {str(b.value.type)} {b.name}")
            if b.value.expr:
                ans.append(f"{d} = {b.value.expr}")
            else:
                ans.append(d)
            add_wrapped_parameter_meta(target, b.name, ans)
    ans.append(bold("\noutputs:"))
    for b in target.effective_outputs:
        ans.append(bold(f"  {str(b.value)} {b.name}"))
    for line in ans:
        print(line, file=sys.stderr)


def runner_env_override(cfg, args):
    env_override = cfg["task_runtime"].get_dict("env")
    for item in args:
        sep = item.find("=")
        if sep == 0:
            raise Error.InputError("invalid --env argument: " + item)
        name = item[: sep if sep >= 0 else len(item)]
        value = None
        if sep != -1:
            value = item[sep + 1 :]
        env_override[name] = value
    return env_override


def is_constant_expr(expr):
    """
    Decide if the expression is "constant" for the above purposes
    """
    if isinstance(expr, (Expr.Int, Expr.Float, Expr.Boolean)):
        return True
    if isinstance(expr, Expr.String) and (
        len(expr.parts) == 2 or (len(expr.parts) == 3 and isinstance(expr.parts[1], str))
    ):
        return True
    if isinstance(expr, Expr.Array):
        return not [item for item in expr.items if not is_constant_expr(item)]
    # TODO: Pair, Map, Struct???
    return False


def add_wrapped_parameter_meta(target, input_name, output_list):
    ans = ""
    if input_name in target.parameter_meta:
        entry = target.parameter_meta[input_name]
        if isinstance(entry, str):
            ans = entry
        elif isinstance(entry, dict) and isinstance(entry.get("help", None), str):
            ans = entry["help"]
    if ans:
        output_list.extend((" " * 4 + line) for line in textwrap.wrap(ans, 96))


def runner_input_value(s_value, ty, downloadable, root):
    """
    Given an input value from the command line (right-hand side of =) and the
    WDL type of the corresponding input decl, create an appropriate Value.
    """
    if isinstance(ty, Type.String):
        return Value.String(s_value)
    if isinstance(ty, (Type.File, Type.Directory)):
        # check existence and absolutify path
        directory = isinstance(ty, Type.Directory)
        s_value = validate_input_path(os.path.expanduser(s_value), directory, downloadable, root)
        return Value.Directory(s_value) if directory else Value.File(s_value)
    if isinstance(ty, Type.Boolean):
        if s_value == "true":
            return Value.Boolean(True)
        if s_value == "false":
            return Value.Boolean(False)
        raise Error.InputError(
            "Boolean input should be true or false instead of `{}'".format(s_value)
        )
    if isinstance(ty, Type.Int):
        return Value.Int(int(s_value))
    if isinstance(ty, Type.Float):
        return Value.Float(float(s_value))
    if isinstance(ty, Type.Array) and isinstance(
        ty.item_type, (Type.String, Type.File, Type.Int, Type.Float)
    ):
        # just produce a length-1 array, to be combined ex post facto
        return Value.Array(
            ty.item_type, [runner_input_value(s_value, ty.item_type, downloadable, root)]
        )
    if isinstance(ty, Type.Any):
        # infer dynamically-typed runtime overrides
        try:
            return Value.Int(int(s_value))
        except ValueError:
            pass
        try:
            return Value.Float(float(s_value))
        except ValueError:
            pass
        return Value.String(s_value)
    raise Error.InputError(
        "No command-line support yet for inputs of type {}; workaround: specify in JSON file with --input".format(
            str(ty)
        )
    )


def validate_input_path(path, directory, downloadable, root):
    """
    If the path is downloadable, return it back. Otherwise, return the absolute path after checking
    1. exists and is a file or directory (according to directory: bool)
    2. resides within root
    3. contains no symlinks pointing outside or to absolute paths
    """
    if downloadable and downloadable(path, directory):
        return path

    if not ((directory and os.path.isdir(path)) or (not directory and os.path.isfile(path))):
        raise Error.InputError(("Directory" if directory else "File") + " not found: " + path)

    path = os.path.abspath(path)

    if not path_really_within(path, root):
        raise Error.InputError(
            f"File & Directory inputs must be located within the configured `file_io.root' directory `{root}' "
            f"unlike `{path}'"
        )

    if directory:

        def raiser(exc: OSError):
            raise exc

        for root, subdirs, files in os.walk(path, onerror=raiser, followlinks=False):
            for fn in files:
                fn = os.path.join(root, fn)
                if os.path.islink(fn) and (
                    not os.path.exists(fn)
                    or os.path.isabs(os.readlink(fn))
                    or not path_really_within(fn, path)
                ):
                    raise Error.InputError("Input Directory contains unusable symlink: " + path)

    return path


def runner_standard_output(content, stdout_file, error_json, log_json):
    """
    Write the runner output/error JSON in the way requested by the user
    """
    if error_json or stdout_file or "error" not in content:
        content_json = json.dumps(content, indent=(None if log_json else 2))
        if stdout_file:
            with open(stdout_file, "w") as outfile:
                print(content_json, file=outfile)
        else:
            print(content_json)


def fill_run_self_test_subparser(subparsers):
    run_parser = subparsers.add_parser(
        "run_self_test", help="Run a short built-in workflow to test system configuration"
    )
    run_parser.add_argument(
        "--dir",
        metavar="DIR",
        default=None,
        help="run the test in specified directory, instead of some new temporary directory",
    )
    run_parser.add_argument(
        "--cfg",
        metavar="FILE",
        type=str,
        default=None,
        help=(
            "configuration file to load (in preference to file named by MINIWDL_CFG environment, "
            "or XDG_CONFIG_{HOME,DIRS}/miniwdl.cfg)"
        ),
    )
    run_parser.add_argument("--log-json", action="store_true", help="write all logs in JSON")
    run_parser.add_argument(
        "--as-me", action="store_true", help="run all containers as the current user uid:gid"
    )
    return run_parser


def run_self_test(**kwargs):
    dn = kwargs["dir"]
    if dn:
        os.makedirs(dn, exist_ok=True)
    else:
        dn = tempfile.mkdtemp(prefix="miniwdl_run_self_test_")
    with open(os.path.join(dn, "test.wdl"), "w") as outfile:
        outfile.write(
            r"""
            version 1.0
            workflow hello_caller {
                input {
                    File who
                }
                scatter (name in read_lines(who)) {
                    call hello {
                        input:
                            who = write_lines([name])
                    }
                    if (defined(hello.message)) {
                        String msg = read_string(select_first([hello.message]))
                    }
                }
                output {
                    Array[String] messages = select_all(msg)
                }
            }
            task hello {
                input {
                    File who
                }
                command {
                    if grep -qv ^\# "${who}" ; then
                        echo "Hello, $(cat ${who})!" | tee message.txt 1>&2
                    fi
                }
                output {
                    File? message = "message.txt"
                }
                runtime {
                    docker: "ubuntu:18.04"
                    memory: "1G"
                }
            }
            """
        )

    check(uri=[os.path.join(dn, "test.wdl")])

    argv = [
        "run",
        os.path.join(dn, "test.wdl"),
        "who=https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/tests/alyssa_ben.txt",
        "--dir",
        dn if dn not in [".", "./"] else os.getcwd(),
        "--no-cache",
        "--debug",
        "-e",
        os.path.join(dn, "miniwdl_run_self_test.log"),
    ]
    if kwargs["as_me"]:
        argv.append("--as-me")
    if kwargs["cfg"]:
        argv.append("--cfg")
        argv.append(kwargs["cfg"])
    if kwargs["log_json"]:
        argv.append("--log-json")
    try:
        outputs = main(argv)["outputs"]  # pylint: disable=E1136
        assert len(outputs["hello_caller.messages"]) == 2
        assert outputs["hello_caller.messages"][0].rstrip() == "Hello, Alyssa P. Hacker!"
        assert outputs["hello_caller.messages"][1].rstrip() == "Hello, Ben Bitdiddle!"
    except BaseException as exn:
        if not (isinstance(exn, SystemExit) and getattr(exn, "code") == 0):
            atexit.register(
                lambda: print(
                    "* Hint: ensure Docker is installed & running"
                    + (
                        ", and user has permission to control it per\n"
                        "  https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user"
                        if platform.system() != "Darwin"
                        else "; and on macOS override the environment variable TMPDIR=/tmp/"
                    )
                    + "\n* To request help at https://github.com/chanzuckerberg/miniwdl/issues\n"
                    "  attach the log file " + os.path.join(dn, "miniwdl_run_self_test.log"),
                    file=sys.stderr,
                )
            )
            raise exn

    print(
        "\nminiwdl run_self_test OK; try `miniwdl configure` to set common options or show current selections.",
        file=sys.stderr,
    )
    if os.geteuid() == 0:
        print(
            "* Hint: non-root users should be able to run miniwdl if they have permission to control Docker per\n"
            "        https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user",
            file=sys.stderr,
        )


def fill_localize_subparser(subparsers):
    localize_parser = subparsers.add_parser(
        "localize",
        help="Download URI input Files to local cache for use in subsequent runs",
        description="Prime the local download cache with URI File/Directory inputs found in Cromwell-style input JSON. "
        "This is only needed if it's useful to perform downloads in advance rather than on next run start.",
    )
    localize_parser.add_argument(
        "wdlfile",
        metavar="DOC.wdl",
        type=str,
        help="WDL document filename/URI",
        default=None,
        nargs="?",
    )
    localize_parser.add_argument(
        "infile",
        metavar="INPUT.json",
        type=str,
        help="input JSON filename (- for standard input) or literal object",
        default=None,
        nargs="?",
    )
    localize_parser.add_argument(
        "--task",
        metavar="TASK_NAME",
        help="name of task (for WDL documents with multiple tasks & no workflow)",
    )
    localize_parser.add_argument(
        "--file",
        metavar="URI",
        action="append",
        help="additional File URI to process; if present then WDL & JSON may be omitted",
    )
    localize_parser.add_argument(
        "--directory",
        metavar="URI",
        action="append",
        help="additional Directory URI to process; if present then WDL & JSON may be omitted",
    )
    localize_parser.add_argument(
        "--uri",
        metavar="URI",
        action="append",
        dest="file",
        help=SUPPRESS,  # vestigial, before splitting --file/--directory
    )
    localize_parser.add_argument(
        "--no-cache",
        action="store_true",
        help="if a URI is already cached, re-download and replace it",
    )
    localize_parser.add_argument(
        "--cfg",
        metavar="FILE",
        type=str,
        default=None,
        help=(
            "configuration file to load (in preference to file named by MINIWDL_CFG environment, "
            "or XDG_CONFIG_{HOME,DIRS}/miniwdl.cfg)"
        ),
    )
    group = localize_parser.add_argument_group("logging")
    group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="increase logging detail & stream tasks' stderr",
    )
    group.add_argument(
        "--no-color",
        action="store_true",
        help="disable colored logging and status bar on terminal (also set by NO_COLOR environment variable)",
    )
    group.add_argument("--log-json", action="store_true", help="write all logs in JSON")
    return localize_parser


def localize(
    wdlfile=None,
    infile=None,
    file=None,
    directory=None,
    no_cache=False,
    task=None,
    cfg=None,
    path=None,
    check_quant=True,
    no_outside_imports=False,
    **kwargs,
):
    # set up logging
    level = NOTICE_LEVEL
    logging.raiseExceptions = False
    if kwargs["verbose"]:
        level = VERBOSE_LEVEL
    if kwargs["debug"]:
        level = logging.DEBUG
    if kwargs["no_color"]:
        os.environ["NO_COLOR"] = os.environ.get("NO_COLOR", "")
    log_json = kwargs["log_json"] or (
        os.environ.get("MINIWDL__LOGGING__JSON", "").lower().strip()
        in ("t", "y", "1", "true", "yes")
    )
    logging.basicConfig(level=level)
    logger = logging.getLogger("miniwdl-localize")
    with configure_logger(json=log_json) as set_status:
        from . import runtime

        cfg_arg = None
        if cfg:
            assert os.path.isfile(cfg), "--cfg file not found"
            cfg_arg = [cfg]
        cfg = runtime.config.Loader(logger, filenames=cfg_arg)
        cache_cfg = cfg["download_cache"]
        original_get = cache_cfg.get_bool("get")
        if original_get and no_cache:
            cfg.override({"download_cache": {"get": False}})
        logger.notice(
            _(
                "effective configuration",
                put=cache_cfg.get_bool("put"),
                get=cache_cfg.get_bool("get"),
                dir=cache_cfg["dir"],
                ignore_query=cache_cfg.get_bool("ignore_query"),
                enable_patterns=cache_cfg.get_list("enable_patterns"),
                disable_patterns=cache_cfg.get_list("disable_patterns"),
            )
        )

        file = set(file or [])
        directory = set(directory or [])

        if infile:
            # load WDL document
            doc = load(
                wdlfile,
                path or [],
                check_quant=check_quant,
                read_source=make_read_source(no_outside_imports),
            )

            try:
                target, input_env, input_json = runner_input(
                    doc,
                    [],
                    infile,
                    [],
                    [],
                    task=task,
                    check_required=False,
                    downloadable=lambda fn, is_dir: runtime.download.able(
                        cfg, fn, directory=is_dir
                    ),
                )
            except Error.InputError as exn:
                die(exn.args[0])

            # scan inputs for donwloadable URIs that appear to be downloadable URIs
            def scan(v):
                is_directory = isinstance(v, Value.Directory)
                if runtime.download.able(cfg, v.value, directory=is_directory):
                    (directory if is_directory else file).add(v.value)
                return v.value

            Value.rewrite_env_paths(input_env, scan)

        if not (file or directory):
            logger.warning(
                "nothing to do; if inputs use special URI schemes, make sure necessary downloader plugin(s) are "
                "installed and enabled"
            )
            sys.exit(0)

        if not cache_cfg.get_bool("put"):
            logger.error(
                'configuration section "download_cache", option "put" (env MINIWDL__DOWNLOAD_CACHE__PUT) must be true '
                "for this operation to be effective"
            )
            sys.exit(2)

        if not path_really_within(cfg["download_cache"]["dir"], cfg["file_io"]["root"]):
            logger.error(
                _(
                    "configuration error: `download_cache.dir' must be within the `file_io.root' directory",
                    dir=cfg["download_cache"]["dir"],
                    root=cfg["file_io"]["root"],
                )
            )
            sys.exit(2)

        with runtime.cache.CallCache(cfg, logger) as cache:
            disabled_files = set(u for u in file if not cache.download_path(u))
            disabled_dirs = set(u for u in directory if not cache.download_path(u, directory=True))
        if disabled_files or disabled_dirs:
            logger.notice(
                _(
                    "URIs found but not cacheable per configuration",
                    uri=list(disabled_files | disabled_dirs),
                )
            )
        file = list(file - disabled_files)
        directory = list(directory - disabled_dirs)

        if not (file or directory):
            logger.warning("nothing to do; check configured enable_patterns and disable_patterns")
            sys.exit(0)
        logger.notice(_("starting downloads", files=file, directories=directory))

        # cheesy trick: provide the list of URIs as File inputs to a dummy workflow, causing the
        # runtime to download & cache them
        localizer_wdl = """
            version development
            workflow localize {
                input {
                    Array[File] files
                    Array[Directory] directories
                }
                output {
                    Array[File] downloaded_files = files
                    Array[Directory] downloaded_directories = directories
                }
            }
            """
        localizer = parse_document(localizer_wdl)
        localizer.typecheck()
        cfg = runtime.config.Loader(logger)
        subdir, outputs = runtime.run(
            cfg,
            localizer.workflow,
            values_from_json(
                {"files": file, "directories": directory}, localizer.workflow.available_inputs
            ),
            run_dir=os.environ.get("TMPDIR", "/tmp"),
        )
        outputs = values_to_json(outputs)

        if not original_get:
            logger.warning(
                """future runs won't use the cache unless configuration section "download_cache", key "get" """
                """(env MINIWDL__DOWNLOAD_CACHE__GET) is set to true"""
            )


def fill_configure_subparser(subparsers):
    configure_parser = subparsers.add_parser(
        "configure",
        help="Generate runner config file / display effective config",
        description="Generate a config file for `miniwdl run`; if it already exists, display effective config",
    )
    configure_parser.add_argument(
        "cfg",
        metavar="FILE",
        type=str,
        nargs="?",
        default=None,
        help="existing or to-be-created config file location; default XDG_CONFIG_HOME/miniwdl.cfg",
    )
    configure_parser.add_argument(
        "--show", action="store_true", help="just show effective configuration"
    )
    configure_parser.add_argument(
        "--force", action="store_true", help="overwrite existing .cfg file"
    )
    return configure_parser


def configure(cfg=None, show=False, force=False, **kwargs):
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        die("`miniwdl configure` is for interactive use")

    from datetime import datetime
    import bullet
    from xdg import XDG_CONFIG_HOME

    miniwdl_version = pkg_version()
    if miniwdl_version:
        miniwdl_version = "v" + miniwdl_version

    logging.raiseExceptions = False
    logging.basicConfig(level=VERBOSE_LEVEL)
    logger = logging.getLogger("miniwdl-configure")
    with configure_logger() as set_status:
        if (show or not force) and configure_existing(logger, cfg, always=show):
            sys.exit(0)

        if not cfg:
            cfg = os.path.join(XDG_CONFIG_HOME, "miniwdl.cfg")

        def yes(prompt):
            return bullet.Bullet(prompt=prompt, choices=["No", "Yes"]).launch() == "Yes"

        if os.path.exists(cfg):
            assert force
            logger.warn("Proceeding will overwrite existing configuration file at " + cfg)
            sys.stderr.flush()
            if not yes("OVERWRITE?"):
                sys.exit(0)
            os.unlink(cfg)
        logger.notice("Generating configuration file at " + cfg)
        sys.stderr.flush()

        options = {}
        try:
            print(
                textwrap.dedent(
                    """
                    CALL CACHE: upon task/workflow success, store a copy of JSON output in a central directory where it
                    can be reused for subsequent runs of the same WDL & input. The JSON files reference input & output
                    files at their original locations, and invalidate automatically if any such file is deleted or
                    changed (mtime).
                    """
                )
            )
            if yes("ENABLE?"):
                options["call_cache"] = {"get": "true", "put": "true"}
                print("\nCall cache JSON file storage directory: ~/.cache/miniwdl/")

                if yes("OVERRIDE?"):
                    options["call_cache"]["dir"] = bullet.Input(
                        prompt="Call cache directory: ", strip=True
                    ).launch()

            print(
                textwrap.dedent(
                    """
                    DOWNLOAD CACHE: upon downloading a File or Directory input URI (https:// s3:// etc.), store it in a
                    central directory where it can be reused for subsequent run inputs with the same URI (even if the
                    WDL differs). If a subsequent run finds a cached copy, it does NOT check whether the remote URI
                    content may have changed.
                    """
                )
            )
            if yes("ENABLE?"):
                options["download_cache"] = {"get": "true", "put": "true"}
                print("\nDownload cache directory: /tmp/miniwdl_download_cache")

                if yes("OVERRIDE?"):
                    options["download_cache"]["dir"] = bullet.Input(
                        prompt="Download cache directory: ", strip=True
                    ).launch()

            print()
            if yes("Configure non-public Amazon s3:// access?"):
                print(
                    textwrap.dedent(
                        """
                    HOST AWS CREDENTIALS: allow S3 transfers to adopt AWS credentials from host AWS CLI configuration
                    (detected by boto3). This is usually needed for access to non-public S3 objects only when NOT
                    running on an EC2 instance (where S3 tools can contact the instance metadata service to assume an
                    instance profile automatically).
                    """
                    )
                )
                if yes("ENABLE?"):
                    options["download_awscli"] = {"host_credentials": "true"}
        except KeyboardInterrupt:
            print()
            sys.exit(1)

        if not options:
            print("", file=sys.stderr)
            logger.warning("All selections match defaults; exiting")
            sys.exit(0)

        cfg_content = format_cfg(options)
        print()
        print(cfg_content)
        print()
        sys.stdout.flush()
        os.makedirs(os.path.dirname(cfg), exist_ok=True)
        with open(cfg, "w") as outfile:
            print(
                f"# miniwdl configure {miniwdl_version or '(version unknown)'} {datetime.utcnow()}Z",
                file=outfile,
            )
            print(cfg_content, file=outfile)
        logger.notice("Wrote configuration file " + cfg)
        logger.notice("Edit the file manually to set advanced options available: ")
        logger.notice(
            "    https://github.com/chanzuckerberg/miniwdl/blob/"
            f"{miniwdl_version or 'main'}/WDL/runtime/config_templates/default.cfg"
        )
        logger.notice(
            "Runtime environment variables may override configuration file options; see documentation:"
        )
        logger.notice(
            "    https://miniwdl.readthedocs.io/en/latest/runner_reference.html#configuration"
        )


def configure_existing(logger, cfg, always=False, miniwdl_version="main"):
    from . import runtime

    envlog = {}
    for k in os.environ:
        if k.upper().startswith("MINIWDL"):
            envlog[k] = os.environ[k]
    if envlog:
        logger.info(_("environment", **envlog))

    loader = runtime.config.Loader(logger, filenames=[cfg] if cfg else None)
    if always or loader.cfg_filename:
        logger.info(
            "see documentation: https://miniwdl.readthedocs.io/en/latest/runner_reference.html#configuration"
        )
        logger.info(
            "see defaults: https://github.com/chanzuckerberg/miniwdl/blob/"
            f"{miniwdl_version}/WDL/runtime/config_templates/default.cfg"
        )
        if not always:
            logger.info("set --force to overwrite existing configuration file")
        eff_opts = loader.get_all(defaults=False)
        if not eff_opts:
            logger.notice("only default configuration options currently apply")
        else:
            logger.notice("effective non-default options (including any environment variables):")
            sys.stderr.flush()
            print(format_cfg(eff_opts))
        return True
    return False


def format_cfg(sections):
    ans = []
    for section, options in sorted(sections.items()):
        ans.append(f"\n[{section}]")
        for key, value in sorted(options.items()):
            value = value.replace("\n", "\n  ")
            ans.append(f"{key} = {value}")
    return "\n".join(ans)


def pkg_version(pkg="miniwdl"):
    import importlib_metadata

    try:
        return importlib_metadata.version(pkg)
    except importlib_metadata.PackageNotFoundError:
        return None


def die(msg, status=2):
    msg = "\n".join(textwrap.wrap(msg, 100))
    if sys.stderr.isatty():
        print(f"\n{ANSI.BHRED}{msg}{ANSI.RESET}\n", file=sys.stderr)
    else:
        print(f"\n{msg}\n", file=sys.stderr)
    sys.exit(status)
