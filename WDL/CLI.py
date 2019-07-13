"""
miniwdl command-line interface
"""
# PYTHON_ARGCOMPLETE_OK
import sys
import os
import subprocess
import tempfile
import glob
import json
import math
import argcomplete
import logging
from shlex import quote as shellquote
from datetime import datetime
from argparse import ArgumentParser, Action
import pkg_resources
from . import *

quant_warning = False


def main(args=None):
    parser = ArgumentParser()
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
    fill_common(fill_cromwell_subparser(subparsers), path=False)  # FIXME path issue #131
    fill_common(fill_run_subparser(subparsers))

    argcomplete.autocomplete(parser)
    args = parser.parse_args(args if args is not None else sys.argv[1:])

    try:
        if args.command == "check":
            check(**vars(args))
        elif args.command == "run":
            runner(**vars(args))
        elif args.command == "cromwell":
            cromwell(**vars(args))
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
                "* Hint: for compatibility with older existing WDL code, try setting --no-quant-check to relax quantifier validation rules.",
                file=sys.stderr,
            )
        if args.debug:
            raise exn
        sys.exit(2)


class PipVersionAction(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        try:
            print(pkg_resources.get_distribution("miniwdl"))
        except pkg_resources.DistributionNotFound as exc:
            print("miniwdl version unknown ({}: {})".format(type(exc).__name__, exc))
        print("Cromwell version: " + CROMWELL_VERSION)
        sys.exit(0)


def fill_common(subparser, path=True):
    subparser.add_argument(
        "--no-quant-check",
        dest="check_quant",
        action="store_false",
        help="relax static typechecking of optional types, and permit coercion of T to Array[T] (discouraged; for backwards compatibility with older WDL)",
    )
    if path:
        subparser.add_argument(
            "-p",
            "--path",
            metavar="DIR",
            type=str,
            action="append",
            help="local directory to search for imports",
        )
    subparser.add_argument("--debug", action="store_true", help="show full exception traceback")


def fill_check_subparser(subparsers):
    check_parser = subparsers.add_parser(
        "check", help="Load and typecheck a WDL document; show an outline with lint warnings"
    )
    check_parser.add_argument(
        "uri", metavar="URI", type=str, nargs="+", help="WDL document filename/URI"
    )
    check_parser.add_argument(
        "--no-shellcheck",
        dest="shellcheck",
        action="store_false",
        help="don't use shellcheck on task commands even if available, and suppress message if it isn't",
    )
    return check_parser


def check(uri=None, path=None, check_quant=True, shellcheck=True, **kwargs):
    # Load the document (read, parse, and typecheck)
    if not shellcheck:
        Lint._shellcheck_available = False

    for uri1 in uri or []:
        doc = load(uri1, path or [], check_quant=check_quant, import_uri=import_uri)

        Lint.lint(doc)

        # Print an outline
        print(os.path.basename(uri1))
        outline(doc, 0, show_called=(doc.workflow is not None))

    if shellcheck and Lint._shellcheck_available == False:
        print(
            "* Hint: install shellcheck (www.shellcheck.net) to check task commands. (--no-shellcheck suppresses this message)",
            file=sys.stderr,
        )


def outline(obj, level, file=sys.stdout, show_called=True):
    # recursively pretty-print a brief outline of the workflow
    s = "".join(" " for i in range(level * 4))

    first_descent = []

    def descend(dobj=None, first_descent=first_descent):
        # show lint for the node just prior to first descent beneath it
        if not first_descent and hasattr(obj, "lint"):
            for (pos, klass, msg) in sorted(obj.lint, key=lambda t: t[0]):
                print(
                    "{}    (Ln {}, Col {}) {}, {}".format(s, pos.line, pos.column, klass, msg),
                    file=file,
                )
        first_descent.append(False)
        if dobj:
            outline(
                dobj,
                level + (1 if not isinstance(dobj, Decl) else 0),
                file=file,
                show_called=show_called,
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
        if isinstance(getattr(exn, "pos", None), SourcePosition):
            print(
                "({} Ln {} Col {}) {}".format(
                    exn.pos.filename, exn.pos.line, exn.pos.column, str(exn)
                ),
                file=sys.stderr,
            )
        else:
            print(str(exn), file=sys.stderr)
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


def import_uri(uri):
    dn = tempfile.mkdtemp(prefix="miniwdl_import_uri_")
    subprocess.check_call(["wget", "-nv", uri], cwd=dn)
    return glob.glob(dn + "/*")[0]


def fill_run_subparser(subparsers):
    run_parser = subparsers.add_parser("run", help="Run task locally [alpha test]")
    run_parser.add_argument("uri", metavar="URI", type=str, help="WDL document filename/URI")
    run_parser.add_argument(
        "inputs",
        metavar="input_key=value",
        type=str,
        nargs="*",
        help="Workflow inputs. Arrays may be supplied by repeating, key=value1 key=value2 ...",
    ).completer = runner_input_completer
    run_parser.add_argument(
        "-d",
        "--dir",
        metavar="NEW_DIR",
        dest="rundir",
        help="directory to be created for the workflow products (must not already exist; defaults to a timestamp-based subdirectory of the current directory)",
    )
    run_parser.add_argument(
        "-i",
        "--input",
        metavar="INPUT.json",
        dest="input_file",
        help="file with Cromwell-style input JSON; command-line inputs will be merged in",
    )
    run_parser.add_argument(
        "-j",
        "--json",
        dest="json_only",
        action="store_true",
        help="just print Cromwell-style input JSON to standard output, then exit",
    )
    run_parser.add_argument(
        "--empty",
        metavar="input_key",
        action="append",
        help="explicitly set an array input to the empty array (to override a default)",
    )
    # TODO:
    # way to specify None for an optional value (that has a default)
    return run_parser


def runner(
    uri, inputs, input_file, json_only, empty, check_quant, rundir=None, path=None, **kwargs
):
    # load WDL document
    doc = load(uri, path or [], check_quant=check_quant, import_uri=import_uri)

    # validate the provided inputs and prepare Cromwell-style JSON
    target, input_env, input_json = runner_input(doc, inputs, input_file, empty)

    if json_only:
        print(json.dumps(input_json, indent=2))
        sys.exit(0)

    # provision a run directory
    rundir = runner_provision_directory(target, rundir)

    # write the JSON inputs file
    input_json_filename = None
    print("input JSON: " + json.dumps(input_json, indent=2), file=sys.stderr)
    input_json_filename = os.path.join(rundir, "inputs.json")
    with open(input_json_filename, "w") as outfile:
        print(json.dumps(input_json, indent=2), file=outfile)

    # run task
    logging.basicConfig(level=logging.DEBUG if kwargs["debug"] else logging.INFO)
    try:
        runner = runtime.run_local_task if isinstance(target, Task) else runtime.run_local_workflow
        _, output_env = runner(target, input_env, run_id=target.name, parent_dir=rundir)
    except Error.EvalError as exn:
        print(
            "({} Ln {} Col {}) {}, {}".format(
                exn.pos.filename, exn.pos.line, exn.pos.column, exn.__class__.__name__, str(exn)
            ),
            file=sys.stderr,
        )
        if kwargs["debug"]:
            raise exn
        sys.exit(2)
    except runtime.task.TaskFailure as exn:
        if exn.__cause__ and isinstance(getattr(exn.__cause__, "pos", None), SourcePosition):
            pos = getattr(exn.__cause__, "pos")
            print(
                "({} Ln {} Col {}) {}, {}".format(
                    pos.filename,
                    pos.line,
                    pos.column,
                    exn.__cause__.__class__.__name__,
                    str(exn.__cause__),
                ),
                file=sys.stderr,
            )
        else:
            print(str(exn.__cause__ or exn), file=sys.stderr)
        if kwargs["debug"]:
            raise (exn.__cause__ or exn)
        sys.exit(2)

    # link output files
    outputs_json = values_to_json(output_env, namespace=[target.name])
    runner_organize_outputs(target, {"outputs": outputs_json}, rundir)


def runner_input_completer(prefix, parsed_args, **kwargs):
    # argcomplete completer for `miniwdl run` and `miniwdl cromwell`
    if "uri" in parsed_args:
        # load document. in the completer setting, we need to substitute the home directory
        # and environment variables
        uri = os.path.expandvars(os.path.expanduser(parsed_args.uri))
        if not os.path.exists(uri):
            argcomplete.warn("file not found: " + uri)
            return []
        try:
            doc = load(uri, parsed_args.path, parsed_args.check_quant, import_uri=import_uri)
        except Exception as exn:
            argcomplete.warn(
                "unable to load {}; try 'miniwdl check' on it ({})".format(uri, str(exn))
            )
            return []
        # resolve target
        if doc.workflow:
            target = doc.workflow
        elif len(doc.tasks) == 1:
            target = doc.tasks[0]
        elif len(doc.tasks) > 1:
            argcomplete.warn("WDL document contains multiple tasks and no workflow")
            return []
        else:
            argcomplete.warn("WDL document is empty")
            return []
        assert target
        # figure the available input names (starting with prefix, if any)
        available_input_names = [nm + "=" for nm in values_to_json(target.available_inputs)]
        if prefix and prefix.find("=") == -1:
            available_input_names = [nm for nm in available_input_names if nm.startswith(prefix)]
        # TODO idea -- complete only required inputs until they're all present, then start
        # completing the non-required inputs. Tricky with arrays, because we want to keep
        # allowing their completion even after already supplied.
        # compute set of inputs already supplied
        return available_input_names


def runner_input(doc, inputs, input_file, empty):
    """
    - Determine the target workflow/task
    - Check types of supplied inputs
    - Check all required inputs are supplied
    - Return inputs as Env.Values
    """

    # resolve target
    if doc.workflow:
        target = doc.workflow
    elif len(doc.tasks) == 1:
        target = doc.tasks[0]
    elif len(doc.tasks) > 1:
        die("WDL document contains multiple tasks and no workflow")
    else:
        die("Empty WDL document")
    assert target

    # build up an Env.Values of the provided inputs
    available_inputs = target.available_inputs
    input_env = []

    # first load input JSON file if any
    if input_file:
        with open(input_file) as infile:
            input_env = values_from_json(
                json.loads(infile.read()),
                available_inputs,
                namespace=([target.name] if isinstance(target, Workflow) else []),
            )

    # set explicitly empty arrays
    for empty_name in empty or []:
        empty_name = empty_name.split(".")
        if not empty_name or ([True for s in empty_name if not s]):
            die("Invalid input name: " + empty_name)
        namespace = empty_name[:-1]
        name = empty_name[-1]
        try:
            decl = Env.resolve(available_inputs, namespace, name)
        except KeyError:
            die(
                "No such input to {}: {}\n{}".format(
                    target.name, ".".join(empty_name), runner_input_help(target)
                )
            )
        if not isinstance(decl.type, Type.Array) or decl.type.nonempty:
            die("Cannot set input {} {} to empty array".format(str(decl.type), decl.name))
        input_env = Env.bind(input_env, namespace, name, Value.Array(decl.type, []), ctx=decl)

    # add in command-line inputs
    for one_input in inputs:
        # parse [namespace], name, and value
        buf = one_input.split("=", 1)
        if len(buf) != 2 or not buf[0]:
            die("Invalid input name=value pair: " + one_input)
        name, s_value = buf
        name = name.split(".")
        if not name or ([True for s in name if not s]):
            die("Invalid input name=value pair: " + one_input)
        namespace = name[:-1]
        name = name[-1]

        # find corresponding input declaration
        try:
            decl = Env.resolve(available_inputs, namespace, name)
        except KeyError:
            die(
                "No such input to {}: {}\n{}".format(target.name, buf[0], runner_input_help(target))
            )

        # create a Value based on the expected type
        v = runner_input_value(s_value, decl.type)

        # insert value into input_env
        try:
            existing = Env.resolve(input_env, namespace, name)
        except KeyError:
            existing = None
        if existing:
            if isinstance(v, Value.Array):
                assert isinstance(existing, Value.Array) and existing.type == v.type
                existing.value.extend(v.value)
            else:
                die(
                    "non-array input {} duplicated on command line\n{}".format(
                        buf[0], runner_input_help(target)
                    )
                )
        else:
            input_env = Env.bind(input_env, namespace, name, v, ctx=decl)

    # check for missing inputs
    missing_inputs = values_to_json(Env.subtract(target.required_inputs, input_env))
    if missing_inputs:
        die(
            "missing required inputs for {}: {}\n{}".format(
                target.name, ", ".join(missing_inputs.keys()), runner_input_help(target)
            )
        )

    # make a pass over the Env to create a dict for Cromwell-style input JSON
    return (
        target,
        input_env,
        values_to_json(
            input_env, namespace=([target.name] if isinstance(target, Workflow) else [])
        ),
    )


def runner_input_help(target):
    # TODO: get help message from parameter_meta
    # TODO: show default values of optionals
    ans = []
    required_inputs = target.required_inputs
    ans.append("\nrequired inputs:")
    for name, ty in values_to_json(required_inputs).items():
        ans.append("  {} {}".format(ty, name))
    optional_inputs = Env.subtract(target.available_inputs, target.required_inputs)
    if target.inputs is None:
        # if the target doesn't have an input{} section (pre WDL 1.0), exclude
        # declarations bound to a non-constant expression (heuristic)
        optional_inputs = Env.filter(
            optional_inputs, lambda _, b: b.rhs.expr is None or is_constant_expr(b.rhs.expr)
        )
    if optional_inputs:
        ans.append("\noptional inputs:")
        for name, ty in values_to_json(optional_inputs).items():
            ans.append("  {} {}".format(ty, name))
    ans.append("\noutputs:")
    for name, ty in values_to_json(target.effective_outputs).items():
        ans.append("  {} {}".format(ty, name))
    return "\n".join(ans)


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


def runner_input_value(s_value, ty):
    """
    Given an input value from the command line (right-hand side of =) and the
    WDL type of the corresponding input decl, create an appropriate Value.
    """
    if isinstance(ty, Type.String):
        return Value.String(s_value)
    if isinstance(ty, Type.File):
        if not os.path.exists(s_value):
            die("File not found: " + s_value)
        return Value.File(os.path.abspath(s_value))
    if isinstance(ty, Type.Boolean):
        if s_value == "true":
            return Value.Boolean(True)
        if s_value == "false":
            return Value.Boolean(False)
        die("Boolean input should be true or false instead of {}".format(s_value))
    if isinstance(ty, Type.Int):
        return Value.Int(int(s_value))
    if isinstance(ty, Type.Float):
        return Value.Float(float(s_value))
    if isinstance(ty, Type.Array) and isinstance(
        ty.item_type, (Type.String, Type.File, Type.Int, Type.Float)
    ):
        # just produce a length-1 array, to be combined ex post facto
        return Value.Array(ty, [runner_input_value(s_value, ty.item_type)])
    return die(
        "No command-line support yet for inputs of type {}; workaround: specify in JSON file with --input".format(
            str(ty)
        )
    )


def runner_provision_directory(target, rundir=None):
    if rundir:
        rundir = os.path.abspath(rundir)
        try:
            os.makedirs(rundir, exist_ok=False)
        except FileExistsError:
            die("workflow directory already exists: " + rundir)
    else:
        now = datetime.today()
        try:
            rundir = os.path.join(os.getcwd(), now.strftime("%Y%m%d_%H%M%S") + "_" + target.name)
            os.makedirs(rundir, exist_ok=False)
        except FileExistsError:
            rundir = os.path.join(
                os.getcwd(),
                now.strftime("%Y%m%d_%H%M%S_") + str(now.microsecond) + "_" + target.name,
            )
            os.makedirs(rundir, exist_ok=False)
    return rundir


def runner_organize_outputs(target, outputs_json, rundir):
    """
    After a successful workflow run, the output files are typically sprayed
    across a bushy directory tree used for execution. To help the user find
    what they're looking for, we create another directory tree with nicer
    organization, containing symlinks to the output files (so as not to disturb
    them).

    One of the subtleties is to organize compound outputs like Array[File],
    Array[Array[File]], etc.
    """
    assert "dir" not in outputs_json
    outputs_json["dir"] = rundir
    print(json.dumps(outputs_json, indent=2))
    with open(os.path.join(rundir, "outputs.json"), "w") as outfile:
        print(json.dumps(outputs_json, indent=2), file=outfile)

    os.makedirs(os.path.join(rundir, "outputs"), exist_ok=False)

    def link_output_files(dn, files):
        # dn: output directory which already exists
        # files: either a filename str, or a [nested] list thereof
        if isinstance(files, str) and os.path.exists(files):
            os.symlink(files, os.path.join(dn, os.path.basename(files)))
        if isinstance(files, list) and files:
            d = int(math.ceil(math.log10(len(files))))  # how many digits needed
            for i, elt in enumerate(files):
                subdn = os.path.join(dn, str(i).rjust(d, "0"))
                os.makedirs(subdn, exist_ok=False)
                link_output_files(subdn, elt)

    def output_links(namespace, binding):
        fqon = ".".join([target.name] + namespace + [binding.name])
        if _is_files(binding.rhs) and fqon in outputs_json["outputs"]:
            odn = os.path.join(rundir, "outputs", fqon)
            os.makedirs(os.path.join(rundir, odn), exist_ok=False)
            link_output_files(odn, outputs_json["outputs"][fqon])
        return True

    Env.filter(target.effective_outputs, output_links)
    # TODO: handle File's inside other compound types,
    # Pair[File,File], Map[String,File], Structs, etc.


def fill_cromwell_subparser(subparsers):
    cromwell_parser = subparsers.add_parser(
        "cromwell", help="Run workflow/task locally using Cromwell " + CROMWELL_VERSION
    )
    cromwell_parser.add_argument("uri", metavar="URI", type=str, help="WDL document filename/URI")
    cromwell_parser.add_argument(
        "inputs",
        metavar="input_key=value",
        type=str,
        nargs="*",
        help="Workflow inputs. Arrays may be supplied by repeating, key=value1 key=value2 ...",
    ).completer = runner_input_completer
    cromwell_parser.add_argument(
        "-d",
        "--dir",
        metavar="NEW_DIR",
        dest="rundir",
        help="directory to be created for the workflow products (must not already exist; defaults to a timestamp-based subdirectory of the current directory)",
    )
    cromwell_parser.add_argument(
        "-i",
        "--input",
        metavar="INPUT.json",
        dest="input_file",
        help="file with Cromwell-style input JSON; command-line inputs will be merged in",
    )
    cromwell_parser.add_argument(
        "-j",
        "--json",
        dest="json_only",
        action="store_true",
        help="just print Cromwell-style input JSON to standard output, then exit",
    )
    cromwell_parser.add_argument(
        "--empty",
        metavar="input_key",
        action="append",
        help="explicitly set an array input to the empty array (to override a default)",
    )
    cromwell_parser.add_argument(
        "-r",
        "--jar",
        metavar="jarfile",
        dest="jarfile",
        type=str,
        help="Cromwell jarfile file path (also set by CROMWELL_JAR environment variable). Overrides default behavior of downloading a hard-coded version",
    )
    cromwell_parser.add_argument(
        "-c",
        "--config",
        metavar="CONFIG.conf",
        dest="config",
        type=str,
        help="Cromwell backend configuration CONF file path (also set by CROMWELL_CONFIG environment variable)",
    )
    # TODO:
    # way to specify None for an optional value (that has a default)
    return cromwell_parser


def cromwell(
    uri,
    inputs,
    input_file,
    json_only,
    empty,
    check_quant,
    rundir=None,
    jarfile=None,
    config=None,
    path=None,
    **kwargs,
):
    path = path or []

    # load WDL document
    doc = load(uri, path, check_quant=check_quant, import_uri=import_uri)

    # validate the provided inputs and prepare Cromwell-style JSON
    target, _, input_json = runner_input(doc, inputs, input_file, empty)

    if json_only:
        print(json.dumps(input_json, indent=2))
        sys.exit(0)

    # provision a run directory
    rundir = runner_provision_directory(target, rundir)
    os.makedirs(os.path.join(rundir, "cromwell"))

    # write the JSON inputs file
    input_json_filename = None
    print("Cromwell input: " + json.dumps(input_json, indent=2), file=sys.stderr)
    input_json_filename = os.path.join(rundir, "inputs.json")
    with open(input_json_filename, "w") as outfile:
        print(json.dumps(input_json, indent=2), file=outfile)

    # write Cromwell options
    cromwell_options = {"final_workflow_log_dir": os.path.join(rundir, "cromwell")}
    cromwell_options_filename = os.path.join(rundir, "cromwell", "options.json")
    with open(cromwell_options_filename, "w") as options_json:
        print(json.dumps(cromwell_options, indent=2), file=options_json)

    # setup Cromwell config file
    config_setting = None
    if config:
        config_setting = "-Dconfig.file={}".format(config)
    elif "CROMWELL_CONFIG" in os.environ:
        config_setting = "-Dconfig.file={}".format(os.getenv("CROMWELL_CONFIG"))

    # launch Cromwell
    jarpath = ensure_cromwell_jar(jarfile)
    cromwell_cmd = ["java", "-DLOG_LEVEL=warn", "-DLOG_MODE=pretty"]
    cromwell_cmd.extend([config_setting] if config_setting else [])
    cromwell_cmd.extend(
        [
            "-jar",
            jarpath,
            "run",
            (os.path.abspath(uri) if "://" not in uri else uri),
            "-o",
            cromwell_options_filename,
            "-i",
            input_json_filename,
        ]
    )

    for p in path:
        # FIXME issue #131
        cromwell_cmd.append("--imports")
        cromwell_cmd.append(p)
    print(" ".join(["+"] + [shellquote(s) for s in cromwell_cmd]), file=sys.stderr)
    proc = subprocess.Popen(
        cromwell_cmd, cwd=os.path.join(rundir, "cromwell"), stdout=subprocess.PIPE
    )

    # stream in Cromwell stdout, which mixes a bunch of stuff. tee it to stderr
    # while recording it so we can go back to look for the output JSON later.
    cromwell_output_lines = []
    while proc.poll() is None:
        line = proc.stdout.readline()
        if line:
            line = str(line, "utf-8").rstrip()
            print(line, file=sys.stderr)
            cromwell_output_lines.append(line)
    assert isinstance(proc.returncode, int)

    # deal with Cromwell outputs

    # remove world-write permissions from created temp files
    subprocess.call(["chmod", "-Rf", "o-w", rundir])

    if proc.returncode == 0:
        # sniff for the outputs JSON as the last subsequence of stdout lines
        # delimited by { and }
        last_lbrace = None
        last_rbrace = None
        try:
            last_lbrace = max(loc for loc, val in enumerate(cromwell_output_lines) if val == "{")
            last_rbrace = max(loc for loc, val in enumerate(cromwell_output_lines) if val == "}")
        except ValueError:
            pass
        try:
            if last_lbrace is None or last_rbrace is None or last_lbrace >= last_rbrace:
                raise KeyError
            outputs_json = json.loads(
                "\n".join(cromwell_output_lines[last_lbrace : (last_rbrace + 1)])
            )
        except:
            die("failed to find outputs JSON in Cromwell standard output")
        runner_organize_outputs(target, outputs_json, rundir)

    sys.exit(proc.returncode)


CROMWELL_VERSION = "42"


def ensure_cromwell_jar(jarfile=None):
    """
    Return local path to Cromwell JAR file, first downloading it if necessary.
    """
    if jarfile:
        jarpath = jarfile
    elif "CROMWELL_JAR" in os.environ:
        jarpath = os.getenv("CROMWELL_JAR")
    else:
        CROMWELL_JAR_URL = "https://github.com/broadinstitute/cromwell/releases/download/{v}/cromwell-{v}.jar".format(
            v=CROMWELL_VERSION
        )
        CROMWELL_JAR_SIZE = 185_212_769
        CROMWELL_JAR_NAME = os.path.basename(CROMWELL_JAR_URL)

        jarpath = os.path.join(tempfile.gettempdir(), CROMWELL_JAR_NAME)
        try:
            if os.path.getsize(jarpath) == CROMWELL_JAR_SIZE:
                return jarpath
        except:
            pass
        subprocess.check_call(["wget", "-nv", "-O", jarpath, CROMWELL_JAR_URL])
        assert os.path.getsize(jarpath) == CROMWELL_JAR_SIZE, (
            "unexpected size of downloaded " + jarpath
        )
    return jarpath


def _is_files(ty):
    """
    is ty a File or an Array[File] or an Array[Array[File]] or an Array[Array[Array[File]]]...
    """
    return isinstance(ty, Type.File) or (
        isinstance(ty, Type.Array)
        and (isinstance(ty.item_type, Type.File) or _is_files(ty.item_type))
    )


def die(msg, status=2):
    print("\n" + msg + "\n", file=sys.stderr)
    sys.exit(status)
