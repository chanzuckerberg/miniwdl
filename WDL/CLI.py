"""
miniwdl command-line interface
"""
import sys
import os
import subprocess
import tempfile
import glob
import json
from argparse import ArgumentParser
import WDL
import WDL.Lint


def main(args=None):
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = "command"
    fill_common(fill_check_subparser(subparsers))
    fill_common(fill_inputs_subparser(subparsers))

    args = parser.parse_args(args if args is not None else sys.argv[1:])

    try:
        if args.command == "check":
            check(**vars(args))
        elif args.command == "inputs":
            inputs(**vars(args))
        else:
            assert False
    except (
        WDL.Error.SyntaxError,
        WDL.Error.ImportError,
        WDL.Error.ValidationError,
        WDL.Error.MultipleValidationErrors,
    ) as exn:
        print_error(exn)
        if args.debug:
            raise exn
        else:
            sys.exit(2)


def fill_common(subparser):
    subparser.add_argument(
        "--no-quant-check",
        dest="check_quant",
        action="store_false",
        help="relax static typechecking of optional (?) and nonempty (+) type quantifiers (discouraged; for backwards compatibility with older WDL)",
    )
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
        help="don't use shellcheck on task commands even if available, and suppress warning if it isn't",
    )
    return check_parser


def check(uri=[], path=[], check_quant=True, shellcheck=True, **kwargs):
    # Load the document (read, parse, and typecheck)
    if not shellcheck:
        WDL.Lint._shellcheck_available = False

    for uri in uri:
        doc = WDL.load(uri, path or [], check_quant=check_quant, import_uri=import_uri)

        WDL.Lint.lint(doc)

        # Print an outline
        print(os.path.basename(uri))
        outline(doc, 0, show_called=(doc.workflow is not None))

    if shellcheck and WDL.Lint._shellcheck_available == False:
        print(
            "* Recommendation: install shellcheck (www.shellcheck.net) to check task commands. (--no-shellcheck suppresses this warning)",
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
                level + (1 if not isinstance(dobj, WDL.Decl) else 0),
                file=file,
                show_called=show_called,
            )

    # document
    if isinstance(obj, WDL.Document):
        # workflow
        if obj.workflow:
            descend(obj.workflow)
        # tasks
        for task in sorted(obj.tasks, key=lambda task: (not task.called, task.name)):
            descend(task)
        # imports
        for uri, namespace, subdoc in sorted(obj.imports, key=lambda t: t[1]):
            print("    {}{} : {}".format(s, namespace, os.path.basename(uri)), file=file)
            descend(subdoc)
    # workflow
    elif isinstance(obj, WDL.Workflow):
        print(
            "{}workflow {}{}".format(
                s, obj.name, " (not called)" if show_called and not obj.called else ""
            ),
            file=file,
        )
        for elt in (obj.inputs or []) + obj.elements + (obj.outputs or []):
            descend(elt)
    # task
    elif isinstance(obj, WDL.Task):
        print(
            "{}task {}{}".format(
                s, obj.name, " (not called)" if show_called and not obj.called else ""
            ),
            file=file,
        )
        for decl in (obj.inputs or []) + obj.postinputs + obj.outputs:
            descend(decl)
    # call
    elif isinstance(obj, WDL.Call):
        if obj.name != obj.callee_id.name:
            print(
                "{}call {} as {}".format(
                    s, ".".join(obj.callee_id.namespace + [obj.callee_id.name]), obj.name
                ),
                file=file,
            )
        else:
            print(
                "{}call {}".format(s, ".".join(obj.callee_id.namespace + [obj.callee_id.name])),
                file=file,
            )
    # scatter
    elif isinstance(obj, WDL.Scatter):
        print("{}scatter {}".format(s, obj.variable), file=file)
        for elt in obj.elements:
            descend(elt)
    # if
    elif isinstance(obj, WDL.Conditional):
        print("{}if".format(s), file=file)
        for elt in obj.elements:
            descend(elt)
    # decl
    elif isinstance(obj, WDL.Decl):
        pass

    descend()


def print_error(exn):
    if isinstance(exn, WDL.Error.MultipleValidationErrors):
        for exn1 in exn.exceptions:
            print_error(exn1)
    else:
        print(str(exn), file=sys.stderr)
        if isinstance(exn, WDL.Error.ImportError) and hasattr(exn, "__cause__"):
            print_error(exn.__cause__)
        if isinstance(exn, WDL.Error.ValidationError) and exn.source_text:
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


def import_uri(uri):
    dn = tempfile.mkdtemp(prefix="miniwdl_import_uri_")
    subprocess.check_call(["wget", "-nv", uri], cwd=dn)
    return glob.glob(dn + "/*")[0]


def fill_inputs_subparser(subparsers):
    inputs_parser = subparsers.add_parser(
        "inputs", help="Generate template workflow input specification"
    )
    inputs_parser.add_argument("uri", metavar="URI", type=str, help="WDL document filename/URI")
    inputs_parser.add_argument(
        "--format",
        type=str,
        dest="fmt",
        choices=["json"],
        required=True,
        help="json: Cromwell-style inputs JSON",
    )
    inputs_parser.add_argument(
        "--task", metavar="NAME", type=str, help="Analyze one task instead of workflow"
    )
    inputs_parser.add_argument(
        "--optional",
        dest="include_optional",
        action="store_true",
        help="Include optional inputs that have constant or absent defaults",
    )
    return inputs_parser


def inputs(uri, fmt, include_optional, check_quant, task=None, path=[], **kwargs):
    doc = WDL.load(uri, path or [], check_quant=check_quant, import_uri=import_uri)

    if task:
        try:
            target = next(t for t in doc.tasks if t.name == task)
        except StopIteration:
            print(
                "No such task {} among: {}".format(task, ", ".join(t.name for t in doc.tasks)),
                file=sys.stderr,
            )
            sys.exit(2)
    elif doc.workflow:
        target = doc.workflow
    elif len(doc.tasks) == 1:
        target = doc.tasks[0]
    elif len(doc.tasks) > 1:
        print(
            "Please specify --task NAME, as document contains multiple tasks and no workflow",
            file=sys.stderr,
        )
        sys.exit(2)
    else:
        print("Empty document", file=sys.stderr)
        sys.exit(2)
    assert target

    # inclusion logic:
    # Always include required inputs (declarations with non-optional types
    # that aren't bound to an expression).
    # With --optional,
    #   If the target has an input{} section (WDL 1.0+), then include all
    #   other declarations in that section.
    #   Otherwise, (pre-1.0 heuristic) include any other declarations either
    #   not bound to an expression or bound to a constant expression.
    include = lambda decl: (not decl.type.optional and not decl.expr) or (
        include_optional
        and (target.inputs is not None or not decl.expr or is_constant_expr(decl.expr))
    )
    included_inputs = []
    for elt in target.available_inputs:
        if isinstance(elt, WDL.Env.Binding) and include(elt.rhs):
            included_inputs.append(elt)
        if isinstance(elt, WDL.Env.Namespace):
            subelts = []
            for subelt in elt.bindings:
                if isinstance(subelt, WDL.Env.Binding) and include(subelt.rhs):
                    subelts.append(subelt)
                # should we do anything with sub-sub-workflow inputs?
            if subelts:
                included_inputs.append(WDL.Env.Namespace(elt.namespace, subelts))

    if fmt == "json":
        print(json_inputs_template(target, included_inputs))
    else:
        assert False


def is_constant_expr(expr):
    """
    Decide if the expression is "constant" for the purposes of `miniwdl inputs --optional`
    """
    if isinstance(expr, (WDL.Expr.Int, WDL.Expr.Float, WDL.Expr.Boolean)):
        return True
    if isinstance(expr, WDL.Expr.String) and (
        len(expr.parts) == 2 or (len(expr.parts) == 3 and isinstance(expr.parts[1], str))
    ):
        return True
    if isinstance(expr, WDL.Expr.Array):
        return not [item for item in expr.items if not is_constant_expr(item)]
    # TODO: Pair, Map, Struct???
    return False


def json_inputs_template(target, inputs_env):
    """
    Formulate template for Cromwell-style JSON inputs
    """
    ans = {}
    for elt in reversed(inputs_env):
        if isinstance(elt, WDL.Env.Binding):
            ans[".".join([target.name, elt.name])] = json_template_value(elt.rhs.type, elt.rhs.expr)
        if isinstance(elt, WDL.Env.Namespace):
            for subelt in reversed(elt.bindings):
                assert isinstance(subelt, WDL.Env.Binding)
                ans[".".join([target.name, elt.namespace, subelt.name])] = json_template_value(
                    subelt.rhs.type, subelt.rhs.expr
                )
    return json.dumps(ans, indent=2)


def json_template_value(ty, expr=None):
    """
    Given an input declaration, return a template value for the inputs JSON.
    - If the declaration is bound to a constant expression, use its value.
    - Otherwise, produce some dummy based on the type.
    """

    if isinstance(expr, (WDL.Expr.Int, WDL.Expr.Float, WDL.Expr.Boolean)):
        return expr.value
    if isinstance(expr, WDL.Expr.String) and len(expr.parts) == 2:
        return ""
    if (
        isinstance(expr, WDL.Expr.String)
        and len(expr.parts) == 3
        and isinstance(expr.parts[1], str)
    ):
        return expr.parts[1]
    if isinstance(expr, WDL.Expr.Array):
        return [json_template_value(ty.item_type, x) for x in expr.items]

    if isinstance(ty, WDL.Type.Int):
        return 42
    if isinstance(ty, WDL.Type.Float):
        return 3.14
    if isinstance(ty, WDL.Type.Boolean):
        return False
    if isinstance(ty, WDL.Type.String):
        return "foo"
    if isinstance(ty, WDL.Type.File):
        return "/path/to/file"
    if isinstance(ty, WDL.Type.Array):
        tmpl = json_template_value(ty.item_type)
        return [tmpl, tmpl]

    # TODO: Pair, Map, Struct??
    raise NotImplementedError(str(type) + " input template")
