"""
miniwdl command-line interface
"""
import sys, os
from argparse import ArgumentParser
import WDL
import WDL.Lint

def main(args=None):
    parser = ArgumentParser()
    
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = 'command'

    check_parser = subparsers.add_parser('check', help='Load and typecheck a WDL document; show an outline with lint warnings')
    check_parser.add_argument('uri', metavar='URI', type=str, help="WDL document filename/URI")
    check_parser.add_argument('-p', '--path', metavar='DIR', type=str, action='append', help="local directory to search for imports")

    args = parser.parse_args(args if args is not None else sys.argv[1:])

    if args.command == "check":
        check(args)
    else:
        assert False

def check(args):
    # Load the document (read, parse, and typecheck)
    if args.path is None:
        args.path = []
    doc = WDL.load(args.uri, args.path)

    # Mark up the AST
    WDL.Walker.SetParents()(doc)
    WDL.Walker.MarkCalled()(doc)

    linters = [
        WDL.Lint.ImpliedStringCoercion(),
        WDL.Lint.IncompleteCall(),
        WDL.Lint.CallImportNameCollision(),
        WDL.Lint.UnusedImport()
    ]
    for linter in linters:
        linter(doc)

    # Print an outline
    print(os.path.basename(args.uri))
    outline(doc,1)

# recursively pretty-print a brief outline of the workflow
def outline(obj, level, file=sys.stdout):
    s = ''.join(' ' for i in range(level*4))

    first_descent = []
    def descend(dobj=None, first_descent=first_descent):
        # show lint for the node just prior to first descent beneath it
        if len(first_descent) == 0 and hasattr(obj, 'lint'):
            for (node,klass,msg) in sorted(obj.lint, key=lambda t: t[0]):
                print('{}  (Ln {}, Col {}) {}: {}'.format(s,node.pos.line,node.pos.column,klass,msg), file=file)
        first_descent.append(False)
        if dobj:
            outline(dobj, level+1, file=file)

    # document
    if isinstance(obj, WDL.Document):
        # workflow
        if obj.workflow:
            if level<=1 or obj.workflow.called:
                print("{}workflow {}".format(s, obj.workflow.name), file=file)
                for elt in obj.workflow.elements:
                    descend(elt)
            else:
                # omit the outline of an imported workflow that isn't
                # actually called from the top level
                print("{}workflow {} (not called)".format(s, obj.workflow.name), file=file)
        # tasks
        for task in sorted(obj.tasks, key=lambda task: (not task.called,task.name)):
            print("{}task {}{}".format(s, task.name, " (not called)" if not task.called else ""), file=file)
            for decl in task.inputs + task.postinputs + task.outputs:
                descend(decl)
        # imports
        for uri, namespace, subdoc in sorted(obj.imports, key=lambda t: t[1]):
            print("{}{} : {}".format(s, namespace, os.path.basename(uri)), file=file)
            descend(subdoc)
    # call
    elif isinstance(obj, WDL.Call):
        print("{}call {}".format(s, '.'.join(obj.callee_id.namespace + [obj.callee_id.name])), file=file)
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
