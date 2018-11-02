import sys, os
from argparse import ArgumentParser
import WDL

def main():
    parser = ArgumentParser()
    
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = 'command'

    check_parser = subparsers.add_parser('check', help='Load and typecheck a WDL document; show an outline with lint warnings')
    check_parser.add_argument('uri', metavar='URI', type=str, help="WDL document filename/URI")
    check_parser.add_argument('-p', '--path', metavar='DIR', type=str, action='append', help="local directory to search for imports")

    args = parser.parse_args()

    if args.command == "check":
        check(args)
    else:
        assert False

def check(args):
    # Load the document (read, parse, and typecheck)
    if args.path is None:
        args.path = []
    doc = WDL.load(args.uri, args.path)

    WDL.Walker.SetParents()(doc)
    WDL.Walker.MarkCalled()(doc)

    # recursively pretty-print a brief outline of the workflow
    def outline(obj, level, file=sys.stdout):
        s = ''.join(' ' for i in range(level*4))
        # document
        if isinstance(obj, WDL.Document.Document):
            # workflow
            if obj.workflow:
                if level<=1 or obj.workflow.called:
                    print("{}workflow {}".format(s, obj.workflow.name), file=file)
                    for elt in obj.workflow.elements:
                        outline(elt, level+1, file)
                else:
                    # omit the outline of an imported workflow that isn't
                    # actually called from the top level
                    print("{}workflow {} (not called)".format(s, obj.workflow.name), file=file)
            # tasks
            for task in sorted(obj.tasks, key=lambda task: (not task.called,task.name)):
                print("{}task {}{}".format(s, task.name, " (not called)" if not task.called else ""), file=file)
            # imports
            for uri, namespace, subdoc in sorted(obj.imports, key=lambda t: t[1]):
                print("{}{} : {}".format(s, namespace, os.path.basename(uri)), file=file)
                outline(subdoc, level+1, file)
        # call
        elif isinstance(obj, WDL.Document.Call):
            print("{}call {}".format(s, '.'.join(obj.callee_id.namespace + [obj.callee_id.name])), file=file)
        # scatter
        elif isinstance(obj, WDL.Document.Scatter):
            print("{}scatter {}".format(s, obj.variable), file=file)
            for elt in obj.elements:
                outline(elt, level+1, file)
        # if
        elif isinstance(obj, WDL.Document.Conditional):
            print("{}if".format(s), file=file)
            for elt in obj.elements:
                outline(elt, level+1, file)
        # decl
        elif isinstance(obj, WDL.Document.Decl):
            pass
    print(os.path.basename(args.uri))
    outline(doc,1)

if __name__ == "__main__":
    main()
