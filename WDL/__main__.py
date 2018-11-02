import sys
import os
from argparse import ArgumentParser
import WDL

def main():
    parser = ArgumentParser()
    
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = 'command'

    check_parser = subparsers.add_parser('check', help='load a WDL document and check for anti-patterns')
    check_parser.add_argument('uri', metavar='URI', type=str, help="WDL document filename/URI")
    check_parser.add_argument('-p', '--path', metavar='DIR', type=str, action='append', help="local directory to search for imports")

    args = parser.parse_args()

    if args.command == "check":
        check(args)
    else:
        assert False

def check(args):
    if args.path is None:
        args.path = []
    doc = WDL.load(args.uri, args.path)

    def show(obj, indent):
        s = ''.join(' ' for i in range(indent*4))
        if isinstance(obj, WDL.Document.Document):
            for uri, namespace, subdoc in obj.imports:
                print("{}{} ({})".format(s, namespace, os.path.basename(uri)))
                show(subdoc, indent+1)
            for task in obj.tasks:
                print("{}task {}".format(s, task.name))
            if obj.workflow:
                print("{}workflow {}".format(s, obj.workflow.name))
                for elt in obj.workflow.elements:
                    show(elt, indent+1)
        elif isinstance(obj, WDL.Document.Call):
            print("{}call {}".format(s, '.'.join(obj.callee_id.namespace + [obj.callee_id.name])))
        elif isinstance(obj, WDL.Document.Scatter):
            print("{}scatter {}".format(s, obj.variable))
            for elt in obj.elements:
                show(elt, indent+1)
        elif isinstance(obj, WDL.Document.Conditional):
            print("{}if".format(s))
            for elt in obj.elements:
                show(elt, indent+1)
    print(os.path.basename(args.uri))
    show(doc,1)

if __name__ == "__main__":
    main()
