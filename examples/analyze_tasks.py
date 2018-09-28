#!/usr/bin/env python3
#
# Example static analysis of WDL tasks. Given one or more WDL filenames, parses
# and type-checks each individual tasks, prints their inputs and outputs, and
# warns of implicit String coercions.

import sys, os
import WDL

def analyze_tasks(filename):
    with open(filename) as infile:
        # parse task(s) in the file
        for task in WDL.parse_tasks(infile.read()):
            print("  task " + task.name)

            # Statically type-check expressions and declarations in the task.
            # Afterwards, each expression is decorated with its type.
            task.typecheck()

            def format_decl(decl):
                # helper to pretty-print a declaration
                quant = ""
                if decl.type.optional:
                    quant = "?"
                if isinstance(decl.type, WDL.Type.Array) and decl.type.nonempty:
                    quant = "+"
                return "{}{} {}".format(str(decl.type), quant, decl.name)

            # enumerate task inputs and any other unbound declarations
            inputs = [format_decl(decl) for decl in task.inputs]
            inputs = inputs + [format_decl(decl) for decl in task.postinputs if decl.expr is None]
            print("    I: " + ", ".join(inputs))
            # and outputs
            print("    O: " + ", ".join([format_decl(decl) for decl in task.outputs]))

            # detect declarations implying implicit String-ification
            for decl in task.inputs + task.postinputs + task.outputs:
                if isinstance(decl.type, WDL.Type.String) \
                   and decl.expr is not None \
                   and not isinstance(decl.expr.type, WDL.Type.String) \
                   and not isinstance(decl.expr.type, WDL.Type.File):
                   print("    [WARN] (Ln {}, Col {}): implicit coercion of {} to String {}".format(
                                decl.expr.pos.line, decl.expr.pos.column, str(decl.expr.type), decl.name))

if len(sys.argv) > 1:
    for filename in sys.argv[1:]:
            print(format(os.path.split(filename)[1]))
            analyze_tasks(filename)
else:
    print("Usage: analyze_tasks.py tasks.wdl [tasks2.wdl ...]")
    sys.exit(1)
