#!/usr/bin/env python3
#
# Example static analysis of WDL tasks. Crawls tasks in HCA skylab, parses and
# typechecks them, displays their inputs and outputs, and detects and warns of
# bindings of non-String values to String declarations.

import subprocess, tempfile, os, glob
import WDL

# fetch tip of master from github.com/HCA/skylab and crawl
# library/tasks/*.wdl files 
def fetch_skylab_tasks():
    tdn = tempfile.mkdtemp(prefix='analyze_HCAskylab_tasks_')
    subprocess.check_call(['wget', '-q', 'https://github.com/HumanCellAtlas/skylab/archive/master.zip'], cwd=tdn)
    subprocess.check_call(['unzip', '-q', 'master.zip'], cwd=tdn)
    return glob.glob(os.path.join(tdn, 'skylab-master', 'library', 'tasks', '*.wdl'))

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

for filename in fetch_skylab_tasks():
    print(format(os.path.split(filename)[1]))
    analyze_tasks(filename)
