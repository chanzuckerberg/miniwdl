#!/usr/bin/env python3

"""
Generate a standalone WDL document from a given workflow using imported tasks. Requires: miniwdl

    python3 paste_wdl_imports.py [-o STANDALONE.wdl] WORKFLOW.wdl

For each "call imported_namespace.task_name [as alias]" in the workflow, appends the task's source
code with the task name changed to "imported_namespace__task_name", and rewrites the call to refer
to this new name (keeping the original alias). Also blanks out the import statements.

This is a quick-and-dirty hack with salient limitations:
1. Can't handle imported structs
2. Can't handle imported sub-workflows (tasks only)
3. Each call, task, and import must begin on its own line of code with only whitespace preceding.

Notes on these limitations:
1. Structs would be tricky to get right, but probably doable. The problem is they can be imported
   recursively (including diamond imports) with different names in every document.
2. We can't do much about sub-workflows because a WDL document can only have one workflow.
3. Stems from a crappy regex-based approach used because miniwdl WDL library lacks code generation
"""

import re
import argparse
import WDL


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o",
        metavar="STANDALONE.wdl",
        help="output filename (default: standard output)",
        default=None,
    )
    parser.add_argument(
        "workflow_wdl", metavar="WORKFLOW.wdl", help="input workflow document with imports"
    )
    args = parser.parse_args()

    # load workflow document
    doc = WDL.load(args.workflow_wdl)
    assert doc.workflow, "document has no workflow"

    # run SetParents to facilitate getting from a called task to its containing document
    WDL.Walker.SetParents()(doc)

    # for each call
    tasks_processed = set()
    for call in calls(doc.workflow):
        if len(call.callee_id) > 1:  # imported
            # create name for pasted task
            new_task_name = "__".join(call.callee_id)
            assert isinstance(call.callee, WDL.Task), "can't import sub-workflows"
            # rewrite the call with the new task name
            doc.source_lines[call.pos.line - 1] = rewrite_line(
                doc.source_lines[call.pos.line - 1],
                "call",
                f"{new_task_name} as {call.name}",
                old_name="[0-9A-Za-z_\\.]+(\\s+as\\s+[0-9A-Za-z_]+)?",
            )
            if new_task_name not in tasks_processed:
                task_lines = task_source_lines(call.callee)
                task_lines[0] = rewrite_line(task_lines[0], "task", new_task_name)
                doc.source_lines += ["\n"] + task_lines + ["\n"]
                tasks_processed.add(new_task_name)

    # blank out the imports
    for imp in doc.imports:
        for ln in range(imp.pos.line - 1, imp.pos.end_line):
            doc.source_lines[ln] = ""

    # print output
    if args.o:
        with open(args.o, "w") as outfile:
            for line in doc.source_lines:
                print(line, file=outfile)
    else:
        for line in doc.source_lines:
            print(line)


def calls(element):
    # Yield each Call in the workflow, including those nested withis scatter/conditional sections
    for ch in element.children:
        if isinstance(ch, WDL.Call):
            yield ch
        elif isinstance(ch, WDL.WorkflowSection):
            yield from calls(ch)


def rewrite_line(line, keyword, new_name, old_name="[0-9A-Za-z_\\.]+"):
    # given a line like:
    #   call OLD_NAME {
    # replace OLD_NAME with new_name
    parts = re.fullmatch(f"(?P<front>\\s*{keyword}\\s+){old_name}(?P<back>.*)", line)
    assert parts, f"{keyword} should start on its own source line: {line}"
    return parts.group("front") + new_name + parts.group("back")


def task_source_lines(task):
    # copy the source lines of the given task and change the task name
    task_doc = task
    while not isinstance(task_doc, WDL.Document):
        task_doc = task_doc.parent
    if not hasattr(task_doc, "source_lines"):
        setattr(task_doc, "source_lines", task_doc.source_text.split("\n"))
    first_line = task_doc.source_lines[task.pos.line - 1][task.pos.column - 1 :]
    assert first_line.startswith("task")
    assert task.pos.end_line > task.pos.line, "expected task to span multiple lines"
    ans = (
        [first_line]
        + task_doc.source_lines[task.pos.line : (task.pos.end_line - 1)]
        + [task_doc.source_lines[task.pos.end_line - 1][: task.pos.end_column]]
    )
    assert ans[-1][-1] == "}"
    return ans


if __name__ == "__main__":
    main()
