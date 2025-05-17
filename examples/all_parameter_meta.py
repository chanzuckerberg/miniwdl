"""
Demonstrate how to find the parameter_meta for all of a workflow's `available_inputs`,
including nested inputs where the parameter_meta declaration is found in the respective
executable.
"""

import argparse

import WDL


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "workflow_wdl",
        metavar="WORKFLOW.wdl",
        help="input workflow document with imports",
    )
    args = parser.parse_args()

    # load workflow document and find the top-level workflow/task
    doc = WDL.load(args.workflow_wdl)
    exe = doc.workflow
    if not exe:
        assert (
            len(doc.tasks) == 1
        ), "document has neither a workflow nor exactly one task"
        exe = doc.tasks[0]

    # resolve the parameter_meta of its available_inputs
    all_parameter_meta = resolve_parameter_meta(exe.available_inputs)
    for name, meta in all_parameter_meta.items():
        meta = meta if meta else "-"
        print(f"{name}\t{meta}")


def resolve_parameter_meta(inputs):
    # inputs is WDL.Env.Bindings[WDL.Decl], i.e. a mapping from namespaced input name
    # to its declaration. The "namespaced" input name is dot-separated.
    ans = {}
    for binding in inputs:
        decl = binding.value
        # find decl's parent workflow/task
        decl_exe = decl
        while not isinstance(decl_exe, (WDL.Workflow, WDL.Task)):
            decl_exe = decl.parent
        # look in decl_exe's parameter_meta for a corresponding entry; removing
        # namespaces if any.
        decl_exe_meta = decl_exe.parameter_meta
        binding_basename = binding.name.split(".")[-1]
        ans[binding.name] = decl_exe_meta.get(binding_basename, None)
    return ans


if __name__ == "__main__":
    main()
