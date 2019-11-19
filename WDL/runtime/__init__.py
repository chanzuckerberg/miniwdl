# pyre-strict
from typing import Union, Dict, Tuple, Any
from .. import Tree, Value, Env
from . import task
from . import workflow
from .error import *
from .task import run_local_task, make_output_links
from .workflow import run_local_workflow


def run(
    exe: Union[Tree.Task, Tree.Workflow],
    inputs: Env.Bindings[Value.Base],
    **run_kwargs: Dict[str, Any],
) -> Tuple[str, Env.Bindings[Value.Base]]:
    """
    Run the task or workflow given the inputs environment, and any configuration arguments to
    ``run_local_{task,workflow}``.

    ``inputs`` may be parsed from a JSON dict using :func:`~WDL.values_from_json`. The
    workflow/task name should NOT be used as a namespace for the input values.
    """
    if "max_tasks" in run_kwargs and isinstance(exe, Tree.Task):
        del run_kwargs["max_tasks"]  # N/A to run_local_task
    entrypoint = run_local_task if isinstance(exe, Tree.Task) else run_local_workflow
    return entrypoint(exe, inputs, **run_kwargs)  # pyre-ignore
