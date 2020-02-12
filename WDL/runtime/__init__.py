# pyre-strict
from typing import Union, Dict, Tuple, Any
from .. import Tree, Value, Env
from . import config
from . import task
from . import workflow
from .error import *
from .task import run_local_task, link_outputs
from .workflow import run_local_workflow


def run(
    cfg: config.Loader,
    exe: Union[Tree.Task, Tree.Workflow],
    inputs: Env.Bindings[Value.Base],
    **run_kwargs: Dict[str, Any],
) -> Tuple[str, Env.Bindings[Value.Base]]:
    """
    Run the task or workflow given the inputs environment, and any configuration arguments to
    ``run_local_{task,workflow}``.

    ``inputs`` may be parsed from a JSON dict using :func:`~WDL.values_from_json`. The
    workflow/task name should NOT be used as a namespace for the input values. They should have
    been typechecked against ``exe.available_inputs`` already.
    """
    if "max_tasks" in run_kwargs and isinstance(exe, Tree.Task):
        del run_kwargs["max_tasks"]  # N/A to run_local_task
    entrypoint = run_local_task if isinstance(exe, Tree.Task) else run_local_workflow
    return entrypoint(cfg, exe, inputs, **run_kwargs)  # pyre-ignore
