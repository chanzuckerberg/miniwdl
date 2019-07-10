# pyre-strict
import os
from datetime import datetime
from typing import Optional, Tuple
from .. import Tree, Env
from . import plan
from . import online
from .task import run_local_task


def run_local_workflow(
    workflow: Tree.Workflow,
    posix_inputs: Env.Values,
    run_id: Optional[str] = None,
    parent_dir: Optional[str] = None,
) -> Tuple[str, Env.Values]:
    """
    Run a workflow locally.

    Inputs shall have been typechecked already.

    File inputs are presumed to be local POSIX file paths that can be mounted into containers
    """

    state = online.StateMachine(plan.compile(workflow), posix_inputs)

    parent_dir = parent_dir or os.getcwd()

    if run_id:
        run_dir = os.path.join(parent_dir, run_id)
        os.makedirs(run_dir, exist_ok=False)
    else:
        now = datetime.today()
        run_id = now.strftime("%Y%m%d_%H%M%S") + "_" + workflow.name
        try:
            run_dir = os.path.join(parent_dir, run_id)
            os.makedirs(run_dir, exist_ok=False)
        except FileExistsError:
            run_id = now.strftime("%Y%m%d_%H%M%S_") + str(now.microsecond) + "_" + workflow.name
            run_dir = os.path.join(parent_dir, run_id)
            os.makedirs(run_dir, exist_ok=False)

    while True:
        crq = state.step()
        if crq:
            if isinstance(crq.callee, Tree.Task):
                _, outputs = run_local_task(
                    crq.callee, crq.inputs, run_id=crq.id, parent_dir=run_dir
                )
                state.call_finished(crq.id, outputs)
            else:
                raise NotImplementedError()
        elif state.outputs:
            return (run_dir, state.outputs)
