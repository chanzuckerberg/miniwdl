# pyre-strict
import os
import math
from typing import Union, Dict, Any, List, Tuple
from .. import Tree, Type, Value, Env
from .._util import parse_byte_size
from . import task
from . import workflow
from .error import *
from .task import run_local_task
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


def make_output_links(
    exe: Union[Tree.Task, Tree.Workflow], run_dir: str, outputs_json: Dict[str, Any]
) -> None:
    """
    Following a successful run, the output files may be scattered throughout a complex directory
    tree used for execution. To help navigating this, generate a subdirectory of the run directory
    containing nicely organized symlinks to the output files.

    Given ``WDL.Env.Bindings[WDL.Value.Base]`` outputs, this expects to receive
    ``WDL.values_to_json(outputs, namespace=targets.name)`` instead of outputs directly. This makes
    it compatible with Cromwell's output JSON too.
    """

    def _is_files(ty: Type.Base) -> bool:
        """
        is ty a File or an Array[File] or an Array[Array[File]] or an Array[Array[Array[File]]]...
        """
        return isinstance(ty, Type.File) or (
            isinstance(ty, Type.Array)
            and (isinstance(ty.item_type, Type.File) or _is_files(ty.item_type))
        )

    def link_output_files(dn: str, files: Union[str, List[str]]) -> None:
        # dn: output directory which already exists
        # files: either a filename str, or a [nested] list thereof
        if isinstance(files, str) and os.path.exists(files):
            os.symlink(files, os.path.join(dn, os.path.basename(files)))
        if isinstance(files, list) and files:
            d = int(math.ceil(math.log10(len(files))))  # how many digits needed
            for i, elt in enumerate(files):
                subdn = os.path.join(dn, str(i).rjust(d, "0"))
                os.makedirs(subdn, exist_ok=False)
                link_output_files(subdn, elt)

    for binding in exe.effective_outputs:
        fqon = ".".join([exe.name, binding.name])
        if _is_files(binding.value) and fqon in outputs_json:
            odn = os.path.join(run_dir, "output_links", fqon)
            os.makedirs(odn, exist_ok=False)
            link_output_files(odn, outputs_json[fqon])
    # TODO: handle File's inside other compound types,
    # Pair[File,File], Map[String,File], Structs, etc.
