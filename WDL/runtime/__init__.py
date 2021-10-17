"""
The recommended way to run a WDL workflow programmatically is to invoke ``miniwdl run`` as a
subprocess, capturing its JSON standard output. This leverages its logging, configuration, and
flexible input loading features; and avoids conflicting with the runtime's thread pools and signal
handlers. Alternatively, it's possible to call ``WDL.runtime.run()`` directly if needed.
"""
# pyre-strict
from typing import Union, Dict, Tuple, Any
from .. import Tree, Value, Env
from . import config
from . import task
from . import workflow
from . import _statusbar
from .error import (
    RunFailed,
    CommandFailed,
    Terminated,
    Interrupted,
    OutputError,
    DownloadFailed,
    error_json,
)
from .task import run_local_task
from .workflow import run_local_workflow


def run(
    cfg: config.Loader,
    exe: Union[Tree.Task, Tree.Workflow],
    inputs: Env.Bindings[Value.Base],
    **run_kwargs: Dict[str, Any],
) -> Tuple[str, Env.Bindings[Value.Base]]:
    """
    Run the task or workflow given the inputs environment and configuration, returning the outputs
    environment. ``inputs`` may be parsed from a JSON dict using :func:`~WDL.values_from_json`,
    which can also validate them; see example below.

    :param run_id: a run identifier used in logs and filenames; defaults to executable name
    :param run_dir: directory under which to create a timestamp-named subdirectory for this run
                    (defaults to current working directory).
                    If the final path component is ``.`` then operate in run_dir directly.

    Typical usage:

    .. code-block:: python

       import WDL
       import WDL.runtime

       # Convert JSON-like inputs dict to WDL environment, validating them against exe's available
       # and required inputs. The dict keys should NOT be namespaced by the executable name;
       # if namespaces are present, then add namespace=exe.name to effectively remove them.
       inputs_env = WDL.values_from_json(inputs_dict, exe.available_inputs, exe.required_inputs)
       # Load configuration (see below)
       cfg = WDL.runtime.config.Loader(logging.getLogger(__name__))
       # Run executable
       run_subdir, outputs_env = WDL.runtime.run(cfg, exe, inputs_env, run_dir="/tmp")
       # Generate JSON-like outputs dict, with keys namespaced by the executable name
       outputs_dict = WDL.values_to_json(outputs_env, exe.name)
    """
    if "max_tasks" in run_kwargs and isinstance(exe, Tree.Task):
        del run_kwargs["max_tasks"]  # N/A to run_local_task
    entrypoint = run_local_task if isinstance(exe, Tree.Task) else run_local_workflow
    return entrypoint(cfg, exe, inputs, **run_kwargs)  # pyre-ignore
