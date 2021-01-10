"""
Example miniwdl task runtime plugin, contrived as an "omnibus" extensibility demonstration.

`pip3 install .` installs the plugin and registers it by means of this in setup.py:
    entry_points={
        'miniwdl.plugin.task': ['omnibus = miniwdl_task_omnibus_example:main'],
    }
`miniwdl --version` will list the plugins registered via this entry points mechanism. Furthermore,
the configuration section [plugins] has options to enable/disable installed plugins based on glob
patterns on the module/function name ("miniwdl_task_omnibus_example:main" in this case).

The plugin entry point is a generator function which operates as a "coroutine" communicating
bidirectionally with the runtime at a few key points during the task's lifecycle. (We use the term
"coroutine" in the general sense of PEP 342, not the more-recent async/await constructs, which
aren't involved here.) The coroutine operates as follows:

1. First, it's given information about the requested task and the provided inputs, which it may
   manipulate, and yields back the inputs to use (possibly transformed).
2. Just before scheduling the task container, it's shown the task command, runtime.* values, and
   WDL.runtime.task.TaskContainer object, and yields them back, manipulated as needed.
3. Lastly it's shown the task's evaluated outputs, and yields back the outputs to return.

If the task fails once the coroutine has started, then the coroutine is thrown the relevant
exception, which it may manipulate or replace (but not suppress). Conversely, the coroutine can
raise an exception at any point, causing the task to fail.

The plugin coroutine is intended for quick manipulation/reconfiguration; it blocks the task runtime
thread, and doesn't reserve any cpu/memory. Therefore, it should usually avoid or offload any time-
or resource-intensive activities. As tasks may run concurrently on different threads, so too may
they start concurrent invocations of plugin coroutines.
"""

import WDL
import WDL.runtime


def main(cfg, logger, run_id, run_dir, task, **recv):
    """
    Task plugin entry point, a generator/coroutine initially given:

    :param cfg: the effective miniwdl configuration; see WDL/runtime/config.py
    :param logger: task's logging.Logger instance; plugin might write directly into this logger,
                   or use its getChild() method
    :param run_id: list of run ID strings; the last element identifies this run, while preceding
                   elements capture the "stack" of the workflow run and nested calls, if any.
    :param run_dir: path to task run directory
    :param task: the WDL.Task object from the syntax tree
    :param recv: dict including key "inputs" : WDL.Env.Bindings[WDL.Value.Base]; and possibly other
                 keys in the future.
    """

    logger = logger.getChild("omnibus")
    logger.critical(f"HELLO {task.name} {run_dir}")
    try:
        # recv["inputs"] is a WDL.Env.Bindings[WDL.Value.Base] of the provided task inputs.
        # See relevant documentation for these data structures:
        #   https://miniwdl.readthedocs.io/en/latest/WDL.html#
        # Here we'll map the inputs to add 42 to any (top-level) integer, just to show we can.
        recv["inputs"] = recv["inputs"].map(
            lambda binding: WDL.Env.Binding(binding.name, WDL.Value.Int(binding.value.value + 42))
            if isinstance(binding.value, WDL.Value.Int)
            else binding
        )

        # The internal WDL.Env.Bindings[WDL.Value.Base] structure can be converted to and from a
        # Cromwell-style JSON dict as follows:
        #
        #     inputs_dict = WDL.values_to_json(recv["inputs"])
        #     ...
        #     recv["inputs"] = WDL.values_from_json(inputs_dict, task.available_inputs)
        #
        # However, notice the JSON representation tends to conflate String and File values -- to
        # distinguish them precisely, you have to consult the type information in
        # task.available_inputs, recursing into nested/compound types e.g. Array[Pair[Int,File]].

        # Now we yield the manipulated inputs, which the runner uses for evaluation of WDL
        # expressions and the task command.
        recv = yield recv

        # We receive back a dict with the following keys:
        #   "command" : str, the evaluated task command
        #   "runtime" : Dict[str, Any], the evaluated task runtime section
        #   "container" : WDL.runtime.task.TaskContainer, interface to the container implementation

        # Let's prepend "set -euxo pipefail" to all task commands, which isn't a bad idea actually!
        recv["command"] = "set -euxo pipefail\n\n" + recv["command"]
        # Set custom keyword arguments for the docker swarm service create() invocation. see:
        #   https://docker-py.readthedocs.io/en/stable/services.html#docker.models.services.ServiceCollection.create
        # the dict supplied here will be merged into the runner-generated dict of arguments
        recv["container"].create_service_kwargs = {"env": ["OMNIBUS=123"]}

        # Yield this back, and the runner will start scheduling the container+command
        recv = yield recv

        # In the final round, we receive "outputs" : WDL.Env.Bindings[WDL.Value.Base], after the
        # task finishes successfully. We yield back the final outputs.
        recv["outputs"] = recv["outputs"].bind(
            "injected_output", WDL.Value.String("we're no strangers to love")
        )
        yield recv

        # Important: the coroutine must follow the bidirectional communication protocol outlined
        # here, with exactly three yield statements. If it doesn't elect to manipulate anything on
        # a given round, it should just yield back what it was given.

    except WDL.runtime.CommandFailed as exn:
        # Here we're notified that the task command exited with a nonzero status code (the most
        # common, but not only, cause of task failure). We can manipulate the exception or reraise
        # a different one (but we can't suppress the exception altogether).
        raise RuntimeError("never gonna give you up")
    finally:
        logger.critical(f"GOODBYE {task.name} {run_dir}")


"""
quick test:

pip3 install examples/plugin_task_omnibus/
echo -e 'version 1.0\ntask omnibus { input { Int n } command { echo "~{n}"; >&2 echo "OMNIBUS=$OMNIBUS"; if (( ~{n} == 43 )); then exit 1; fi } output { String out=read_string(stdout()) } }' > /tmp/omnibus.wdl
MINIWDL__PLUGINS__DISABLE_PATTERNS=[] python3 -m WDL run /tmp/omnibus.wdl n=0 --dir=/tmp --verbose
MINIWDL__PLUGINS__DISABLE_PATTERNS=[] python3 -m WDL run /tmp/omnibus.wdl n=1 --dir=/tmp --verbose

note: we override MINIWDL__PLUGINS__DISABLE_PATTERNS there because default.cfg includes a disable
      pattern for this example, to avoid inadvertently interfering with normal ops just from being
      installed.
"""
