# FAQ

:::{tip}
If you're new to WDL, you may want to review the open-source [learn-wdl course](https://github.com/openwdl/learn-wdl). It also includes a short [learn-miniwdl course](https://github.com/openwdl/learn-wdl/tree/master/6_miniwdl_course) with screencasts reviewing the concepts on this page in more detail.
:::

## Common Questions

### Install and verify miniwdl

- Q: Miniwdl won't install (or work) on my machine
  - Verify installation of Python 3.10+.
  - Verify installation of Docker.

- Q: Miniwdl isn't working on my Mac
  - Verify Docker version (17+) and user permission for Docker.
  - You may need to override the `TMPDIR` environment variable, e.g. `export TMPDIR=/tmp` to allow Docker containers to mount shared working directories.
  - Please [file any other issues](https://github.com/chanzuckerberg/miniwdl/issues) that arise!

- Q: The `miniwdl run` command won't run my WDL workflow
  - Each task runs in the Docker container image given in its `runtime`/`requirements` section, or a [configurable default image](runner_reference.md#task-requirements-attributes) if none is specified.
  - Verify that the image exists and supplies the tools used in the task command.

- Q: What is the quickest way to verify that miniwdl is properly installed?
  - Run `miniwdl --help` and review the list of possible commands.

---

### Run WDL workflow with the `miniwdl run` command

- Q: What is the simplest way to test running a WDL workflow on miniwdl?
  - Run `miniwdl run_self_test` - it includes an example workflow.
  - Verify that it concludes with `miniwdl run_self_test OK`.

- Q: How can I run a quick `hello.wdl` WDL workflow?
  - Copy [this file](https://github.com/openwdl/learn-wdl/blob/master/6_miniwdl_course/1_hello.wdl) to your miniwdl development environment.
  - Run `miniwdl run hello.wdl`; this quick test has no input parameters.

- Q: Where is the output from a miniwdl run?
  - Miniwdl produces workflow and task logs as well as task outputs.
  - See the [run directory structure](runner_reference.md#io-and-run-directory-structure) generated for each run.

- Q: What does `miniwdl run myfile.wdl` do?
  - If any required inputs are missing, then it prints a list of the inputs and outputs, noting which inputs are required.

- Q: How do I assign values to input variables when I use miniwdl to run a WDL file?
  - You can assign them directly on the command line, e.g. `miniwdl run myfile.wdl input1=input.bam input2=input.bai`
  - Or you can use the `--input` flag to supply input values in an `input.json` file

- Q: How can I get more information about job execution?
  - Run `miniwdl run myfile.wdl --verbose`

- Q: What is the `_LAST` directory?
  - Miniwdl generates a symbolic link `_LAST` pointing to the timestamped subdirectory for the most recent run,
    which contains an `out` directory tree with symbolic links to the output files.

---

### Scaling up `miniwdl run`

- Q: Can miniwdl handle large-scale workloads?
  - A more-powerful host enables larger workloads, as it schedules WDL tasks in parallel up to the available CPUs & memory
  - The optimization guide has [host configuration suggestions](runner_advanced.md#host-configuration)

- Q: Can miniwdl run workflows on my cloud/cluster?
  - If you can factor the workload into separate workflow runs, submit each one as a job to run miniwdl on a powerful worker node.
  - [Alternate container runtimes](runner_backends.md) widen local compatibility
  - [External plugins](runner_backends.md#external-plugins) enable cloud & cluster integrations, with an API to add new ones.

---

### Check WDL syntax with the `miniwdl check` command

- Q: When I run `miniwdl check myfile.wdl` it doesn't produce any output
  - Parsing and type-checking can take a few seconds, especially for workflows with many imports. When finished, it prints an outline of the document with any warnings nested within; an outline with no warnings means none were found.

- Q: Do I have to fix all of the items listed in the results of `miniwdl check myfile.wdl`?
  - This command reports both warnings (which you may optionally fix or [suppress](check.rst)) and errors (which you must fix).

---

### Links

* [chanzuckerberg/miniwdl GitHub](https://github.com/chanzuckerberg/miniwdl/) where issues & contributions are welcome
* [openwdl/wdl GitHub](https://github.com/openwdl/wdl) for WDL spec & proposals
* [OpenWDL Slack #miniwdl](https://openwdl.slack.com/archives/C02JCRJU79T) for discussions
* [CZI Science Tech](https://tech.chanzuckerberg.com/scitech/) sponsors this project
