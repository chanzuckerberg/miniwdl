# `miniwdl run` optimization

Follow these guidelines for optimal performance and reliability from `miniwdl run`.

## WDL guidelines

### Consolidate very short tasks

If your production workload uses many tasks that take only seconds each, consider restructuring the workflow to consolidate them. For example, if you're scattering over a large array of very small items, try processing the items in batches, with tasks internally parallelized using [GNU parallel](https://www.gnu.org/software/parallel/) or the like. There's overhead in scheduling and provisioning each task container, which makes it inefficient to deploy large numbers of very short tasks. 

### Avoid unnecessary large WDL data structures

Except as needed for workflow orchestration (e.g. scatter arrays), pass large datasets through Files instead of WDL data structures (Arrays, Maps, structs, etc.). Excessively large WDL data structures might bottleneck the workflow, as they're handled within the miniwdl orchestrator process.

Passing large `Array[File]` (more than dozens of files, regardless of their individual sizes) through task inputs can be especially costly, due to overhead localizing and mounting each file into the task's container. Instead, consider `Directory` inputs/outputs (available in the WDL `development` version), or consolidating the files into tar or zip archives to extract inside the task.

### Read-only input files

By default, miniwdl mounts task input files & directories read-only, blocking attempts to open them for writing, or more commonly, to move or rename them. Tasks that do so must be run with `--copy-input-files`, which should be avoided because it consumes more time and disk space.

If you just need to rename an input file to satisfy some tool's convention, try symlinking the desired name to the input file at the beginning of the task command script.

### Write scratch files under `$TMPDIR`

Scratch files (created but never output) should be written somewhere under `$TMPDIR` instead of the task's initial working directory. Common examples include intermediate files, external sorting buffers, and decompressed reference databases.

Output files should be written directly into the initial working directory, which might be on a network file system to facilitate downstream reuse.

### Scale runtime resources to input size

Miniwdl can schedule multiple task containers on one host concurrently, but only if it has enough CPU and memory resources to fulfill their total requirements (as declared in their runtime sections). If you run a workflow on small inputs (e.g. for testing) but the task runtime sections have hard-coded high CPU and memory requirements, the scheduler tends to serialize them unnecessarily.

Therefore, consider using WDL expressions to scale the runtime resource reservations to the input sizes:

```wdl
task res {
  input {
    Array[File]+ input_files

    Int cpu = if length(input_files) < 16 then length(input_files) else 16
    Int memGB = cpu*2
  }

  command {
    cat "~{write_lines(input_files)}" | xargs -i -P ~{cpu} echo {}
  }

  runtime {
    cpu: cpu
    memory: "~{memGB}GB"
  }
}
```

WDL's `size()` function to measure `File` size (or total `Array[File]` size) can also be useful here. Placing the expressions among the inputs allows the caller to override the reservations if needed.

### Control multithreading explicitly

With multiple concurrent containers, each one might "see" all of the host processors (for example, using `nproc` or Python `multiprocessing.cpu_count()`) even if its `runtime.cpu` reserved fewer. If the tasks use parallel tools that default their thread/process count to the detected processor count, they might cause an overload. Therefore, tasks should explicitly control their internal multithreading to match their reserved CPU count, as illustrated in the previous example with `xargs -P`.

### bash shell configuration

In any task whose command consists of a non-trivial script, `set -euxo pipefail` at the beginning to configure the bash shell in a way that [usually improves robustness and debuggability](https://vaneyckt.io/posts/safer_bash_scripts_with_set_euxo_pipefail/).

The record of invocations left by `set -x` goes well with `miniwdl run --verbose`, which includes them in the task log along with timestamps. You can also put arbitrary messages into this log by writing them into standard error, e.g. `>&2 echo "progress report"`

### Protect task command templates from script injection

WDL's task command placeholders (e.g. `~{some_value}`) are evaluated by text substitution into the template. This tends to admit the possibility of script injection if a malicious user has control of the interpolated value.

For example, consider the command template `echo ~{some_value}`. If a malicious user can set the task input `some_value` to `; cat /etc/passwd >&2` then the task will dump `/etc/passwd` into the log. (Or cause any other code to execute, potentially a serious vulnerability.)

Applications that must use untrusted inputs can mitigate this risk by:

1. Validating string and filename inputs before supplying them to miniwdl
2. Enclosing task command placeholders in single quotes `'~{some_value}'` *and* preventing values from containing single-quote marks
3. Consuming untrusted text through input File contents instead of String placeholders

A configuration option `[task_runtime] placeholder_regex` (environment `MINIWDL__TASK_RUNTIME__PLACEHOLDER_REGEX`) defines a POSIX regular expression for allowable placeholder values; it causes miniwdl to fail the evaluation of any task command template if the runtime value doesn't match. For example, it can be set to `[^']*` to fail if the value contains a single-quote mark, or to `[0-9A-Za-z:/._-]*` to permit only alphanumeric and filename/URL-like values. This blunt tool applies to *all* tasks, so prior input validation should be preferred where feasible.

## Host configuration

### Enable call and download caches

Miniwdl can cache task & workflow call outputs and downloaded URIs for reuse across multiple runs, but these features must be enabled in the [runner configuration](runner_reference.html#configuration).

### Use local disks for Docker storage

Docker images and containers should reside on fast local disks, rather than a network file system, to optimize container startup and scratch I/O performance. These typically reside under `/var/lib/docker`, which on a cloud instance would usually be on the network-attached root file system. Suppose your instance has a local scratch disk mounted to `/mnt`. You can [change the Docker storage location](https://linuxconfig.org/how-to-move-docker-s-default-var-lib-docker-to-another-directory-on-ubuntu-debian-linux) using a procedure like this:

```
systemctl stop docker    # or appropriate command to stop dockerd
mv /var/lib/docker /mnt
ln -s /mnt/docker /var/lib/docker
systemctl start docker
```

If the host has multiple scratch disks, consider first [assembling them into a RAID0 array](https://gist.github.com/joemiller/6049831) with all available space and IOPS.

### How to `miniwdl run` inside a Docker container

It's possible to operate `miniwdl run` inside a Docker container, with the following requirements:

1. The host's Docker socket must be mounted inside the container.
2. Input files and the workflow run directory must reside on the *host* file system, mounted inside the miniwdl container *at identical paths*.

For example from the host,

```bash
mkdir /tmp/wdl
echo Alice > /tmp/wdl/alice
echo 'version 1.0
      task hello {
        input {
          File who
        }
        command {
          echo "Hello, ~{read_string(who)}!" | tee message.txt
        }
        output {
          File message = "message.txt"
        }
      }' > /tmp/wdl/hello.wdl
docker run --rm -it -v /var/run/docker.sock:/var/run/docker.sock -v /tmp/wdl:/tmp/wdl continuumio/miniconda3 \
  bash -c 'conda install -c conda-forge -y miniwdl &&
    miniwdl run /tmp/wdl/hello.wdl who=/tmp/wdl/alice --dir /tmp/wdl'
```

If the input files aren't already located within the desired run directory, then it'd be necessary to mount them with additional `docker run -v` arguments.
