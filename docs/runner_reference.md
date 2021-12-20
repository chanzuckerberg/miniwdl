# `miniwdl run` reference

## I/O and run directory structure

By default `miniwdl run` creates a new subdirectory of the current working directory for each run, timestamp-named so that a series of them sorts in order of invocation. A different root directory can be specified with `--dir /my/runs`, while timestamp-named subdirectory creation can be prevented with `--dir myrun/.`; in the latter case, the given directory should be nonexistent or empty.

Upon run success, the JSON outputs are written to `outputs.json` in the run directory and printed to standard output (including a key providing the run directory path). Upon failure, the process exits with a non-zero status code and JSON error information is written to `error.json` in the run directory; if `--error-json` is supplied, this information is also printed to standard output.

The existence of `outputs.json` is a reliable indicator of success; `error.json` indicates failure, but may not appear if miniwdl is force-killed (by SIGKILL, such as the out-of-memory killer), or if writing the file fails for any reason. Barring such extremes, an external process should assume a run directory is still in use if it contains neither `outputs.json` nor `error.json`.

For tasks, the run directory also contains:

* `task.log`
* `write_/` with any files written during the evaluation of WDL expressions (e.g. `write_lines()`)
* `download/` with any files downloaded from URIs in task inputs
* `work/` the working directory mounted into the task container, where the command leaves its output files
* `stdout.txt` and `stderr.txt` from the task command, streamed as it runs.
* `out/` if the task succeeded, symbolic links to the individual output files, organized in a directory tree reflecting the WDL output declarations

For workflows,

* `workflow.log`
* `write_/` and `download/` as above
* subdirectories for each call to a task or sub-workflow, each structured similarly
* `out/` with links reaching into the call subdirectories where each output file was generated

The top-level run directory also contains:

* `wdl/` a copy of the original WDL that was run, including imported documents (except any referenced by URI or absolute path)
* `rerun` can be "sourced" to run the WDL (as found in the original location, possibly updated) using the same inputs

When miniwdl creates a new timestamp-named subdirectory for a run, it also creates a symbolic link `_LAST` to it in the same parent directory. (For convenience referring to the most recent run; should not be relied upon if multiple runs can start concurrently.)

The miniwdl source repository includes several [example scripts](https://github.com/chanzuckerberg/miniwdl/blob/main/examples) illustrating how the structures described here can inform production automation (e.g. retrieving error messages, uploading output files).

## Task runtime attributes

The default local scheduler observes these task `runtime {}` attributes:

* `docker`/`container` (String): docker image tag used to instantiate container; if omitted, a default image is specified in the miniwdl configuration option `[task_runtime] defaults` (currently `ubuntu:20.04`)
* `cpu` (Int): container reserves, and is throttled to, this many CPUs
  * Automatically rounds down to all host CPUs, if fewer
  * Multiple tasks can run concurrently on the local host, if CPUs and memory are available to meet their total reservations, and the workflow dependencies allow
* `memory` (Int/String): container reserves this many bytes of memory, or string with unit such as "8 GiB"
  * Automatically rounds down to all host memory, if less
  * The memory reservation informs scheduling, but isn't an enforced limit unless the configuration option `[task_runtime] memory_limit_multiplier` is set
* `maxRetries` (Int): retry failing tasks up to this many additional attempts (after the first)
* `returnCodes` (Int|Array[Int]|"*"): consider the given non-zero exit code(s) to indicate command success
* `privileged` (Boolean): if true, *and* configuration option `[task_runtime] allow_privileged = true`, then run task containers with privileged capabilities. (Not recommended, for security & portability reasons.)

## File & Directory URI downloads

Instead of local paths for File and Directory inputs, miniwdl can accept URIs and download them automatically on run start. The following URI schemes have built-in support, which can be extended with plugins:

* `http:`, `https:`, and `ftp:` downloads for Files
* Amazon S3 `s3:` URIs for both File and Directory inputs
  * On an EC2 instance, the downloader attempts to assume an [attached IAM role](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html) by contacting the [instance metadata service](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#instance-metadata-security-credentials)
  * Outside EC2, to use AWS credentials from the invoking session, set the configuration option `[download_awscli] host_credentials = true` or environment `MINIWDL__DOWNLOAD_AWSCLI__HOST_CREDENTIALS=true` (requires [boto3](https://aws.amazon.com/sdk-for-python/) package installed if not already)
  * Affix a trailing slash for Directory inputs
* Google Cloud Storage `gs:` URIs for Files
  * On a GCE instance, the downloader attempts to use the [associated service account](https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances) by contacting the [instance metadata service](https://cloud.google.com/compute/docs/storing-retrieving-metadata)

## Configuration

The miniwdl runner's configuration loader sources from command-line options, environment variables, and a configuration file, in that priority order.

**default.cfg**

The available configuration options are exemplified in [`WDL/runtime/config_templates/default.cfg`](https://github.com/chanzuckerberg/miniwdl/blob/main/WDL/runtime/config_templates/default.cfg), which follows the format of [Python's configparser module](https://docs.python.org/3/library/configparser.html#quick-start), organized into `[SECTION]` headings with `KEY = VALUE` entries in each section. Some values take the form of a JSON object or array, which may span multiple lines with indentation.

Miniwdl loads these defaults from the locally installed copy of that file.

**Custom configuration files**

First step: `miniwdl configure` interactively drafts a configuration file with common options in the default location. Subsequently, running this command will display the effective configuration.

Upon starting, miniwdl looks for a custom configuration file in the following locations:

1. File named by `--cfg` command-line argument
2. File named by `MINIWDL_CFG` environment variable
3. `XDG_CONFIG_HOME/miniwdl.cfg` (usually `~/.config/miniwdl.cfg`)
4. `miniwdl.cfg` in [XDG_CONFIG_DIRS](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html) (usually `/etc/xdg/.config/miniwdl.cfg`)

Miniwdl loads *only the first file found* in this priority order, and merges its options into the defaults; so the file needs only contain selected sections & options to override. For example, the following overrides the default docker image (used when a task doesn't specify `runtime.docker`), leaving other defaults in place:

```
$ cat << 'EOF' > ~/.config/miniwdl.cfg
[task_runtime]
defaults = {
        "docker": "ubuntu:20.10"
    }
EOF
```

**Environment and command line**

Environment variables following the convention `MINIWDL__SECTION__KEY=VALUE` override individual cfg file and default options. Note the variable name is all-uppercase and delimited with *double* underscores (as section and key names may contain underscores). Reusing the previous example, the default docker image may be changed by setting in the environment:

```
MINIWDL__TASK_RUNTIME__DEFAULTS='{"docker":"ubuntu:20.10"}'
```

Any option can thus be set/changed temporarily without a configuration file.

`miniwdl run` command-line arguments override the other sources. If in doubt, running with `--debug` logs the effective configuration and sources.

### Call cache

Miniwdl can cache task & workflow outputs, reusing them for repeat calls with the same inputs, for example while debugging an intermediate task or sub-workflow, or resuming from a transient error. This functionality must be enabled in the configuration; the relevant options are listed in the [`default.cfg`](https://github.com/chanzuckerberg/miniwdl/blob/main/WDL/runtime/config_templates/default.cfg) template, ``[call_cache]`` section. A minimal configuration might include:

```
[call_cache]
put = true
get = true
dir = ~/.cache/miniwdl
```

Details:

* The call cache is keyed by opaque digests of (i) the WDL source code for each task/workflow, and (ii) the inputs given to it
* Cached outputs are stored as `*.json` files under the cache directory, which can simply be deleted when no longer needed
* Local File and Directory inputs & outputs are referenced at their original paths, not copied into the cache directory
* Cache entries are automatically invalidated if any referenced local File or Directory is later modified or deleted (based on modification timestamps)
  * However, the cache does NOT test whether downloaded URIs or docker images may have changed
* With the cache enabled in configuration, `--no-cache` disables it for one run

### Download cache

By default, downloaded URI inputs are used only for the current run, stored under the run directory. Miniwdl can also store downloads in a central directory, where subsequent runs (even of different tasks and workflows) can reference them by URI without downloading them again. This permits efficient use of WDL input templates referring to public databases by URI (e.g. reference genomes, sequence databases, interval lists), without having to rewrite them with local, non-portable paths.

The download cache must be enabled in the configuration; the relevant options are listed in the [`default.cfg`](https://github.com/chanzuckerberg/miniwdl/blob/main/WDL/runtime/config_templates/default.cfg) template, ``[download_cache]`` section. A minimal configuration might include:

```
[download_cache]
put = true
get = true
dir = /tmp/miniwdl_download_cache
```

Details:

* The cache is **keyed by URI**: a cached File or Directory is used if previously stored for the same URI. This doesn't depend on which task/workflow is running, and doesn't use timestamps or digests. Therefore, the cache should only be used with immutable remote content, or if there's no need for immediate coherence with remote changes.
* URIs can be excluded from caching using the "pattern" options, in which case they'll be downloaded under the current run directory. Typically, write the patterns to **include reusable reference data while excluding any run-specific inputs** that might be supplied as URIs.
* Cached content that's no longer needed can simply be **deleted from the cache directory**, once no longer in use by a running workflow.
* Miniwdl doesn't delete anything from the cache, but to support an **external cleanup process**, it updates the access timestamp (atime) and opens a shared `flock()` on any cached File or Directory it's using. The script [examples/clean_download_cache.sh](https://github.com/chanzuckerberg/miniwdl/blob/main/examples/clean_download_cache.sh) illustrates a process to shrink the cache to a desired maximum size, by evicting the least-recently used content that can be exclusively flocked (the latter condition needed only if the cleaner must run alongside concurrent workflows).
* If needed, the `miniwdl localize` subcommand can **"prime" the local cache** with URIs found in a given JSON input template (or a simple list of URIs) before actually running any workflow.
* With the cache enabled in configuration, `--no-cache` disables it for one run.

## WDL interoperability

The runner supports versions 1.1, 1.0, and draft-2 of the [WDL specification](https://github.com/openwdl/wdl), with known errata:

* `Object` type is unsupported except for initializing WDL 1.0+ `struct` types, which should be used instead.
  * The `read_object()` and `read_objects()` library functions are available *only* for initializing structs and `Map[String,String]`
* Task may only *output* files created within/beneath its container's initial working directory, not e.g. under `/tmp` ([#214](https://github.com/chanzuckerberg/miniwdl/issues/214))
* The following task runtime values are ignored: `disks` `gpu`
* Rejects certain name collisions that Cromwell admits (spec-ambiguous), such as between:
  * scatter variable and prior declaration
  * output declaration and prior non-output declaration
  * task and workflow in the same WDL file

Please [file an issue](https://github.com/chanzuckerberg/miniwdl/issues?q=is%3Aopen+is%3Aissue+label%3Ainterop) for any other incompatibilities observed.

Additionally, miniwdl's `version development` strives to implement [features and changes pending](https://github.com/openwdl/wdl/pulls?q=is%3Apr+is%3Aclosed) for the next specification release (best-effort & potentially unstable). The `Directory` type is a salient current example.
