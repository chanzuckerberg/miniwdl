# Runner reference

## I/O and run directory structure

By default `miniwdl run` creates a new subdirectory of the current working directory for each run, timestamp-named so that a series of them sorts in order of invocation. A different root directory can be specified with `--dir /my/runs`, while timestamp-named subdirectory creation can be prevented with `--dir myrun/.`; in the latter case, the given directory should be nonexistent or empty.

Upon run success, the JSON outputs are written to `outputs.json` in the run directory and printed to standard output (including a key providing the run directory path). Upon failure, the process exits with a non-zero status code and JSON error information is written to `error.json` in the run directory; if `--error-json` is supplied, this information is also printed to standard output. The existence of `outputs.json` is a reliable indicator of success; `error.json` indicates failure, but may not appear if miniwdl is force-killed (by SIGKILL, such as the out-of-memory killer), or if writing the file fails for any reason.

For tasks, the run directory also contains:

* `task.log`
* `write_/` with any files written during the evaluation of WDL expressions (e.g. `write_lines()`)
* `download/` with any files downloaded from URIs in task inputs
* `work/` the working directory mounted into the task container, where the command leaves its output files
* `stdout.txt` and `stderr.txt` from the task command, streamed as it runs.
* `output_links/` if the task succeeded, symbolic links to the individual output files, organized in a directory tree mirroring the WDL output values & associated JSON structure

For workflows,

* `workflow.log`
* `write_/` and `download/` as above
* subdirectories for each call to a task or sub-workflow, each structured similarly
* `output_links/` with links reaching into the call subdirectories where each output file was generated

The top-level run directory also contains:

* `wdl/` a copy of the original WDL that was run, including imported documents (except any referenced by URI or absolute path)
* `rerun` can be "sourced" to run the WDL (as found in the original location, possibly updated) using the same inputs
* The top-level `workflow.log` file is "flocked" while `miniwdl run` is still in progress (`task.log` if running a task directly)

The miniwdl source repository includes several [example scripts](https://github.com/chanzuckerberg/miniwdl/tree/master/examples) illustrating how this structure can inform production automation (e.g. retrieving error messages, uploading output files).

## Configuration

The miniwdl runner's configuration loader sources from command-line options, environment variables, and a configuration file, in that priority order.

**default.cfg**

The available configuration options are exemplified in [`WDL/runtime/config_templates/default.cfg`](https://github.com/chanzuckerberg/miniwdl/blob/master/WDL/runtime/config_templates/default.cfg), which follows the format of [Python's configparser module](https://docs.python.org/3/library/configparser.html#quick-start), organized into `[SECTION]` headings with `KEY = VALUE` entries in each section. Some values take the form of a JSON object or array, which may span multiple lines with indentation.

Miniwdl loads these defaults from the locally installed copy of that file.

**Custom configuration files**

Upon starting, miniwdl looks for a custom configuration file in the following locations:

1. File named by `--cfg` command-line argument
2. File named by `MINIWDL_CFG` environment variable
3. `XDG_CONFIG_HOME/miniwdl.cfg` (typically `${HOME}/.config/miniwdl.cfg`)
4. `miniwdl.cfg` in [XDG_CONFIG_DIRS](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html) (typically `/etc/xdg/.config/miniwdl.cfg`)

Miniwdl loads *only the first file found* in this priority order, and merges its options into the defaults; so the file needs only contain selected sections & options to override. For example, the following overrides the default docker image (used when a task doesn't specify `runtime.docker`), leaving other defaults in place:

```
$ cat << 'EOF' > ${HOME}/.config/miniwdl.cfg
[task_runtime]
defaults = {
        "docker": "ubuntu:19.10"
    }
EOF
```

**Environment and command line**

Environment variables following the convention `MINIWDL__SECTION__KEY=VALUE` override individual cfg file and default options. Note the variable name is all-uppercase and delimited with *double* underscores (as section and key names may contain underscores). Reusing the previous example, the default docker image may be changed by setting in miniwdl's environment:

```
MINIWDL__TASK_RUNTIME__DEFAULTS='{"docker":"ubuntu:19.10"}'
```

`miniwdl run` command-line arguments override the other sources. If in doubt, running with `--debug` logs the effective configuration and sources.

## File download cache

Miniwdl automatically downloads input files supplied as URIs instead of locally-mounted filenames. It's also able to cache these downloads in a local directory, so that multiple workflow runs can reference files by URI without downloading them repeatedly. This permits efficient use of WDL input templates referring to public databases by URI (e.g. reference genomes, sequence databases, interval lists), without having to compromise portability by rewriting them with local paths.

The download cache functionality must be enabled in the configuration. The relevant options, exemplified in the [`default.cfg`](https://github.com/chanzuckerberg/miniwdl/blob/master/WDL/runtime/config_templates/default.cfg) template, are in the `download_cache` section, especially `put = true`, `get = true`, and `dir`. Additional options such as `ignore_query`, `enable_patterns`, and `disable_patterns` provide control over which URIs will be cached. If the cache is enabled in persistent configuration, then `--no-cache` disables it for one run.

Details:

* The cache is **keyed by URI**: when a workflow starts with a URI file input, a cached file is used if previously stored for the same URI. This doesn't depend on which task/workflow is running, and doesn't use checksums or timestamps of the file contents. Therefore, the cache should only be used with immutable remote files, or if there's no need for immediate coherence with remote content changes.
* Enabling the cache changes **where downloaded files are stored**: if the cache is enabled, they're stored in the cache directory; otherwise, they're stored under the triggering run directory.
* URIs excluded from the cache by the enable/disable patterns fall back to being downloaded under the current run directory. Typically, write the patterns to **include reusable reference data while excluding any run-specific inputs** that might be supplied as URIs.
* If needed, the `miniwdl localize` subcommand can **"prime" the local cache** with URIs found in a given JSON input template (or a simple list of URIs) before actually running any workflow.
* Cached files that are no longer needed can simply be **deleted from the cache directory**, once they're no longer in use by a running workflow.
* Miniwdl itself doesn't delete files from the cache, but to support an **external cleanup process**, it updates the access timestamp (atime) and opens a shared `flock()` on any cached file it's using. The script [examples/clean_download_cache.sh](https://github.com/chanzuckerberg/miniwdl/blob/master/examples/clean_download_cache.sh) illustrates a process to shrink the cache to a desired maximum size, by evicting the least-recently used files that can be exclusively flocked (the latter condition needed only if the cleaner must run alongside concurrent workflows).
