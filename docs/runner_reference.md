# Runner reference

## I/O and run directory structure

By default `miniwdl run` creates a new subdirectory of the current working directory for each run, timestamp-named so that a series of them sorts in order of invocation. A different root directory can be specified with e.g. `--dir /my/runs`, while timestamp-named subdirectory creation can be suppressed with e.g. `--dir myrun/.`; in the latter case, the given directory should be nonexistent or empty.

Upon run success, the JSON outputs are written to `outputs.json` in the run directory and printed to standard output (including a key providing the run directory path). Upon failure, the process exits with a non-zero status code and JSON error information is written to `error.json` in the run directory; if `--error-json` is supplied, this information is also printed to standard output. The existence of `outputs.json` is a reliable indicator of success; `error.json` indicates failure, but may not appear if miniwdl is force-killed (e.g. by `kill -9` or the out-of-memory killer), or if writing the file fails for any reason.

For tasks, the run directory also contains:

* `task.log`
* `stdout.txt` and `stderr.txt` if the task container started
* `write_/` with any files written during the evaluation of WDL expressions (e.g. `write_lines()`)
* `download/` with any files downloaded from URIs in task inputs
* `work/` the working directory mounted into the task container, where the command leaves its output files
* `output_links/` if the task succeeded, symbolic links to the individual output files, organized in a directory tree mirroring the WDL output values & associated JSON structure

For workflows,

* `workflow.log`
* `write_/` and `download/` as above
* subdirectories for each call to a task or sub-workflow, each structured similarly
* `output_links/` with links reaching into the call subdirectories where each output file was generated

The miniwdl source repository includes several [example scripts](https://github.com/chanzuckerberg/miniwdl/tree/master/examples) illustrating production automation techniques (e.g. retrieving error messages, uploading output files) using these byproducts.

## Configuration

The miniwdl runner's configuration loader sources from command-line options, environment variables, and a configuration file, in that priority order. If in doubt, running with `--debug` logs the effective configuration and sources.

**default.cfg**

The available configuration options are exemplified in [`WDL/runtime/config_templates/default.cfg`](https://github.com/chanzuckerberg/miniwdl/blob/master/WDL/runtime/config_templates/default.cfg), which follows the format of [Python's configparser module](https://docs.python.org/3/library/configparser.html#quick-start), organized into `[SECTION]` headings with `KEY = VALUE` entries in each section. Some values take the form of a JSON object or array, which may span multiple lines with indentation.

Miniwdl loads these defaults from the locally installed copy of that file.

**Custom configuration files**

Upon starting, miniwdl looks for a custom configuration file in the following locations:

1. File named by `--cfg` command-line argument
2. File named by `MINIWDL_CFG` environment variable
3. `XDG_CONFIG_HOME/miniwdl.cfg` (typically `${HOME}/.config/miniwdl.cfg`)
4. `miniwdl.cfg` in [XDG_CONFIG_DIRS](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html) (typically `/etc/xdg/.config`)

Miniwdl loads *only the first file found* in this priority order, and merges its options into the defaults; so the file needs only contain selected sections & options to override.

**Environment and command line**

Environment variables following the convention `MINIWDL__SECTION__KEY=VALUE` override cfg file and default options. Note the variable name is all-uppercase and delimited with *double* underscores (as section and key names may contain underscores).

`miniwdl run` command-line arguments override the other sources.

## File download cache

Miniwdl automatically downloads input files supplied as URIs. It's also able to cache these downloads in a local directory, so that multiple workflow runs can reference files by URI without downloading them repeatedly. This permits efficient use of WDL input templates referring to public databases by URI (e.g. reference genomes, sequence databases, interval lists), without having to compromise portability by rewriting them with local paths.

The download cache functionality must be enabled in the configuration. The relevant options, exemplified in the [`default.cfg`](https://github.com/chanzuckerberg/miniwdl/blob/master/WDL/runtime/config_templates/default.cfg) template, are in the `download_cache` section, especially `put = true`, `get = true`, and `dir`. Additional options such as `ignore_query`, `enable_patterns`, and `disable_patterns` provide control over which URIs will be cached. If the cache is enabled in persistent configuration, then `--no-cache` disables it for one run.

Details:

* Enabling the cache changes where downloaded files are stored: if the cache is enabled, they're stored in the cache directory; otherwise, they're stored under the triggering run directory.
* The cache is keyed by URI: when a workflow starts with a URI file input, a cached file is used if previously stored for the same URI. This doesn't depend on which task/workflow is running, and doesn't use checksums or timestamps of the file contents. Therefore, the cache should only be used with immutable remote files, or if there's no need for immediate coherence with remote content changes.
* Miniwdl doesn't internally manage the cache's total storage usage; but to support such processes, it updates the access timestamp (atime) and opens a shared `flock()` on any cached file it's using. The script [examples/clean_download_cache.sh](https://github.com/chanzuckerberg/miniwdl/blob/master/examples/clean_download_cache.sh) illustrates a process to evict the least-recently used cache files that can be exclusively flocked (the latter condition needed only if the cleaner must run alongside concurrent workflows).
