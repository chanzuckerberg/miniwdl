# Runner configuration

The miniwdl runner's configuration loader sources from command-line options, environment variables, and a configuration file, in that priority order. The available configuration is divided into *sections* for organization, and one or more *keys* in each section naming individual options.

**default.cfg**

The configuration file format and the available options are exemplified in [`WDL/runtime/config_templates/default.cfg`](https://github.com/chanzuckerberg/miniwdl/blob/master/WDL/runtime/config_templates/default.cfg). Miniwdl loads these defaults from the locally installed copy of that file.  The file is organized into `[SECTION]` headings and `KEY = VALUE` entries in each section.

**Custom configuration files**

Miniwdl looks for a configuration file overriding selected options in the following priority order:

1. File named by `--cfg` command-line argument
2. File named by `MINIWDL_CFG` environment variable
3. `XDG_CONFIG_HOME/miniwdl.cfg` (typically `$HOME/.config/miniwdl.cfg`)
4. `miniwdl.cfg` in [XDG_CONFIG_DIRS](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html) (typically `/etc/xdg/.config`)

Miniwdl loads the first such file found and merges the options into the defaults. It does *not* merge options from multiple non-default configuration files.

**Environment variables**

Environment variables can override individual options with the convention `MINIWDL__SECTION__KEY=VALUE`. Note the variable name is all-uppercase and delimited with double underscores (as section and key names may have underscores).

Environment variables override any options loaded from a configuration file, and in turn command-line options to `miniwdl run` override all other configuration sources. If in doubt, running with `--debug` logs the effective configuration and sources.

### File download cache

Miniwdl automatically downloads input files supplied as URIs. It's also able to cache these downloads in a local directory, so that multiple workflow runs can reference files by URI without downloading them repeatedly. This permits efficient use of WDL input templates referring to public databases by URI (e.g. reference genomes, sequence databases, interval lists), without having to compromise portability by rewriting them with local paths.

The download cache functionality must be enabled in the configuration. The relevant options, exemplified in the [`default.cfg`](https://github.com/chanzuckerberg/miniwdl/blob/master/WDL/runtime/config_templates/default.cfg) template, are in the `download_cache` section, especially `put = true`, `get = true`, and `dir`. Additional options such as `ignore_query`, `enable_patterns`, and `disable_patterns` provide control over which URIs will be cached.

Details about the file download cache:

* Enabling the cache changes where downloaded files are stored: if the cache is enabled, they're stored in the cache directory; otherwise, they're stored under the triggering run directory.
* The cache is keyed by URI: when a workflow starts with a URI file input, a cached file is used if previously stored for the same URI. This doesn't depend on which task/workflow is running, and doesn't use checksums or timestamps of the file contents. Therefore, the cache should only be used with immutable remote files, or if there's no need for immediate coherence with remote content changes.
* Miniwdl doesn't internally manage the cache's total storage usage; but to support such processes, it updates the access timestamp (atime) and opens a shared `flock()` on any cached file it's using. The script [examples/clean_download_cache.sh](https://github.com/chanzuckerberg/miniwdl/blob/master/examples/clean_download_cache.sh) illustrates a process to evict the least-recently used cache files that can be exclusively flocked (the latter condition needed only if the cleaner must run alongside concurrent workflows).
