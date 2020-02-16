# Runner configuration

The miniwdl runner's configuration loader sources from command-line options, environment variables, and a configuration file, in that priority order. The available configuration is divided into *sections* for organization, and one or more *keys* in each section naming individual options.

### default.cfg

The configuration file format and the available options are exemplified in [`WDL/runtime/config_templates/default.cfg`](https://github.com/chanzuckerberg/miniwdl/blob/master/WDL/runtime/config_templates/default.cfg). Miniwdl loads these defaults from the locally installed copy of that file.  The file is organized into `[SECTION]` headings and `KEY = VALUE` entries in each section.

### Custom configuration files

Miniwdl looks for a configuration file overriding selected options in the following priority order:

1. File named by `--cfg` command-line argument
2. File named by `MINIWDL_CFG` environment variable
3. `XDG_CONFIG_HOME/miniwdl.cfg` (typically `$HOME/.config/miniwdl.cfg`)
4. `miniwdl.cfg` in [XDG_CONFIG_DIRS](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html) (typically `/etc/xdg/.config`)

Miniwdl loads the first such file found and merges the options into the defaults. It does *not* merge options from multiple non-default configuration files.

### Environment variables

Environment variables can override individual options with the convention `MINIWDL__SECTION__KEY=VALUE`. Note the variable name is all-uppercase and delimited with double underscores (as section and key names may have underscores).

Environment variables override any options loaded from a configuration file, and in turn command-line options to `miniwdl run` override all other configuration sources. If in doubt, running with `--debug` logs the effective configuration and sources.

