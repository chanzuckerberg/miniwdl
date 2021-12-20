# Portions copied from AirflowConfigParser --
#  https://github.com/apache/airflow/blob/master/airflow/configuration.py
# Exposition --
#  https://medium.com/@tszumowski/delightful-designs-airflows-configuration-parser-1ef1a6b3d03c


import os
import configparser
import logging
import json
from fnmatch import fnmatchcase
from typing import Optional, List, Dict, Any, Callable, TypeVar, Set, Tuple, Iterable
from xdg import XDG_CONFIG_DIRS, XDG_CONFIG_HOME
from .._util import StructuredLogMessage as _

_T = TypeVar("_T")


class ConfigMissing(Exception):
    # configparser.No{Option,Section}Error have vague hard-coded error messages
    pass


class Section:
    _section: str
    _parent: "Loader"

    def __init__(self, parent: "Loader", section: str):
        self._parent = parent
        self._section = section

    def get(self, key: str) -> str:
        return self._parent.get(self._section, key)

    def __getitem__(self, key: str) -> str:
        return self.get(key)

    def get_int(self, key: str) -> int:
        return self._parent.get_int(self._section, key)

    def get_float(self, key: str) -> float:
        return self._parent.get_float(self._section, key)

    def get_bool(self, key: str) -> bool:
        return self._parent.get_bool(self._section, key)

    def get_dict(self, key: str) -> Dict[str, Any]:
        return self._parent.get_dict(self._section, key)

    def get_list(self, key: str) -> List[Any]:
        return self._parent.get_list(self._section, key)


class Loader:
    """
    Runtime configuration options, identified by section & key, are sourced in the following
    priority order:

    1. Supplied ``overrides`` dict, ``{"section": {"key": value}}``
    2. Environment variables ``MINIWDL__SECTION__KEY`` (uppercased with double-underscores)
    3. Custom configuration file (mutually exclusive):

       a. filename given to ``__init__``
       b. file named by environment variable ``MINIWDL_CFG``
       c. ``miniwdl.cfg`` in XDG_CONFIG_HOME or XDG_CONFIG_DIRS, usually ``~/.config/miniwdl.cfg``

    4. ``WDL/runtime/config_templates/default.cfg`` from installed package

    If ``filenames`` is an empty list, then no custom configuration file is used. Or ``filenames``
    may be a prioritized list of candidate filenames instead of (a-c) above; the first extant file
    is used, if any.
    """

    _logger: logging.Logger
    _defaults: configparser.ConfigParser
    _options: configparser.ConfigParser
    _overrides: configparser.ConfigParser
    _used: Set[Tuple[str, str]]
    _used_env: Set[str]

    cfg_filename: Optional[str] = None

    def __init__(
        self,
        logger: logging.Logger,
        filenames: Optional[List[str]] = None,
        overrides: Optional[Dict[str, Dict[str, str]]] = None,
    ):
        self._logger = logger
        self._used = set()
        self._used_env = set(["MINIWDL_CFG"])

        common_kws = {}

        self._defaults = configparser.ConfigParser(**common_kws)
        self._options = configparser.ConfigParser(**common_kws)
        self._overrides = configparser.ConfigParser(**common_kws)

        # load default.cfg
        default_cfg = os.path.join(os.path.dirname(__file__), "config_templates", "default.cfg")
        self._logger.debug(_("read configuration defaults", filename=default_cfg))
        self._defaults.read(default_cfg)

        # load miniwdl.cfg, if any
        if filenames is None:
            if "MINIWDL_CFG" in os.environ:
                filenames = os.environ["MINIWDL_CFG"].split(":")
            else:
                filenames = [
                    str(dn.joinpath("miniwdl.cfg"))
                    for dn in reversed(XDG_CONFIG_DIRS + [XDG_CONFIG_HOME])
                ]
        if filenames:
            self._logger.debug(_("searching for configuration files", filenames=filenames))
            filenames = [fn for fn in filenames if os.path.isfile(fn)]
            filenames = filenames[:1]
        if filenames:
            self._logger.info(_("read configuration file", path=filenames[0]))
            self._options.read(filenames)
            self.cfg_filename = filenames[0]
        else:
            self._logger.info("no configuration file found")

        # load overrides
        if overrides:
            self.override(overrides)

    def override(self, options: Dict[str, Dict[str, Any]]) -> None:
        options2 = {}
        for section in options:
            if options[section]:
                options2[section] = {}
                for key in options[section]:
                    v = options[section][key]
                    if isinstance(v, (list, dict, bool)) or v is None:
                        options2[section][key] = json.dumps(v)
                    else:
                        options2[section][key] = str(v)
        if options2:
            self._logger.debug(_("applying configuration overrides", **options2))
            self._overrides.read_dict(options2)

    def get(self, section: str, key: str) -> str:
        section = str(section).lower()
        key = str(key).lower()
        common_kws = {}
        ans = None

        # first check overrides
        if self._overrides.has_option(section, key):
            ans = self._overrides.get(section, key, **common_kws)
        else:
            # ...then environment
            env_key = _env_var_name(section, key)
            if env_key in os.environ:
                ans = os.environ[env_key]
                self._used_env.add(env_key)
            # ...then the config file
            elif self._options.has_option(section, key):
                ans = self._options.get(section, key, **common_kws)
            # ...then the default config
            elif self._defaults.has_option(section, key):
                ans = self._defaults.get(section, key, **common_kws)

        if ans is None:
            if not self.has_section(section):
                raise ConfigMissing(f"missing config section [{section}]")
            raise ConfigMissing(f"missing config option [{section}] {key}")

        self._used.add((section, key))
        ans = _strip(ans)
        if (section, key) not in [("task_runtime", "placeholder_regex")]:
            # expand environment variables unless in a hard-coded list of exceptions
            ans = _expand_env_var(ans)
        return ans

    def has_section(self, section: str) -> bool:
        return (
            self._defaults.has_section(section)
            or self._options.has_section(section)
            or self._overrides.has_section(section)
        )

    def has_option(self, section: str, option: str) -> bool:
        try:
            self.get(section, option)
            return True
        except ConfigMissing:
            return False

    def __getitem__(self, section: str) -> Section:
        return Section(self, section)

    def _parse(self, section: str, key: str, ty: str, parse: Callable[[str], _T]) -> _T:
        ans = self.get(section, key)
        try:
            return parse(ans)
        except:
            self._logger.debug(
                _(
                    "failed to parse configuration option",
                    section=section,
                    key=key,
                    value=ans,
                    expected_type=ty,
                )
            )
            raise ValueError(f"configuration option [{section}] {key} should be {ty}")

    def get_int(self, section: str, key: str) -> int:
        return self._parse(section, key, "int", int)

    def get_float(self, section: str, key: str) -> float:
        return self._parse(section, key, "float", float)

    def get_bool(self, section: str, key: str) -> bool:
        return self._parse(section, key, "bool", _parse_bool)

    def get_dict(self, section: str, key: str) -> Dict[str, Any]:
        return self._parse(section, key, "JSON dict", _parse_dict)

    def get_list(self, section: str, key: str) -> List[Any]:
        return self._parse(section, key, "JSON list", _parse_list)

    def get_all(self, defaults=True):
        options = set()
        for section in self._overrides.sections():
            options |= set((section, key) for key in self._overrides.options(section))
        for section in self._options.sections():
            options |= set((section, key) for key in self._options.options(section))
        for section in self._defaults.sections():
            options |= set((section, key) for key in self._defaults.options(section))

        all = {}
        for (section, key) in options:
            value = self.get(section, key)
            default_value = None
            if not defaults and self._defaults.has_option(section, key):
                default_value = self._defaults.get(section, key)
                default_value = _expand_env_var(_strip(default_value))
            if defaults or value != default_value:
                all[section] = all.get(section, dict())
                all[section][key] = value

        return all

    def log_all(self):
        """
        Write a debug log message with all options
        """
        self._logger.debug(_("configuration", **self.get_all()))

    def log_unused_options(self):
        """
        After a run, log warnings about any options that were set, but were never accessed and
        don't correspond to any default option.
        """
        options = set()
        for section in self._overrides.sections():
            options |= set((section, key) for key in self._overrides.options(section))
        for section in self._options.sections():
            options |= set((section, key) for key in self._options.options(section))
        known_options = set()
        for section in self._defaults.sections():
            known_options |= set((section, key) for key in self._defaults.options(section))
        unused = options - self._used - known_options
        if unused:
            self._logger.warning(
                _("unused configuration", options=[f"{section}:{key}" for (section, key) in unused])
            )

        evs = set(k for k in os.environ if k.upper().startswith("MINIWDL_"))
        ev_unused = evs - self._used_env
        if ev_unused:
            self._logger.warning(_("unused environment", variables=list(ev_unused)))


def _strip(value):
    ans = value
    if ans:
        ans = value.strip()
        if len(ans) >= 2 and (
            (
                ans.startswith("'")
                and ans.endswith("'")
                or (ans.startswith('"') and ans.endswith('"'))
            )
        ):
            ans = ans[1:-1]
    return ans


def _expand_env_var(env_var: str) -> str:
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
    if interpolated == env_var:
        return interpolated
    return _expand_env_var(interpolated)


def _env_var_name(section: str, key: str) -> str:
    return f"MINIWDL__{section.upper()}__{key.upper()}"


def _parse_bool(v: str) -> bool:
    v = v.lower()
    if v in ("t", "y", "1", "true", "yes"):
        return True
    if v in ("f", "n", "0", "false", "no"):
        return False
    assert False


def _parse_dict(v: str) -> Dict[str, Any]:
    ans = json.loads(v)
    assert isinstance(ans, dict)
    return ans


def _parse_list(v: str) -> List[Any]:
    if not v.startswith("["):
        return [v]
    ans = json.loads(v)
    assert isinstance(ans, list)
    return ans


def default_plugins() -> "Dict[str,List[importlib_metadata.EntryPoint]]":
    import importlib_metadata  # delayed heavy import

    return {
        "file_download": [
            importlib_metadata.EntryPoint(
                group="miniwdl.plugin.file_download",
                name="s3",
                value="WDL.runtime.download:awscli_downloader",
            ),
            importlib_metadata.EntryPoint(
                group="miniwdl.plugin.file_download",
                name="gs",
                value="WDL.runtime.download:gsutil_downloader",
            ),
        ],
        "directory_download": [
            importlib_metadata.EntryPoint(
                group="miniwdl.plugin.directory_download",
                name="s3",
                value="WDL.runtime.download:awscli_directory_downloader",
            )
        ],
        "task": [],
        "workflow": [],
        "container_backend": [
            importlib_metadata.EntryPoint(
                group="miniwdl.plugin.container_backend",
                name="docker_swarm",
                value="WDL.runtime.backend.docker_swarm:SwarmContainer",
            ),
            importlib_metadata.EntryPoint(
                group="miniwdl.plugin.container_backend",
                name="singularity",
                value="WDL.runtime.backend.singularity:SingularityContainer",
            ),
            importlib_metadata.EntryPoint(
                group="miniwdl.plugin.container_backend",
                name="podman",
                value="WDL.runtime.backend.podman:PodmanContainer",
            ),
            importlib_metadata.EntryPoint(
                group="miniwdl.plugin.container_backend",
                name="udocker",
                value="WDL.runtime.backend.udocker:UdockerContainer",
            ),
        ],
        "cache_backend": [
            importlib_metadata.EntryPoint(
                group="miniwdl.plugin.cache_backend",
                name="dir",
                value="WDL.runtime.cache:CallCache",
            )
        ],
    }


def load_all_plugins(cfg: Loader, group: str) -> Iterable[Tuple[bool, Any]]:
    import importlib_metadata  # delayed heavy import

    defaults = default_plugins()

    assert group in defaults, group
    enable_patterns = cfg["plugins"].get_list("enable_patterns")
    disable_patterns = cfg["plugins"].get_list("disable_patterns")
    for plugin in defaults[group] + list(
        importlib_metadata.entry_points().get(f"miniwdl.plugin.{group}", [])
    ):
        enabled = next(
            (pat for pat in enable_patterns if fnmatchcase(plugin.value, pat)), False
        ) and not next((pat for pat in disable_patterns if fnmatchcase(plugin.value, pat)), False)
        yield (enabled, plugin)


def load_plugins(cfg: Loader, group: str) -> Iterable[Tuple[str, Callable[..., Any]]]:
    yield from (
        (plugin.name, plugin.load()) for enabled, plugin in load_all_plugins(cfg, group) if enabled
    )
