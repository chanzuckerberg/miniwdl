# pyre-strict
# misc utility functions...

import sys
import os
import json
import logging
import signal
import threading
import copy
from time import sleep
from datetime import datetime
from contextlib import contextmanager
from typing import (
    Tuple,
    Dict,
    Set,
    Iterable,
    Iterator,
    List,
    TypeVar,
    Generic,
    Optional,
    Callable,
    Any,
)
from types import FrameType
import coloredlogs
from pygtail import Pygtail
import docker

__all__: List[str] = []


def export(obj) -> str:  # pyre-ignore
    __all__.append(obj.__name__)
    return obj


@export
def strip_leading_whitespace(txt: str) -> Tuple[int, str]:
    # Given a multi-line string, determine the largest w such that each line
    # begins with at least w whitespace characters. Return w and the string
    # with w characters removed from the beginning of each line.
    lines = txt.split("\n")

    to_strip = None
    for line in lines:
        lsl = len(line.lstrip())
        if lsl:
            c = len(line) - lsl
            assert c >= 0
            if to_strip is None or to_strip > c:
                to_strip = c
            # TODO: do something about mixed tabs & spaces

    if not to_strip:
        return (0, txt)

    for i, line_i in enumerate(lines):
        if line_i.lstrip():
            lines[i] = line_i[to_strip:]

    return (to_strip, "\n".join(lines))


T = TypeVar("T")


@export
class AdjM(Generic[T]):
    # A sparse adjacency matrix for topological sorting
    # which we should not have implemented ourselves
    _forward: Dict[T, Set[T]]
    _reverse: Dict[T, Set[T]]
    _unconstrained: Set[T]

    def __init__(self) -> None:
        self._forward = dict()
        self._reverse = dict()
        self._unconstrained = set()

    def sinks(self, source: T) -> Iterable[T]:
        for sink in self._forward.get(source, []):
            yield sink

    def sources(self, sink: T) -> Iterable[T]:
        for source in self._reverse.get(sink, []):
            yield source

    @property
    def nodes(self) -> Iterable[T]:
        for node in self._forward:
            yield node

    @property
    def unconstrained(self) -> Iterable[T]:
        for n in self._unconstrained:
            assert not self._reverse[n]
            yield n

    def add_node(self, node: T) -> None:
        if node not in self._forward:
            assert node not in self._reverse
            self._forward[node] = set()
            self._reverse[node] = set()
            self._unconstrained.add(node)
        else:
            assert node in self._reverse

    def add_edge(self, source: T, sink: T) -> None:
        self.add_node(source)
        self.add_node(sink)
        if sink not in self._forward[source]:
            self._forward[source].add(sink)
            self._reverse[sink].add(source)
            if sink in self._unconstrained:
                self._unconstrained.remove(sink)
        else:
            assert source in self._reverse[sink]
            assert sink not in self._unconstrained

    def remove_edge(self, source: T, sink: T) -> None:
        if source in self._forward and sink in self._forward[source]:
            self._forward[source].remove(sink)
            self._reverse[sink].remove(source)
            if not self._reverse[sink]:
                self._unconstrained.add(sink)
        else:
            assert not (sink in self._reverse and source in self._reverse[sink])

    def remove_node(self, node: T) -> None:
        for source in list(self.sources(node)):
            self.remove_edge(source, node)
        for sink in list(self.sinks(node)):
            self.remove_edge(node, sink)
        del self._forward[node]
        del self._reverse[node]
        self._unconstrained.remove(node)


@export
def topsort(adj: AdjM[T]) -> List[T]:
    # topsort node IDs in adj (destroys adj)
    # if there's a cycle, raises err: StopIteration with err.node = ID of a
    # node involved in a cycle.
    ans = []
    node = next(adj.unconstrained, None)  # pyre-ignore
    while node:
        adj.remove_node(node)
        ans.append(node)
        node = next(adj.unconstrained, None)  # pyre-ignore
    node = next(adj.nodes, None)  # pyre-ignore
    if node:
        err = StopIteration()
        setattr(err, "node", node)
        raise err
    return ans


@export
def write_values_json(
    values_env: "Env.Bindings[Value.Base]", filename: str, namespace: str = ""
) -> None:
    from . import values_to_json

    with open(filename, "w") as outfile:
        print(
            json.dumps(values_to_json(values_env, namespace=namespace), indent=2),  # pyre-ignore
            file=outfile,
        )


@export
def provision_run_dir(name: str, run_dir: Optional[str] = None) -> str:
    run_dir = os.path.abspath(run_dir or os.getcwd())
    try:
        os.makedirs(run_dir, exist_ok=False)
        return run_dir
    except FileExistsError:
        if not os.path.isdir(run_dir):
            raise

    now = datetime.today()
    run_dir2 = os.path.join(run_dir, now.strftime("%Y%m%d_%H%M%S") + "_" + name)
    try:
        os.makedirs(run_dir2, exist_ok=False)
        return run_dir2
    except FileExistsError:
        pass

    while True:
        run_dir2 = os.path.join(
            run_dir,
            now.strftime("%Y%m%d_%H%M%S_") + str(int(now.microsecond / 1000)).zfill(3) + "_" + name,
        )
        try:
            os.makedirs(run_dir2, exist_ok=False)
            return run_dir2
        except FileExistsError:
            sleep(1e-3)


@export
class StructuredLogMessage:
    message: str
    kwargs: Dict[str, Any]

    # from https://docs.python.org/3.8/howto/logging-cookbook.html#implementing-structured-logging
    def __init__(self, _message: str, **kwargs) -> None:  # pyre-fixme
        self.message = _message
        self.kwargs = kwargs

    def __str__(self) -> str:
        return (
            f"{self.message} :: {', '.join(k+ ': ' + json.dumps(v) for k,v in self.kwargs.items())}"
        )


VERBOSE_LEVEL = 15
__all__.append("VERBOSE_LEVEL")
logging.addLevelName(VERBOSE_LEVEL, "VERBOSE")


def verbose(self, message, *args, **kws):  # pyre-fixme
    if self.isEnabledFor(VERBOSE_LEVEL):
        self._log(VERBOSE_LEVEL, message, args, **kws)


logging.Logger.verbose = verbose
NOTICE_LEVEL = 25
__all__.append("NOTICE_LEVEL")
logging.addLevelName(NOTICE_LEVEL, "NOTICE")


def notice(self, message, *args, **kws):  # pyre-fixme
    if self.isEnabledFor(NOTICE_LEVEL):
        self._log(NOTICE_LEVEL, message, args, **kws)


logging.Logger.notice = notice

LOGGING_FORMAT = "%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s"
__all__.append("LOGGING_FORMAT")


@export
def install_coloredlogs(logger: logging.Logger) -> None:
    level_styles = {}
    field_styles = {}

    if sys.stderr.isatty() and "NO_COLOR" not in os.environ:
        level_styles = dict(coloredlogs.DEFAULT_LEVEL_STYLES)
        level_styles["debug"]["color"] = 242
        level_styles["notice"] = {"color": "magenta"}
        level_styles["info"] = {}
        field_styles = None

    coloredlogs.install(
        level=logger.getEffectiveLevel(),
        logger=logger,
        level_styles=level_styles,
        field_styles=field_styles,
        fmt=LOGGING_FORMAT,
    )


@export
@contextmanager
def PygtailLogger(
    logger: logging.Logger, filename: str, prefix: str = "2| "
) -> Iterator[Callable[[], None]]:
    """
    Helper for streaming task stderr into logger using pygtail. Context manager yielding a function
    which reads the latest lines from the file and writes them into logger at verbose level. This
    function also runs automatically on context exit.

    Truncates lines at 4KB in case writer goes haywire.
    """
    pygtail = Pygtail(filename, full_lines=True)
    pygtail_ok = True

    def poll() -> None:
        nonlocal pygtail_ok
        if pygtail_ok:
            try:
                for line in pygtail:
                    logger.verbose((prefix + line.rstrip())[:4096])  # pyre-ignore
            except:
                pygtail_ok = False
                # cf. https://github.com/bgreenlee/pygtail/issues/48
                logger.verbose(  # pyre-ignore
                    "incomplete log stream due to the following exception; see %s",
                    filename,
                    exc_info=sys.exc_info(),
                )

    try:
        yield poll
    finally:
        poll()


@export
def ensure_swarm(logger: logging.Logger) -> None:
    client = docker.from_env()
    try:
        state = "(unknown)"
        while True:
            info = client.info()
            if "Swarm" in info and "LocalNodeState" in info["Swarm"]:
                state = info["Swarm"]["LocalNodeState"]

            # https://github.com/moby/moby/blob/e7b5f7dbe98c559b20c0c8c20c0b31a6b197d717/api/types/swarm/swarm.go#L185
            if state == "inactive":
                logger.warning(
                    "docker swarm is inactive on this host; performing `docker swarm init --advertise-addr 127.0.0.1 --listen-addr 127.0.0.1 --task-history-limit 0`"
                )
                client.swarm.init(
                    advertise_addr="127.0.0.1",
                    listen_addr="127.0.0.1",
                    task_history_retention_limit=0,
                )
            elif state == "active":
                break
            else:
                logger.notice(  # pyre-fixme
                    StructuredLogMessage("waiting for docker swarm to become active", state=state)
                )
                sleep(2)

        miniwdl_services = [
            d
            for d in [s.attrs for s in client.services.list()]
            if "Spec" in d and "Labels" in d["Spec"] and "miniwdl_run_id" in d["Spec"]["Labels"]
        ]
        if miniwdl_services:
            logger.warning(
                "docker swarm lists existing miniwdl-related services. This is normal if other miniwdl processes are running concurrently; otherwise, stale state could interfere with this run. To reset it, `docker swarm leave --force`"
            )
    finally:
        client.close()


_terminating: Optional[bool] = None
_terminating_lock: threading.Lock = threading.Lock()


@export
@contextmanager
def TerminationSignalFlag(logger: logging.Logger) -> Iterator[Callable[[], bool]]:
    """
    Context manager installing termination signal handlers (SIGTERM, SIGQUIT, SIGINT, SIGHUP,
    SIGPIPE) which set a global flag indicating whether such a signal has been received. Yields a
    function which returns this flag.

    Should be opened on the main thread wrapping all the desired operations. Once this is so, more
    instances can be opened on any thread without interfering with each other, as long as they're
    nested within the main one.
    """
    signals = [
        signal.SIGTERM,
        signal.SIGQUIT,
        signal.SIGINT,
        signal.SIGHUP,
        signal.SIGPIPE,
        signal.SIGALRM,
        signal.SIGUSR1,
    ]

    def handle_signal(sig: int, frame: FrameType) -> None:
        global _terminating
        if not _terminating:
            if sig != signal.SIGUSR1:
                logger.critical(StructuredLogMessage("ABORT", signal=sig))
            else:
                # SIGUSR1 comes from ourselves, as the signal to abort after something else has
                # already gone wrong
                logger.notice("aborting workflow")  # pyre-fixme
        _terminating = True

    global _terminating
    global _terminating_lock
    restore_signal_handlers = None
    with _terminating_lock:
        if _terminating is None:
            restore_signal_handlers = dict(
                (sig, signal.signal(sig, handle_signal)) for sig in signals
            )
            _terminating = False
    try:
        yield lambda: _terminating
    finally:
        if restore_signal_handlers:
            with _terminating_lock:
                for sig, handler in restore_signal_handlers.items():
                    signal.signal(sig, handler)
                _terminating = None


@export
class CustomDeepCopyMixin:
    """
    Mixin class overrides __deepcopy__ to consult an internal list of attribute names to be merely
    shallow-copied when the time comes. Useful for attributes referencing large, immutable data
    structures.

    Override class variable _shallow_copy_attrs to a list of the attribute names to be
    shallow-copied.
    """

    _shallow_copy_attrs: Optional[List[str]] = None

    def __deepcopy__(self, memo: Dict[int, Any]) -> Any:  # pyre-ignore
        cls = self.__class__
        cp = cls.__new__(cls)
        memo[id(self)] = cp
        for k in self._shallow_copy_attrs or []:
            v = self.__dict__[k]
            memo[id(v)] = v
        for k, v in self.__dict__.items():
            setattr(cp, k, copy.deepcopy(v, memo))
        return cp


byte_size_units = {
    "K": 1000,
    "KB": 1000,
    "Ki": 1024,
    "KiB": 1024,
    "M": 1000000,
    "MB": 1000000,
    "Mi": 1048576,
    "MiB": 1048576,
    "G": 1000000000,
    "GB": 1000000000,
    "Gi": 1073741824,
    "GiB": 1073741824,
    "T": 1000000000000,
    "TB": 1000000000000,
    "Ti": 1099511627776,
    "TiB": 1099511627776,
}


@export
def parse_byte_size(s: str) -> int:
    """
    convert strings like "2000", "4G", "1 TiB" to a positive number of bytes
    """

    s = s.strip()
    N = None
    unit = None
    for i in range(len(s)):
        if s[i].isdigit():
            N = int(s[: i + 1])
            unit = s[i + 1 :].lstrip()
        else:
            break
    if N and unit:
        N *= byte_size_units.get(unit, 0)
    if not N or N < 0:
        raise ValueError("invalid byte size string, " + s)
    return N
