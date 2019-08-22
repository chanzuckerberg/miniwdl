# pyre-strict
# misc utility functions...

import sys
import os
import json
import logging
import signal
import threading
from time import sleep
from datetime import datetime
from contextlib import contextmanager
from typing import Tuple, Dict, Set, Iterable, Iterator, List, TypeVar, Generic, Optional, Callable
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
    level_styles = dict(coloredlogs.DEFAULT_LEVEL_STYLES)
    level_styles["debug"]["color"] = 242
    coloredlogs.install(
        level=logger.getEffectiveLevel(),
        logger=logger,
        level_styles=level_styles,
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
    """
    pygtail = Pygtail(filename, full_lines=True)
    pygtail_ok = True

    def poll() -> None:
        nonlocal pygtail_ok
        if pygtail_ok:
            try:
                for line in pygtail:
                    logger.verbose(prefix + line.rstrip())  # pyre-ignore
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
        info = client.info()
        if (
            "Swarm" in info
            and "LocalNodeState" in info["Swarm"]
            and info["Swarm"]["LocalNodeState"] == "inactive"
        ):
            logger.warning(
                "docker swarm is inactive on this host; performing `docker swarm init --advertise-addr 127.0.0.1 --listen-addr 127.0.0.1 --task-history-limit 0`"
            )
            client.swarm.init(
                advertise_addr="127.0.0.1", listen_addr="127.0.0.1", task_history_retention_limit=0
            )
    finally:
        client.close()


_terminating: Optional[bool] = None
_terminating_lock: threading.Lock = threading.Lock()


@export
@contextmanager
def TerminationSignalFlag(logger: logging.Logger) -> Iterator[Callable[[], bool]]:
    """
    Context manager which installs a handler for termination signals (SIGTERM, SIGINT, SIGHUP,
    SIGPIPE) that sets an internal flag. Yields a function indicating whether such signal has been
    received. Multiple concurrent handler contexts can be opened without interfering with each
    other, so long as one wraps all the others.
    """
    signals = [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGPIPE, signal.SIGALRM]

    def handle_signal(signal: int, frame: FrameType) -> None:
        global _terminating
        _terminating = True
        logger.critical("received termination signal {}".format(signal))

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
