# pyre-strict
# misc utility functions...

import os
import json
import logging
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
import coloredlogs

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


class CustomDeepCopyMixin:
    """
    Mixin class overrides __deepcopy__ to consult an internal set of attribute names to be merely
    shallow-copied when the time comes. Useful for attributes referencing large, immutable data
    structures.

    Override class variable _shallow_copy_attrs to a set of the attribute names to be
    shallow-copied.
    """

    _shallow_copy_attrs: Optional[Set[str]] = None

    def __deepcopy__(self, memo: Dict[int, Any]) -> Any:  # pyre-ignore
        cls = self.__class__
        cp = cls.__new__(cls)
        for k, v in self.__dict__.items():
            vcp = (
                copy.deepcopy(v, memo)
                if self._shallow_copy_attrs is None or k not in self._shallow_copy_attrs
                else v
            )
            setattr(cp, k, vcp)
        memo[id(self)] = cp
        return cp
