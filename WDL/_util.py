# pyre-strict
# misc utility functions...

from typing import Tuple, Dict, Set, Iterable
import WDL.Error


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


class AdjM:
    # A sparse adjacency matrix for topological sorting
    # which we should not have implemented ourselves
    _forward: Dict[int, Set[int]]
    _reverse: Dict[int, Set[int]]
    _unconstrained: Set[int]

    def __init__(self) -> None:
        self._forward = dict()
        self._reverse = dict()
        self._unconstrained = set()

    def sinks(self, source: int) -> Iterable[int]:
        for sink in self._forward.get(source, []):
            yield sink

    def sources(self, sink: int) -> Iterable[int]:
        for source in self._reverse.get(sink, []):
            yield source

    @property
    def nodes(self) -> Iterable[int]:
        for node in self._forward:
            yield node

    @property
    def unconstrained(self) -> Iterable[int]:
        for n in self._unconstrained:
            assert not self._reverse[n]
            yield n

    def add_node(self, node: int) -> None:
        if node not in self._forward:
            assert node not in self._reverse
            self._forward[node] = set()
            self._reverse[node] = set()
            self._unconstrained.add(node)
        else:
            assert node in self._reverse

    def add_edge(self, source: int, sink: int) -> None:
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

    def remove_edge(self, source: int, sink: int) -> None:
        if source in self._forward and sink in self._forward[source]:
            self._forward[source].remove(sink)
            self._reverse[sink].remove(source)
            if not self._reverse[sink]:
                self._unconstrained.add(sink)
        else:
            assert not (sink in self._reverse and source in self._reverse[sink])

    def remove_node(self, node: int) -> None:
        for source in list(self.sources(node)):
            self.remove_edge(source, node)
        for sink in list(self.sinks(node)):
            self.remove_edge(node, sink)
        del self._forward[node]
        del self._reverse[node]
        self._unconstrained.remove(node)


def detect_cycles(p: Tuple[Dict[int, WDL.Error.SourceNode], AdjM]) -> None:
    # Given a mapping of SourceNode's by their object id(), and an AdjM
    # representing their dependencies, detect if there exists a cycle and if
    # so, then raise WDL.Error.CircularDependencies with a relevant SourceNode.
    nodes, adj = p
    node = next(adj.unconstrained, None)  # pyre-ignore
    while node:
        adj.remove_node(node)
        node = next(adj.unconstrained, None)  # pyre-ignore
    node = next(adj.nodes, None)  # pyre-ignore
    if node:
        raise WDL.Error.CircularDependencies(nodes[node])
