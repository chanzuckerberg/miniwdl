# pyre-strict
# misc utility functions...

import sys
import os
import json
import logging
import signal
import threading
import time
import copy
import fcntl
from time import sleep
from datetime import datetime
from contextlib import contextmanager, AbstractContextManager
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
    Generator,
    IO,
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
def write_atomic(contents: str, filename: str, end: str = "\n") -> None:
    tn = filename + ".tmp"
    with open(tn, "x") as outfile:
        print(contents, file=outfile, end=end)
    os.rename(tn, filename)


@export
def write_values_json(
    values_env: "Env.Bindings[Value.Base]", filename: str, namespace: str = ""
) -> None:
    from . import values_to_json

    write_atomic(
        json.dumps(values_to_json(values_env, namespace=namespace), indent=2),  # pyre-ignore
        filename,
    )


@export
def provision_run_dir(name: str, run_dir: Optional[str] = None) -> str:
    here = (
        (run_dir in [".", "./"] or run_dir.endswith("/.") or run_dir.endswith("/./"))
        if run_dir
        else False
    )
    run_dir = os.path.abspath(run_dir or os.getcwd())

    if here:
        os.makedirs(run_dir, exist_ok=True)
        return run_dir

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

LOGGING_FORMAT = "%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s"
__all__.append("LOGGING_FORMAT")


@export
def install_coloredlogs(logger: logging.Logger) -> None:
    level_styles = {}
    field_styles = {}

    if sys.stderr.isatty() and "NO_COLOR" not in os.environ:
        level_styles = dict(coloredlogs.DEFAULT_LEVEL_STYLES)
        level_styles["debug"]["color"] = 242
        level_styles["notice"] = {"color": "green", "bold": True}
        level_styles["error"]["bold"] = True
        level_styles["warning"]["bold"] = True
        level_styles["info"] = {}
        field_styles = dict(coloredlogs.DEFAULT_FIELD_STYLES)
        field_styles["asctime"] = {"color": "blue"}
        field_styles["name"] = {"color": "magenta"}

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
    pygtail = None
    if logger.getEffectiveLevel() <= VERBOSE_LEVEL:
        pygtail = Pygtail(filename, full_lines=True)

    def poll() -> None:
        nonlocal pygtail
        if pygtail:
            try:
                for line in pygtail:
                    logger.verbose((prefix + line.rstrip())[:4096])  # pyre-ignore
            except:
                # cf. https://github.com/bgreenlee/pygtail/issues/48
                logger.verbose(  # pyre-ignore
                    "incomplete log stream due to the following exception; see %s",
                    filename,
                    exc_info=sys.exc_info(),
                )
                pygtail = None

    try:
        yield poll
    finally:
        poll()


_terminating: Optional[bool] = None
_terminating_lock: threading.Lock = threading.Lock()


@export
@contextmanager
def TerminationSignalFlag(logger: logging.Logger) -> Iterator[Callable[[], bool]]:
    """
    Context manager installing termination signal handlers (SIGTERM, SIGQUIT, SIGINT, SIGHUP) which
    set a global flag indicating whether such a signal has been received. Yields a function which
    returns this flag.

    Should be opened on the main thread wrapping all the desired operations. Once this is so, more
    instances can be opened on any thread without interfering with each other, as long as they're
    nested within the main one.
    """
    signals = [
        signal.SIGTERM,
        signal.SIGQUIT,
        signal.SIGINT,
        signal.SIGHUP,
        signal.SIGUSR1,
        signal.SIGALRM,  # used in unit test
        # don't trap SIGPIPE -- Python has a default handler to generate BrokenPipeError
    ]

    def handle_signal(sig: int, frame: FrameType) -> None:
        global _terminating
        if not _terminating:
            if sig != signal.SIGUSR1:
                logger.critical(StructuredLogMessage("aborting workflow", signal=sig))
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
    convert strings like "2000", "4G", "1.5 TiB" to a positive number of bytes
    """

    s = s.strip()
    N = None
    unit = None
    for i in range(len(s)):
        if s[i].isdigit() or s[i] == ".":
            N = float(s[: i + 1])
            unit = s[i + 1 :].lstrip()
        else:
            break
    if N and unit:
        if unit in byte_size_units:
            N *= byte_size_units[unit]
        else:
            N = None
    if N is None or N < 0:
        raise ValueError("invalid byte size string, " + s)
    return int(N)


def splitall(path: str) -> List[str]:
    """
    https://www.oreilly.com/library/view/python-cookbook/0596001673/ch04s16.html
    """
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path:  # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts


@export
def path_really_within(lhs: str, rhs: str) -> bool:
    """
    After resolving symlinks, is path lhs either equal to or nested within path rhs?
    """
    lhs_cmp = splitall(os.path.realpath(lhs))
    rhs_cmp = splitall(os.path.realpath(rhs))
    return len(lhs_cmp) >= len(rhs_cmp) and lhs_cmp[: len(rhs_cmp)] == rhs_cmp


@export
def chmod_R_plus(path: str, file_bits: int = 0, dir_bits: int = 0) -> None:
    """
    recursive chmod to add permission bits (possibly different for files and subdirectiores)
    does not follow symlinks
    """

    def do1(path1: str, bits: int) -> None:
        assert 0 <= bits < 0o10000
        if path_really_within(path1, path):
            os.chmod(path1, (os.stat(path1).st_mode & 0o7777) | bits)

    if os.path.isdir(path):
        for root, subdirs, files in os.walk(path, followlinks=False):
            for dn in subdirs:
                do1(os.path.join(root, dn), dir_bits)
            for fn in files:
                do1(os.path.join(root, fn), file_bits)
    else:
        do1(path, file_bits)


@export
@contextmanager
def LoggingFileHandler(logger: logging.Logger, filename: str) -> Iterator[logging.FileHandler]:
    """
    Context manager which opens a logging.FileHandler and adds it to the logger; on exit, closes
    the log file and removes the handler.
    """
    fh = logging.FileHandler(filename)
    try:
        logger.addHandler(fh)
        yield fh
    finally:
        fh.flush()
        fh.close()
        logger.removeHandler(fh)


@export
class AtomicCounter:
    _value: int
    _lock: threading.Lock

    def __init__(self) -> None:
        self._value = 0
        self._lock = threading.Lock()

    def next(self) -> int:
        with self._lock:
            self._value += 1
            return self._value


@export
@contextmanager
def compose_coroutines(  # pyre-fixme
    generators: List[Callable[[Any], Generator[Any, Any, None]]], x: Any  # pyre-fixme
) -> Iterator[Generator[Any, Any, None]]:
    """
    Coroutine (generator) which composes several other coroutines to run in lockstep for one or
    more "rounds." On each round, caller sends a value, which is sent to the first coroutine; the
    value it yields is sent to the second coroutine; and so on until finally the value yielded by
    the last coroutine is yielded back to the caller. Exceptions propagate in the same way, so a
    coroutine can catch and manipulate (but not suppress) an exception raised by the caller or by
    one of the other coroutines.
    """

    def _impl() -> Generator[Any, Any, None]:  # pyre-fixme
        # start the coroutines by invoking each generator and taking the first value it yields
        nonlocal x
        cors = []
        try:
            for gen in generators:
                cor = gen(x)
                x = next(cor)
                cors.append(cor)
            while True:  # GeneratorExit will break
                # yield to caller and get updated value back
                try:
                    x = yield x
                except Exception as exn:
                    for cor in cors:
                        try:
                            cor.throw(exn)
                        except Exception as exn2:
                            exn = exn2
                    raise exn
                # pass value through coroutines
                exn = None
                for cor in cors:
                    try:
                        if not exn:
                            x = cor.send(x)
                        else:
                            cor.throw(exn)
                    except Exception as exn2:
                        exn = exn2
                if exn:
                    raise exn
        finally:
            close_exn = None
            for cor in cors:
                try:
                    cor.close()
                except Exception as exn2:
                    close_exn = close_exn or exn2
            if close_exn:
                raise close_exn

    # this outer contextmanager is for closing the coroutines promptly and propagating any caller
    # exceptions back through them. see: https://stackoverflow.com/a/58854646
    chain = _impl()
    try:
        yield chain
    except Exception as exn:
        chain.throw(exn)  # pyre-ignore
        raise
    finally:
        chain.close()


@export
class FlockHolder(AbstractContextManager):
    """
    Context manager exposing a method to take an advisory lock on a file (flock) and hold it until
    context exit. The context manager is reentrant; locks are released upon exit of the outermost
    nested context.
    """

    _lock: threading.Lock
    _flocks: Dict[str, Tuple[IO[Any], bool]]
    _entries: int
    _logger: logging.Logger

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._lock = threading.Lock()
        self._flocks = {}
        self._entries = 0
        self._logger = (
            logger.getChild("FlockHolder") if logger else logging.getLogger("FlockHolder")
        )

    def __enter__(self) -> "FlockHolder":
        assert self._entries > 0 or not self._flocks
        self._entries += 1
        return self

    def __exit__(self, *exc_details) -> None:  # pyre-fixme
        assert self._entries > 0, "FlockHolder context exited prematurely"
        self._entries -= 1
        if self._entries == 0:
            exn = None
            with self._lock:
                for fn, (fh, exclusive) in self._flocks.items():
                    self._logger.debug(StructuredLogMessage("close", file=fn, exclusive=exclusive))
                    try:
                        fh.close()
                    except Exception as exn2:
                        exn = exn or exn2
                self._flocks = {}
            if exn:
                raise exn

    def __del__(self) -> None:
        assert self._entries == 0 and not self._flocks, "FlockHolder context was not exited"

    def flock(  # pyre-fixme
        self,
        filename: str,
        mode: str = "",
        exclusive: bool = False,
        wait: bool = False,
        update_atime: bool = False,
    ) -> IO[Any]:
        """
        Open a file and an advisory lock on it. The file is closed and the lock released upon exit
        of the outermost context. Returns the open file, which the caller shouldn't close (this is
        taken care of).

        :param filename: file to open & lock
        :param mode: open() mode, default: "r+b" if exclusive else "rb"
        :param exclusive: True to open an exclusive lock (default: shared lock)p
        :param wait: True to wait as long as needed to obtain the lock, otherwise (default) raise
                     OSError if the lock isn't available immediately. Self-deadlock is possible;
                     see Python fcntl.flock docs for further details.
        :param update_atime: True to 'touch -a' the file after obtaining the lock
        """
        assert self._entries, "FlockHolder.flock() used out of context"
        while True:
            realfilename = os.path.realpath(filename)
            with self._lock:  # only needed to synchronize self._flocks
                if realfilename in self._flocks and not exclusive:
                    self._logger.debug(
                        StructuredLogMessage(
                            "reuse prior flock",
                            filename=filename,
                            realpath=realfilename,
                            exclusive=self._flocks[realfilename][1],
                        )
                    )
                    return self._flocks[realfilename][0]
                openfile = open(realfilename, mode or ("r+b" if exclusive else "rb"))
                try:
                    op = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
                    if not wait:
                        op |= fcntl.LOCK_NB
                    self._logger.debug(
                        StructuredLogMessage(
                            "flock",
                            file=filename,
                            realpath=realfilename,
                            exclusive=exclusive,
                            wait=wait,
                        )
                    )
                    fcntl.flock(openfile, op)
                    # the flock will release whenever we ultimately openfile.close()

                    file_st = os.stat(openfile.fileno())
                    if update_atime:
                        os.utime(
                            openfile.fileno(), ns=(int(time.time() * 1e9), file_st.st_mtime_ns)
                        )

                    # The filename link could have been replaced or removed in the instant between
                    # our open() and flock() syscalls.
                    # - if it was removed, the following os.stat will trigger FileNotFoundError,
                    #   which is reasonable to propagate.
                    # - if it was replaced, the subsequent condition won't hold, and we'll loop
                    #   around to try again on the replacement file.
                    filename_st = os.stat(realfilename)
                    self._logger.debug(
                        StructuredLogMessage(
                            "flocked",
                            file=filename,
                            realpath=realfilename,
                            exclusive=exclusive,
                            name_inode=filename_st.st_ino,
                            fd_inode=file_st.st_ino,
                        )
                    )
                    if (
                        filename_st.st_dev == file_st.st_dev
                        and filename_st.st_ino == file_st.st_ino
                    ):
                        assert realfilename not in self._flocks
                        self._flocks[realfilename] = (openfile, exclusive)
                        return openfile
                except:
                    openfile.close()
                    raise
                openfile.close()
