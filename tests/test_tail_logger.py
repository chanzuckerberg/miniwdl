import codecs
import logging
import os
import subprocess
import sys
import tempfile
import textwrap
import time
import tracemalloc
import unittest
from unittest.mock import patch

import WDL._util as _util
from WDL._util import (
    TailLogger,
    VERBOSE_LEVEL,
    _tail_emit_lines,
    _tail_close,
    _TailStream,
    _TAIL_MAX_LINE,
    _TAIL_STOPPED,
)


class TestTailLogger(unittest.TestCase):
    def _make_logger(self):
        # name the logger after the running test: logging caches loggers globally, so a name that
        # isn't unique would let one test see another's handlers.
        logger = logging.getLogger(f"TailLoggerTest.{self.id()}")
        logger.setLevel(VERBOSE_LEVEL)
        logger.propagate = False
        for h in list(logger.handlers):
            logger.removeHandler(h)
        return logger

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".log", encoding="utf-8"
        )
        self.tmp.close()
        self.path = self.tmp.name
        open(self.path, "w").close()

    def tearDown(self):
        try:
            os.unlink(self.path)
        except FileNotFoundError:
            pass

    def _append(self, text):
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(text)

    def _append_bytes(self, data):
        with open(self.path, "ab") as fh:
            fh.write(data)

    def test_emits_complete_lines(self):
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("alpha\nbeta\n")
            poll()
            self.assertEqual(seen, ["alpha\n", "beta\n"])

    def test_buffers_partial_line(self):
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("hello ")
            poll()
            self.assertEqual(seen, [])
            self._append("world\n")
            poll()
            self.assertEqual(seen, ["hello world\n"])

    def test_flushes_on_context_exit(self):
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("line1\nline2\n")
        self.assertEqual(seen, ["line1\n", "line2\n"])

    def test_trailing_partial_not_emitted_on_exit(self):
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append):
            self._append("done\ntail-only")
        self.assertEqual(seen, ["done\n"])

    def test_file_missing_at_start(self):
        os.unlink(self.path)
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            poll()
            self.assertEqual(seen, [])
            with open(self.path, "w", encoding="utf-8") as fh:
                fh.write("hi\n")
            poll()
            self.assertEqual(seen, ["hi\n"])

    def test_incremental_reads_across_polls(self):
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("a\n")
            poll()
            self._append("b\nc\n")
            poll()
            self._append("d\n")
            poll()
        self.assertEqual(seen, ["a\n", "b\n", "c\n", "d\n"])

    def test_disabled_when_level_below_threshold(self):
        logger = self._make_logger()
        logger.setLevel(logging.CRITICAL)  # above VERBOSE_LEVEL
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("silent\n")
            poll()
        self.assertEqual(seen, [])

    def test_oversized_line_stops_stream_with_warning(self):
        logger = self._make_logger()
        warnings = []

        class H(logging.Handler):
            def emit(self, record):
                warnings.append(record)

        logger.addHandler(H())
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("ok\n" + ("x" * 5000) + "\nmore\n")
            poll()
            self._append("after-stop\n")
            poll()
        self.assertEqual(seen, ["ok\n"])
        self.assertTrue(any(r.levelno == logging.WARNING for r in warnings))

    def test_oversized_partial_without_newline(self):
        logger = self._make_logger()
        warnings = []

        class H(logging.Handler):
            def emit(self, record):
                warnings.append(record)

        logger.addHandler(H())
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("y" * 5000)
            poll()
        self.assertEqual(seen, [])
        self.assertTrue(any(r.levelno == logging.WARNING for r in warnings))

    def test_default_callback_logs_at_level(self):
        logger = self._make_logger()
        records = []

        class H(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger.addHandler(H())
        with TailLogger(logger, self.path) as poll:
            self._append("hello\n")
            poll()
        msgs = [(r.name, r.levelno, r.getMessage()) for r in records]
        self.assertIn(
            (logger.name + ".stderr", VERBOSE_LEVEL, "hello"),
            msgs,
        )

    def test_multibyte_char_split_across_polls(self):
        # "\U0001F600" (grinning face emoji) encodes to 4 UTF-8 bytes: f0 9f 98 80
        emoji = "\U0001F600"
        raw = emoji.encode("utf-8")
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append_bytes(raw[:2])  # writer only got halfway through the character
            poll()
            self.assertEqual(seen, [])  # nothing corrupted/emitted yet
            self._append_bytes(raw[2:] + b"\n")  # writer completes the character
            poll()
            self.assertEqual(seen, [emoji + "\n"])

    def test_multibyte_char_split_at_chunk_boundary(self):
        # Force TailLogger's internal read chunk size down to 3 bytes so that a single poll() call
        # must read the 4-byte emoji across two internal chunk reads, exercising the same
        # incremental-decode path as a torn read against a concurrently-writing file.
        emoji = "\U0001F600"
        text = "ab" + emoji + "cd\n"
        logger = self._make_logger()
        seen = []
        with patch.object(_util, "_TAIL_CHUNK_BYTES", 3):
            with TailLogger(logger, self.path, callback=seen.append) as poll:
                self._append(text)
                poll()
        self.assertEqual(seen, [text])

    def test_invalid_utf8_byte_replaced(self):
        # 0xFF is never valid in UTF-8 (lead or continuation byte); decoding must not raise, and
        # must deterministically substitute U+FFFD rather than corrupting/dropping the whole line.
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append_bytes(b"bad-\xff-bytes\n")
            poll()
        self.assertEqual(seen, ["bad-�-bytes\n"])

    def test_truly_incomplete_trailing_sequence_dropped_at_exit(self):
        # Writer dies after emitting only the first 2 of 3 bytes of "€" (EURO SIGN), with no
        # terminating newline ever. This must not raise, and the incomplete/unterminated data is
        # simply dropped -- same defined behavior as any other unterminated trailing partial line.
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append_bytes("€".encode("utf-8")[:2])
            poll()
        self.assertEqual(seen, [])

    def test_carriage_return_terminates_line(self):
        # a lone \r terminates a line, as it did under python universal newlines when the file was
        # read in text mode. otherwise a progress bar reads as one ever-growing line.
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("one\rtwo\rthree\n")
            poll()
        self.assertEqual(seen, ["one\n", "two\n", "three\n"])

    def test_crlf_normalized(self):
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("one\r\ntwo\r\n")
            poll()
        self.assertEqual(seen, ["one\n", "two\n"])

    def test_crlf_split_across_polls(self):
        # the \r arrives in one poll and its \n in the next; it must still read as one terminator,
        # not as a lone \r followed by an empty line.
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("one\r")
            poll()
            self.assertEqual(seen, [])  # ambiguous so far: held back
            self._append("\ntwo\n")
            poll()
        self.assertEqual(seen, ["one\n", "two\n"])

    def test_trailing_carriage_return_flushed_at_exit(self):
        # ...but if nothing more ever arrives, the held \r resolves to a terminator on context exit
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append):
            self._append("only\r")
        self.assertEqual(seen, ["only\n"])

    def test_long_progress_bar_does_not_stop_stream(self):
        # regression: >4KiB of \r-delimited progress output must not trip the oversized-line guard
        # and silently kill the rest of the task's stderr.
        logger = self._make_logger()
        warnings = []

        class H(logging.Handler):
            def emit(self, record):
                if record.levelno >= logging.WARNING:
                    warnings.append(record)

        logger.addHandler(H())
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("".join(f"progress {i}\r" for i in range(1000)))  # ~12KB, no \n
            poll()
            self._append("\nIMPORTANT ERROR MESSAGE\n")
            poll()
        self.assertEqual(warnings, [])
        # the final "progress 999\r" and the following "\n" read as one \r\n terminator, even
        # though they arrived in separate polls -- so 1000 progress lines, not 1000 + a blank
        self.assertEqual(len(seen), 1001)
        self.assertEqual(seen[0], "progress 0\n")
        self.assertEqual(seen[999], "progress 999\n")
        self.assertEqual(seen[-1], "IMPORTANT ERROR MESSAGE\n")

    def test_concurrent_writer_process(self):
        # a genuinely concurrent writer in a separate process, which flushes each line in two
        # pieces so that polls routinely land mid-line
        script = textwrap.dedent(
            """
            import sys, time
            with open(sys.argv[1], "a") as fh:
                for i in range(50):
                    fh.write(f"line {i}")
                    fh.flush()
                    time.sleep(0.002)
                    fh.write("\\n")
                    fh.flush()
            """
        )
        proc = subprocess.Popen([sys.executable, "-c", script, self.path])
        logger = self._make_logger()
        seen = []
        try:
            with TailLogger(logger, self.path, callback=seen.append) as poll:
                while proc.poll() is None:
                    poll()
                    time.sleep(0.005)
        finally:
            proc.wait()
        self.assertEqual(seen, [f"line {i}\n" for i in range(50)])

    def test_large_backlog_streams_in_bounded_memory(self):
        # a task that wrote a lot of stderr between polls must stream through in memory
        # proportional to the read chunk, not to the whole backlog
        line = "x" * 99 + "\n"
        backlog = 8 << 20
        self._append(line * (backlog // len(line)))
        logger = self._make_logger()
        count = [0]

        def cb(_line):
            count[0] += 1

        with TailLogger(logger, self.path, callback=cb) as poll:
            tracemalloc.start()
            try:
                poll()
                peak = tracemalloc.get_traced_memory()[1]
            finally:
                tracemalloc.stop()
        self.assertEqual(count[0], backlog // len(line))
        self.assertLess(peak, 2 << 20, "poll() buffered the whole backlog")

    def test_truncation_loses_data_known_limitation(self):
        # Known pre-existing limitation: we hold the file open and keep reading at our own
        # offset, so if the writer truncates the file out from under us (log rotation,
        # copytruncate) we silently skip whatever lands below that offset and resume
        # mid-stream. miniwdl's own writers only ever append to a per-try filename. This pins
        # the current behavior so that changing it is a deliberate choice.
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("aaaa\nbbbb\n")
            poll()
            with open(self.path, "w", encoding="utf-8") as fh:  # truncate & rewrite
                fh.write("cccc\n")
            poll()
            self._append("dddd\neeee\nffff\n")
            poll()
        self.assertEqual(seen, ["aaaa\n", "bbbb\n", "eeee\n", "ffff\n"])

    def test_poll_after_context_exit_is_inert(self):
        # the caller still holds the yielded closure after the context exits. once we've closed the
        # file we're done with it -- polling again must not reopen it and replay from the top.
        logger = self._make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("one\n")
        self.assertEqual(seen, ["one\n"])
        poll()
        self._append("two\n")
        poll()
        self.assertEqual(seen, ["one\n"])

    def test_tail_close_is_terminal_and_idempotent(self):
        fh = open(self.path, "rb")
        state = _TailStream(fh, codecs.getincrementaldecoder("utf-8")(errors="replace"))
        self.assertEqual(state.leftover, "")
        self.assertIs(_tail_close(state), _TAIL_STOPPED)
        self.assertTrue(fh.closed)
        self.assertIs(_tail_close(_TAIL_STOPPED), _TAIL_STOPPED)  # terminal state is a fixed point
        self.assertIs(_tail_close(None), _TAIL_STOPPED)  # never opened: nothing to close

    def test_pygtail_logger_alias(self):
        # deprecated alias for the pre-v1.15 name, imported by out-of-tree container backends
        self.assertIs(_util.PygtailLogger, TailLogger)


class TestTailEmitLines(unittest.TestCase):
    """Unit tests for the line-splitting core, independent of any file I/O."""

    @staticmethod
    def _split(buf, final):
        lines = []
        leftover = _tail_emit_lines(buf, final, lines.append)
        return lines, leftover

    def test_terminators(self):
        cases = [
            ("", ([], "")),
            ("a\nb\n", (["a\n", "b\n"], "")),
            ("a\nb", (["a\n"], "b")),  # trailing partial retained
            ("a\r\nb\r\n", (["a\n", "b\n"], "")),
            ("a\rb\rc\n", (["a\n", "b\n", "c\n"], "")),
            ("\n\n", (["\n", "\n"], "")),  # empty lines preserved
            ("a\r\n", (["a\n"], "")),  # complete \r\n at the end is not ambiguous
        ]
        for buf, expected in cases:
            with self.subTest(buf=buf):
                self.assertEqual(self._split(buf, False), expected)

    def test_max_line_boundary(self):
        # the guard counts the line including its normalized trailing \n, so content of
        # _TAIL_MAX_LINE - 1 is the longest that passes
        ok = "z" * (_TAIL_MAX_LINE - 1)
        self.assertEqual(self._split(ok + "\n", False), ([ok + "\n"], ""))
        with self.assertRaises(RuntimeError):
            self._split("z" * _TAIL_MAX_LINE + "\n", False)

    def test_trailing_cr_held_unless_final(self):
        self.assertEqual(self._split("a\rb\r", False), (["a\n"], "b\r"))
        self.assertEqual(self._split("a\rb\r", True), (["a\n", "b\n"], ""))

    def test_chunking_invariant(self):
        # splitting the same text at any point and feeding it in two calls yields the same lines
        text = "one\rtwo\r\nthree\nfour\r\n"
        whole, rest = self._split(text, True)
        self.assertEqual(rest, "")
        for i in range(len(text) + 1):
            with self.subTest(split_at=i):
                first, leftover = self._split(text[:i], False)
                second, leftover = self._split(leftover + text[i:], True)
                self.assertEqual(first + second, whole)
                self.assertEqual(leftover, "")


if __name__ == "__main__":
    unittest.main()
