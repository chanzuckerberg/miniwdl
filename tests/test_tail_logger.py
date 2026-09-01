import logging
import os
import tempfile
import unittest
from unittest.mock import patch

import WDL._util as _util
from WDL._util import TailLogger, VERBOSE_LEVEL


def _make_logger():
    logger = logging.getLogger(f"TailLoggerTest.{id(object())}")
    logger.setLevel(VERBOSE_LEVEL)
    logger.propagate = False
    for h in list(logger.handlers):
        logger.removeHandler(h)
    return logger


class TestTailLogger(unittest.TestCase):
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
        logger = _make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("alpha\nbeta\n")
            poll()
            self.assertEqual(seen, ["alpha\n", "beta\n"])

    def test_buffers_partial_line(self):
        logger = _make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("hello ")
            poll()
            self.assertEqual(seen, [])
            self._append("world\n")
            poll()
            self.assertEqual(seen, ["hello world\n"])

    def test_flushes_on_context_exit(self):
        logger = _make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("line1\nline2\n")
        self.assertEqual(seen, ["line1\n", "line2\n"])

    def test_trailing_partial_not_emitted_on_exit(self):
        logger = _make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append):
            self._append("done\ntail-only")
        self.assertEqual(seen, ["done\n"])

    def test_file_missing_at_start(self):
        os.unlink(self.path)
        logger = _make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            poll()
            self.assertEqual(seen, [])
            with open(self.path, "w", encoding="utf-8") as fh:
                fh.write("hi\n")
            poll()
            self.assertEqual(seen, ["hi\n"])

    def test_incremental_reads_across_polls(self):
        logger = _make_logger()
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
        logger = _make_logger()
        logger.setLevel(logging.CRITICAL)  # above VERBOSE_LEVEL
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append("silent\n")
            poll()
        self.assertEqual(seen, [])

    def test_oversized_line_stops_stream_with_warning(self):
        logger = _make_logger()
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
        logger = _make_logger()
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
        logger = _make_logger()
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
        logger = _make_logger()
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
        logger = _make_logger()
        seen = []
        with patch.object(_util, "_TAIL_CHUNK_BYTES", 3):
            with TailLogger(logger, self.path, callback=seen.append) as poll:
                self._append(text)
                poll()
        self.assertEqual(seen, [text])

    def test_invalid_utf8_byte_replaced(self):
        # 0xFF is never valid in UTF-8 (lead or continuation byte); decoding must not raise, and
        # must deterministically substitute U+FFFD rather than corrupting/dropping the whole line.
        logger = _make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append_bytes(b"bad-\xff-bytes\n")
            poll()
        self.assertEqual(seen, ["bad-�-bytes\n"])

    def test_truly_incomplete_trailing_sequence_dropped_at_exit(self):
        # Writer dies after emitting only the first 2 of 3 bytes of "€" (EURO SIGN), with no
        # terminating newline ever. This must not raise, and the incomplete/unterminated data is
        # simply dropped -- same defined behavior as any other unterminated trailing partial line.
        logger = _make_logger()
        seen = []
        with TailLogger(logger, self.path, callback=seen.append) as poll:
            self._append_bytes("€".encode("utf-8")[:2])
            poll()
        self.assertEqual(seen, [])


if __name__ == "__main__":
    unittest.main()
