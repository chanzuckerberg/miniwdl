import logging
import os
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
