import unittest
import logging
import tempfile
from .context import WDL

class TestStdLib(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_stdlib_")

    def _test_task(self, wdl:str, inputs = None, expected_exception: Exception = None):
        doc = WDL.parse_document(wdl)
        assert len(doc.tasks) == 1
        doc.typecheck()
        if isinstance(inputs, dict):
            inputs = WDL.values_from_json(inputs, doc.tasks[0].available_inputs, doc.tasks[0].required_inputs)
        if expected_exception:
            try:
                WDL.runtime.run_local_task(doc.tasks[0], (inputs or []), parent_dir=self._dir)
            except WDL.runtime.task.TaskFailure as exn:
                self.assertIsInstance(exn.__context__, expected_exception)
                return exn.__context__
            self.assertFalse(str(expected_exception) + " not raised")
        rundir, outputs = WDL.runtime.run_local_task(doc.tasks[0], (inputs or []), parent_dir=self._dir)
        return WDL.values_to_json(outputs)

    def test_size_polytype(self):
        tmpl = """
        version 1.0
        task test_size {{
            input {{
                File file1
                File file2
            }}
            {}
            command <<<
                echo "nop"
            >>>
        }}
        """

        for case in [
            "Float sz = size(file1)",
            "Float sz = size(file1, 'GB')",
            "Float sz = size([file1,file2], 'KB')",
            "Float sz = size([file1,file2], 'KB')",
        ]:
            doc = WDL.parse_document(tmpl.format(case))
            doc.typecheck()

        for case in [
            ("Float sz = size()", WDL.Error.WrongArity),
            ("Float sz = size(file1,file2,'MB')", WDL.Error.WrongArity),
            ("Float sz = size(42)", WDL.Error.StaticTypeMismatch),
            ("Float sz = size([42])", WDL.Error.StaticTypeMismatch),
            ("Float sz = size(file1,file2)", WDL.Error.StaticTypeMismatch),
            ("Float sz = size(file1,[file2])", WDL.Error.StaticTypeMismatch),
        ]:
            doc = WDL.parse_document(tmpl.format(case[0]))
            with self.assertRaises(case[1]):
                doc.typecheck()

    def test_length_and_defined(self):
        outputs = self._test_task(R"""
        version 1.0
        task test_length {
            input {
                Int one
                Int? two
                Int? three
            }
            command {}
            output {
                Array[Int] lengths = [length([]), length([42]), length([42,43])]
                Array[Boolean] defineds = [defined(one), defined(two), defined(three)]
            }
        }
        """, {"one": 42, "two": 43})
        self.assertEqual(outputs, {"lengths": [0, 1, 2], "defineds": [True, True, False]})

    def test_floor_ceil_round(self):
        outputs = self._test_task(R"""
        version 1.0
        task test_floor_ceil_round {
            command {}
            output {
                Array[Int] ans = [
                    floor(3.14),
                    ceil(-3.14),
                    round(42.5),
                    round(42.5000000001)
                ]
            }
        }
        """)
        self.assertEqual(outputs, {"ans": [3, -3, 42, 43]})

    def test_basename(self):
        outputs = self._test_task(R"""
        version 1.0
        task test_basename {
            command {}
            output {
                Array[String] ans = [
                    basename("file.txt"),
                    basename("to/file.txt"),
                    basename("/path/to/file.txt"),
                    basename("/path/to/file.txt", ".txt"),
                    basename("/path/to/file.sam", ".txt")
                ]
            }
        }
        """)
        self.assertEqual(outputs, {"ans": ["file.txt", "file.txt", "file.txt", "file", "file.sam"]})
