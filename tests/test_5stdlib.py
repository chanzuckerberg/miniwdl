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

    def test_length_defined_range(self):
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
                Array[Array[Int]] ranges = [range(0), range(1), range(3)]
            }
        }
        """, {"one": 42, "two": 43})
        self.assertEqual(outputs, {
            "lengths": [0, 1, 2],
            "defineds": [True, True, False],
            "ranges": [[], [0], [0,1,2]]
        })
        self._test_task(R"""
        version 1.0
        task bogus {
            command {}
            output {
                Array[Int] bogus = range(-42)
            }
        }
        """, expected_exception=WDL.Error.EvalError)

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

    def test_basename_prefix(self):
        outputs = self._test_task(R"""
        version 1.0
        task test_basename_prefix {
            Array[String] env = ["key1=value1", "key2=value2", "key3=value3"]
            Array[Int] env2 = [1, 2, 3]
            command {}
            output {
                Array[String] ans = [
                    basename("file.txt"),
                    basename("to/file.txt"),
                    basename("/path/to/file.txt"),
                    basename("/path/to/file.txt", ".txt"),
                    basename("/path/to/file.sam", ".txt")
                ]
                Array[String] env_param = prefix("-e ", env)
                Array[String] env2_param = prefix("-f ", env2)
            }
        }
        """)
        self.assertEqual(outputs, {
            "ans": ["file.txt", "file.txt", "file.txt", "file", "file.sam"],
            "env_param": ["-e key1=value1", "-e key2=value2", "-e key3=value3"],
            "env2_param": ["-f 1", "-f 2", "-f 3"]
        })

    def test_select(self):
        outputs = self._test_task(R"""
        version 1.0
        task test_select {
            input {
                Array[Int] one
                Array[Int]? two
                Array[Int]? three
            }
            command {}
            output {
                Array[Int] first1 = select_first([one, two, three])
                Array[Int] first2 = select_first([two, three])
                Array[Int] first3 = select_first([three, two])
                Array[Int] first4 = select_first([two])

                Array[Array[Int]] all1 = select_all([one, two, three])
                Array[Array[Int]] all2 = select_all([three])
            }
        }
        """, {"one": [1], "two": [2]})
        self.assertEqual(outputs, {
            "first1": [1],
            "first2": [2],
            "first3": [2],
            "first4": [2],
            "all1": [[1],[2]],
            "all2": []
        })
        outputs = self._test_task(R"""
        version 1.0
        task test_select {
            input {
                Array[Int]? one
            }
            command {}
            output {
                Array[Int] bogus = select_first([one])
            }
        }
        """, expected_exception=WDL.Error.EvalError)

    def test_sub(self):
        outputs = self._test_task(R"""
        version 1.0
        task test_sub {
            command {}
            output {
                String chocolike = "I like chocolate when it's late"
                String chocolove = sub(chocolike, "like", "love") # I love chocolate when it's late
                String chocoearly = sub(chocolike, "late", "early") # I like chocoearly when it's early
                String chocolate = sub(chocolike, "late$", "early") # I like chocolate when it's early
            }
        }
        """)
        self.assertEqual(outputs, {
            "chocolike": "I like chocolate when it's late",
            "chocolove": "I love chocolate when it's late",
            "chocoearly": "I like chocoearly when it's early",
            "chocolate": "I like chocolate when it's early"
        })
        outputs = self._test_task(R"""
        task example {
            input {
                String input_file = "my_input_file.bam"
                String output_file_name = sub(input_file, "\\.bam$", ".index") # my_input_file.index
            }
            command {
                echo "I want an index instead" > ${output_file_name}
            }
            output {
                File outputFile = output_file_name
            }
        }
        """)
        self.assertTrue(outputs["outputFile"].endswith("my_input_file.index"))
        outputs = self._test_task(R"""
        task bogus {
            command {}
            output {
                String bogus = sub("foo", "(()", "bar")
            }
        }
        """, expected_exception=WDL.Error.EvalError)
