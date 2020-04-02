import unittest
import logging
import tempfile
import os
import json
import docker
from .context import WDL

class TestStdLib(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        logger = logging.getLogger(cls.__name__)
        cfg = WDL.runtime.config.Loader(logger, [])
        WDL.runtime.task.SwarmContainer.global_init(cfg, logger)

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_stdlib_")

    def _test_task(self, wdl:str, inputs = None, expected_exception: Exception = None, cfg = None):
        cfg = cfg or WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        try:
            doc = WDL.parse_document(wdl)
            assert len(doc.tasks) == 1
            doc.typecheck()
            assert len(doc.tasks[0].required_inputs.subtract(doc.tasks[0].available_inputs)) == 0
            if isinstance(inputs, dict):
                inputs = WDL.values_from_json(inputs, doc.tasks[0].available_inputs, doc.tasks[0].required_inputs)
            rundir, outputs = WDL.runtime.run(cfg, doc.tasks[0], (inputs or WDL.Env.Bindings()), run_dir=self._dir, max_tasks=1)
        except WDL.runtime.RunFailed as exn:
            if expected_exception:
                self.assertIsInstance(exn.__context__, expected_exception)
                return exn.__context__
            raise exn.__context__
        except Exception as exn:
            if expected_exception:
                self.assertIsInstance(exn, expected_exception)
                return exn.__context__
            raise
        if expected_exception:
            self.assertFalse(str(expected_exception) + " not raised")
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
        """, expected_exception=WDL.Error.NullValue)
        outputs = self._test_task(R"""
        version 1.0
        task test_select {
            input {
            }
            command {}
            output {
                Int bogus = select_first([])
            }
        }
        """, expected_exception=WDL.Error.IndeterminateType)
        outputs = self._test_task(R"""
        version 1.0
        task test_select {
            input {
            }
            command {}
            output {
                Array[Int] bogus = select_all([])
            }
        }
        """, expected_exception=WDL.Error.IndeterminateType)

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
                String chocoearlylate = sub(chocolike, "[^ ]late", "early") # I like chocearly when it's late
                String choco4 = sub(chocolike, " [[:alpha:]]{4} ", " 4444 ") # I 4444 chocolate 4444 it's late
            }
        }
        """)
        self.assertEqual(outputs, {
            "chocolike": "I like chocolate when it's late",
            "chocolove": "I love chocolate when it's late",
            "chocoearly": "I like chocoearly when it's early",
            "chocolate": "I like chocolate when it's early",
            "chocoearlylate": "I like chocearly when it's late",
            "choco4": "I 4444 chocolate 4444 it's late"
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

    def test_flatten(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            Array[Array[Int]] ai2D = [[1, 2, 3], [1], [21, 22]]
            Array[Array[String]] af2D = [["/tmp/X.txt"], ["/tmp/Y.txt", "/tmp/Z.txt"], []]
            Array[Array[Pair[Float,String]]] aap2D = [[(0.1, "mouse")], [(3, "cat"), (15, "dog")]]
            command {}
            output {
                Array[Int] ai = flatten(ai2D)
                Array[String] af = flatten(af2D)
                Array[Pair[Float,String]] ap = flatten(aap2D)
            }
        }
        """)
        self.assertEqual(outputs["ai"], [1, 2, 3, 1, 21, 22])
        self.assertEqual(outputs["af"], ["/tmp/X.txt", "/tmp/Y.txt", "/tmp/Z.txt"])
        self.assertEqual(outputs["ap"], [[0.1, "mouse"], [3, "cat"], [15, "dog"]])

    def test_size(self):
        with open(os.path.join(self._dir, "alyssa.txt"), "w") as outfile:
            outfile.write("Alyssa\n")
        with open(os.path.join(self._dir, "ben.txt"), "w") as outfile:
            outfile.write("Ben\n")
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            Array[File] files
            File? nullfile
            Array[Float] sizes_ = [
                size(files[0]),
                size(files),
                size(files[0], "MB"),
                size(files[0], "MiB")
            ]
            command {
                cat ~{sep=' ' files} > alyssa_ben.txt
            }
            output {
                Array[Float] sizes = flatten([sizes_, [size(files, "GB"), size(files, "Gi")]])
                Float size2 = size("alyssa_ben.txt", "KiB")
                Float nosize1 = size(nullfile)
                Float nosize2 = size([files[0], nullfile])
            }
        }
        """, {"files": [ os.path.join(self._dir, "alyssa.txt"),
                         os.path.join(self._dir, "ben.txt") ]})
        self.assertEqual(len(outputs["sizes"]), 6)
        self.assertEqual(outputs["sizes"][0], 7)
        self.assertEqual(outputs["sizes"][1], 11)
        self.assertAlmostEqual(outputs["sizes"][2], 7/1000000)
        self.assertAlmostEqual(outputs["sizes"][3], 7/1048576)
        self.assertAlmostEqual(outputs["sizes"][4], 11/1000000000)
        self.assertAlmostEqual(outputs["sizes"][5], 11/1073741824)
        self.assertAlmostEqual(outputs["size2"], 11/1024)
        self.assertEqual(outputs["nosize1"], 0)
        self.assertEqual(outputs["nosize2"], 7)

        self._test_task(R"""
        version 1.0
        task hello {
            Float x = size("/etc/passwd")
            command {}
        }
        """, expected_exception=WDL.Error.InputError)

        self._test_task(R"""
        version 1.0
        task hello {
            command {}
            output {
                Float x = size("/etc/passwd")
            }
        }
        """, expected_exception=WDL.runtime.task.OutputError)

        self._test_task(R"""
        version 1.0
        task hello {
            command {
                touch foo
            }
            output {
                Float x = size("foo", "bogus")
            }
        }
        """, expected_exception=WDL.Error.EvalError)

    def test_glob(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command {
                mkdir stuff
                cd stuff
                touch foo bar bas baz bat
            }
            output {
                Array[Array[File]] globs = [
                    glob("stuff/foo"),
                    glob("./stuff/b*"),
                    glob("*/*"),
                    glob("bogus")
                ]
                File f1 = glob("stuff/foo")[0]
                String s1 = read_string(f1)
            }
        }
        """)
        self.assertEqual(len(outputs["globs"][0]), 1)
        self.assertTrue(outputs["globs"][0][0].endswith("/foo"))
        self.assertEqual(len(outputs["globs"][1]), 4)
        self.assertTrue(outputs["globs"][1][0].endswith("/bar"))
        self.assertTrue(outputs["globs"][1][1].endswith("/bas"))
        self.assertTrue(outputs["globs"][1][2].endswith("/bat"))
        self.assertTrue(outputs["globs"][1][3].endswith("/baz"))
        self.assertEqual(len(outputs["globs"][2]), 5)
        self.assertTrue(outputs["globs"][2][4].endswith("/foo"))
        self.assertEqual(len(outputs["globs"][3]), 0)
        for g in outputs["globs"] + [[outputs["f1"]]]:
            for fn in g:
                assert os.path.isfile(fn), fn
        self.assertTrue(outputs["f1"].endswith("/foo"))

        self._test_task(R"""
        version 1.0
        task hello {
            command {}
            output {
                Array[File] filez = glob("/etc/passwd")
            }
        }
        """, expected_exception=WDL.Error.EvalError)

        self._test_task(R"""
        version 1.0
        task hello {
            command {}
            output {
                Array[File] filez = glob("../../etc/passwd")
            }
        }
        """, expected_exception=WDL.Error.EvalError)

    def test_concat(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            String foo = "foo"
            String? bar = "bar"
            String? none
            Int i = 42
            Int? i_none
            command {
                echo ~{foo + bar}
                echo ~{foo + bar + i}
                echo ~{foo + none}
                echo ~{none + bar}
                echo ~{foo + none + bar}
                echo ~{foo + bar + none}
                echo ~{foo + bar + i_none}
                echo ~{sep='::' [foo,bar,none]}
                echo ~{sep='::' [foo,none,bar]}
            }
            output {
                String s = read_string(stdout())
            }
        }
        """)
        self.assertEqual(outputs["s"], "foobar\nfoobar42\n\n\n\n\n\nfoo::bar::\nfoo::::bar")

    def test_read(self):
        with open(os.path.join(self._dir, "strings.txt"), "w") as outfile:
            outfile.write("foo\nbar\nbas\n")
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            input {
                File strings
            }
            String i1 = read_string(strings)
            Array[String] i2 = read_lines(strings)
            File strings2 = write_lines(i2)

            command {
                echo Alyssa
                echo Ben
                echo 42 > fortytwo.txt
                echo 6.02e23 > mole.txt
                echo true > true.txt
                echo false > false.txt
                echo -e "key1\tvalue1" > map.txt
                echo -e "key2\tvalue2" >> map.txt
                echo -e "..\ttricky" >> map.txt
            }
            output {
                String i_strings_string = i1
                String o_strings_string = read_string(strings)
                String o_names_string = read_string(stdout())
                Array[String] i_strings_lines = i2
                Array[String] o_strings_lines = read_lines(strings2)
                Array[String] o_names_lines = read_lines(stdout())
                Int o_fortytwo = read_int("fortytwo.txt")
                Float o_mole = read_float("mole.txt")
                Array[Boolean] o_boolean = [read_boolean("true.txt"), read_boolean("false.txt")]
                Map[String,String] o_map = read_map("map.txt")
            }
        }
        """, {"strings": os.path.join(self._dir, "strings.txt")})
        self.assertEqual(outputs["i_strings_string"], "foo\nbar\nbas")
        self.assertEqual(outputs["o_strings_string"], "foo\nbar\nbas")
        self.assertEqual(outputs["o_names_string"], "Alyssa\nBen")
        self.assertEqual(outputs["i_strings_lines"], ["foo", "bar", "bas"])
        self.assertEqual(outputs["o_strings_lines"], ["foo", "bar", "bas"])
        self.assertEqual(outputs["o_names_lines"], ["Alyssa", "Ben"])
        self.assertEqual(outputs["o_fortytwo"], 42)
        self.assertEqual(outputs["o_boolean"], [True, False])
        self.assertEqual(outputs["o_map"], {"key1": "value1", "key2": "value2", "..": "tricky"})

    def test_read_json(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command <<<
                echo '{"foo": "bar", "bas": "baz"}' > object.json
                echo '[1, 2, 3, 4, 5]' > list.json
                echo 42 > int.json
                echo 3.14159 > float.json
                echo true > bool.json
                echo null > null.json
                echo '{"out": ["Element 1", "Element 2"]}' > out.txt
            >>>
            output {
                Map[String,String] map = read_json("object.json")
                Array[Int] array = read_json("list.json")
                Int int = read_json("int.json")
                Float float = read_json("float.json")
                Boolean bool = read_json("bool.json")
                String? null = read_json("null.json")

                # issue #320
                String baz1 = read_json("object.json")["bas"]
                Int three = read_json("list.json")[2]
                Array[String] out1 = read_json('out.txt')["out"]
                String out2 = read_json('out.txt')["out"][1]
            }
        }
        """)
        self.assertEqual(outputs["map"], {"foo": "bar", "bas": "baz"})
        self.assertEqual(outputs["array"], [1, 2, 3, 4, 5])
        self.assertEqual(outputs["int"], 42)
        self.assertAlmostEqual(outputs["float"], 3.14159)
        self.assertEqual(outputs["bool"], True)
        self.assertEqual(outputs["null"], None)
        self.assertEqual(outputs["baz1"], "baz")
        self.assertEqual(outputs["three"], 3)
        self.assertEqual(outputs["out1"], ["Element 1", "Element 2"])
        self.assertEqual(outputs["out2"], "Element 2")

        outputs = self._test_task(R"""
        version 1.0
        task test {
            command <<<
                echo '["foo", "bar"]'
            >>>
            output {
                Array[String] my_array = read_json(stdout())
            }
        }
        """)
        self.assertEqual(outputs["my_array"], ["foo", "bar"])

        self._test_task(R"""
        version 1.0
        task test {
            command <<<
                echo '{"foo":"bar"}'
            >>>
            output {
                Array[String] my_array = read_json(stdout())
            }
        }
        """, expected_exception=WDL.Error.InputError)

        outputs = self._test_task(R"""
        version 1.0
        task test {
            command <<<
                echo '{"foo":"bar"}'
            >>>
            output {
                Map[String, String] my_map = read_json(stdout())
            }
        }
        """)
        self.assertEqual(outputs["my_map"], {"foo": "bar"})

        self._test_task(R"""
        version 1.0
        task test {
            command <<<
                echo '["foo", "bar"]'
            >>>
            output {
                Map[String, String] my_map = read_json(stdout())
            }
        }
        """, expected_exception=WDL.Error.InputError)

    def test_read_map_ints(self):
        outputs = self._test_task(R"""
        version 1.0
        task test {
            command <<<
                python <<CODE
                for i in range(3):
                    print("key_{idx}\t{idx}".format(idx=i))
                CODE
            >>>
            output {
                Map[String, Int] my_ints = read_map(stdout())
            }
            runtime {
                docker: "continuumio/miniconda3"
            }
        }
        """)
        self.assertEqual(outputs["my_ints"], {"key_0": 0, "key_1": 1, "key_2": 2})

    def test_write(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            File foo = write_lines(["foo","bar","baz"])
            File tsv = write_tsv([["one", "two", "three"], ["un", "deux", "trois"]])
            File json = write_json({"key1": "value1", "key2": "value2"})
            File map = write_map({"key1": "value1", "key2": "value2"})

            command <<<
                foo_sha=$(sha256sum < ~{foo} | cut -f1 -d ' ')
                if [ "$foo_sha" != "b1b113c6ed8ab3a14779f7c54179eac2b87d39fcebbf65a50556b8d68caaa2fb" ]; then
                    exit 1
                fi
                tsv_sha=$(sha256sum < ~{tsv} | cut -f1 -d ' ')
                if [ "$tsv_sha" != "a7124e688203195cd674cf147bbf965eda49e8df581d01c05944330fab096084" ]; then
                    exit 1
                fi
            >>>

            output {
                File o_json = json
                Array[Array[String]] o_tsv = read_tsv(tsv)
                Map[String,String] o_map = read_map(map)
                File whynot = write_lines(["foo","bar","baz"])
            }
        }
        """)
        with open(outputs["o_json"]) as infile:
            self.assertEqual(json.load(infile), {"key1": "value1", "key2": "value2"})
        self.assertEqual(outputs["o_tsv"], [["one", "two", "three"], ["un", "deux", "trois"]])
        self.assertEqual(outputs["o_map"], {"key1": "value1", "key2": "value2"})

    def test_bad_map(self):
        self._test_task(R"""
        version 1.0
        task bad_map {
            File map = write_map({"foo": "bar\t"})
            command {}
        }
        """, expected_exception=WDL.Error.EvalError)

        self._test_task(R"""
        version 1.0
        task bad_map {
            command <<<
                echo -e "a\tb" > map.txt
                echo -e "c\td\te" >> map.txt
            >>>
            output {
                Map[String,String] map = read_map("map.txt")
            }
        }
        """, expected_exception=WDL.Error.EvalError)

        self._test_task(R"""
        version 1.0
        task bad_map {
            command <<<
                echo -e "a\tb" > map.txt
                echo -e "a\tc" >> map.txt
            >>>
            output {
                Map[String,String] map = read_map("map.txt")
            }
        }
        """, expected_exception=WDL.Error.EvalError)

    def test_transpose(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command {}
            output {
                Array[Array[Int]] mat = transpose([[0, 1, 2], [3, 4, 5]])
            }
        }
        """)
        self.assertEqual(outputs["mat"], [[0, 3], [1, 4], [2, 5]])

        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command {}
            output {
                Array[Array[Int]] mat = transpose([[0, 1, 2], [3, 4, 5], []])
            }
        }
        """, expected_exception=WDL.Error.EvalError)

    def test_zip_cross(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            Array[Int] xs = [ 1, 2, 3 ]
            Array[String] ys = [ "a", "b", "c" ]
            Array[String] zs = [ "d", "e" ]
            command {}
            output {
                Array[Pair[Int, String]] zipped = zip(xs, ys)
                Array[Pair[Int, String]] crossed = cross(xs, zs)
            }
        }
        """)
        self.assertEqual(outputs["zipped"], [[1, "a"], [2, "b"], [3, "c"]])
        self.assertEqual(outputs["crossed"], [[1, "d"], [1, "e"], [2, "d"], [2, "e"], [3, "d"], [3, "e"]])

        outputs = self._test_task(R"""
        version 1.0
        task hello {
            Array[Int] xs = [ 1, 2, 3 ]
            Array[String] ys = [ "a", "b", "c" ]
            Array[String] zs = [ "d", "e" ]
            command {}
            output {
                Array[Pair[Int, String]] zipped = zip(xs, zs)
            }
        }
        """, expected_exception=WDL.Error.EvalError)
