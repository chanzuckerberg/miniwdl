import unittest
import logging
import tempfile
import os
import docker
import signal
import time
import sys
from .context import WDL

class TestWorkflowRunner(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_workflowrun_")

    def _test_workflow(self, wdl:str, inputs = None, expected_exception: Exception = None):
        sys.setrecursionlimit(180)  # set artificially low in unit tests to detect excessive recursion (issue #239)
        try:
            with tempfile.NamedTemporaryFile(dir=self._dir, suffix=".wdl", delete=False) as outfile:
                outfile.write(wdl.encode("utf-8"))
                wdlfn = outfile.name
            doc = WDL.load(wdlfn)
            if isinstance(inputs, dict):
                inputs = WDL.values_from_json(inputs, doc.workflow.available_inputs, doc.workflow.required_inputs)
            rundir, outputs = WDL.runtime.run_local_workflow(doc.workflow, (inputs or WDL.Env.Bindings()), run_dir=self._dir, _test_pickle=True)
        except WDL.runtime.TaskFailure as exn:
            if expected_exception:
                self.assertIsInstance(exn.__context__, expected_exception)
                return exn.__context__
            raise exn.__context__
        except WDL.Error.MultipleValidationErrors as multi:
            for exn in multi.exceptions:
                logging.error("%s: %s", str(exn.pos), str(exn))
            raise
        except Exception as exn:
            if expected_exception:
                self.assertIsInstance(exn, expected_exception)
                return exn
            raise
        if expected_exception:
            self.assertFalse(str(expected_exception) + " not raised")
        return WDL.values_to_json(outputs)

    def test_hello(self):
        self.assertEqual(self._test_workflow("""
        version 1.0

        workflow nop {
        }
        """), {})

        outputs = self._test_workflow("""
        version 1.0

        workflow hellowf {
            input {
                Int x
            }
            call hello as hello1 {
                input:
                    who = ["Alice", "Bob"],
                    x = x
            }
            call hello as hello2 {
                input:
                    who = ["Alyssa", "Ben"],
                    x = x
            }
            output {
                Array[String]+ messages = flatten([hello1.messages, hello2.messages])
                Array[Int]+ meanings = [hello1.meaning_of_life, hello2.meaning_of_life]
            }
        }

        task hello {
            input {
                Array[String]+ who
                Int x = 0
            }
            command <<<
                awk '{print "Hello", $0}' "~{write_lines(who)}"
            >>>
            output {
                Array[String]+ messages = read_lines(stdout())
                Int meaning_of_life = x+1
            }
        }
        """, {"x": 41})
        self.assertEqual(outputs["messages"], ["Hello Alice", "Hello Bob", "Hello Alyssa", "Hello Ben"])
        self.assertEqual(outputs["meanings"], [42, 42])

    def test_scatters(self):
        outputs = self._test_workflow("""
        version 1.0

        workflow hellowf {
            input {
                Int n
            }
            scatter (i in range(n)) {
                Int sq = i*i
            }
            output {
                Array[Int] sqs = sq
            }
        }
        """, {"n": 10})
        self.assertEqual(outputs["sqs"], [0, 1, 4, 9, 16, 25, 36, 49, 64, 81])

        outputs = self._test_workflow("""
        version 1.0

        workflow hellowf {
            input {
                Int n
            }
            scatter (i in range(n)) {
                call compute_sq {
                    input:
                        k = i
                }
            }
            output {
                Array[Int] sqs = compute_sq.k_sq
            }
        }

        task compute_sq {
            input {
                Int k
            }
            command {}
            output {
                Int k_sq = k*k
            }
        }
        """, {"n": 10})
        self.assertEqual(outputs["sqs"], [0, 1, 4, 9, 16, 25, 36, 49, 64, 81])

        outputs = self._test_workflow("""
        version 1.0

        workflow crossrange {
            input {
                Int m
                Int n
            }
            scatter (i in range(m)) {
                scatter (j in range(n)) {
                    Pair[Int,Int] p = (i,j)
                }
            }
            output {
                Array[Pair[Int,Int]] pairs = flatten(p)
            }
        }
        """, {"m": 4, "n": 2})
        self.assertEqual(outputs["pairs"], [[0, 0], [0, 1], [1, 0], [1, 1], [2, 0], [2, 1], [3, 0], [3, 1]])

        outputs = self._test_workflow("""
        version 1.0

        workflow crossrange {
            input {
                Int m
                Int n
            }
            scatter (i in range(m)) {
                Int k = i
                scatter (j in range(n)) {
                    Int l = k
                    call cons_pair as cons {
                        input:
                            lhs = l,
                            rhs = j
                    }
                }
            }
            output {
                Array[Pair[Int,Int]] pairs = flatten(cons.pair)
            }
        }

        task cons_pair {
            input {
                Int lhs
                Int rhs
            }
            command {}
            output {
                Pair[Int,Int] pair = (lhs,rhs)
            }
        }
        """, {"m": 4, "n": 2})
        self.assertEqual(outputs["pairs"], [[0, 0], [0, 1], [1, 0], [1, 1], [2, 0], [2, 1], [3, 0], [3, 1]])

    def test_ifs(self):
        outputs = self._test_workflow("""
        version 1.0

        workflow ifwf {
            if (true) {
                Int a = 1
            }
            if (false) {
                Int b = 2
            }
            output {
                Array[Int] s = select_all([a, b])
            }
        }
        """)
        self.assertEqual(outputs["s"], [1])

        outputs = self._test_workflow("""
        version 1.0

        workflow ifwf {
            if (3 == 3) {
                call sum {
                    input:
                        lhs = 1,
                        rhs = select_first([sum2.ans, 1])
                }
            }
            if (3 < 3) {
                call sum as sum2 {
                    input:
                        lhs = 1,
                        rhs = 1
                }
            }
            output {
                Int ans = select_first([sum.ans])
            }
        }

        task sum {
            input {
                Int lhs
                Int rhs
            }
            command {}
            output {
                Int ans = lhs + rhs
            }
        }
        """)
        self.assertEqual(outputs["ans"], 2)

        outputs = self._test_workflow("""
        version 1.0

        workflow ifwf {
            if (true) {
                if (true) {
                    Int x = 1+1
                }
            }
            if (true) {
                if (false) {
                    Int y = 42
                }
                Int z = select_first([x])+2
            }
            if (false) {
                if (true) {
                    Int w = 4
                }
            }
            output {
                Int? x_out = x
                Int? y_out = y
                Int? z_out = z
                Int? w_out = w
            }
        }
        """)
        self.assertEqual(outputs, {"x_out": 2, "y_out": None, "z_out": 4, "w_out": None})

        outputs = self._test_workflow("""
        version 1.0

        workflow ifwf {
            scatter (i in range(3)) {
                call sum {
                    input:
                        lhs = i,
                        rhs = i
                }
                if (i != 1) {
                    scatter (j in range(2)) {
                        Int x = sum.ans + j
                    }
                }
            }
            output {
                Array[Array[Int]?] out = x
            }
        }

        task sum {
            input {
                Int lhs
                Int rhs
            }
            command {}
            output {
                Int ans = lhs + rhs
            }
        }
        """)
        self.assertEqual(outputs, {"out": [[0, 1], None, [4, 5]]})

    def test_io(self):
        txt = """
        version 1.0

        workflow inputs {
            input {
                Int x
                Int z = y+1
            }
            Int y = x+1
            output {
                Array[Int] out = [x, y ,z]
            }
        }
        """
        self.assertEqual(self._test_workflow(txt, {"x": 1}), {"out": [1, 2, 3]})
        self.assertEqual(self._test_workflow(txt, {"x": 1, "z": 42}), {"out": [1, 2, 42]})

        outputs = self._test_workflow("""
        version 1.0

        workflow inputs {
            input {
                Int x
            }
            call sum as y {
                input:
                    lhs = x,
                    rhs = 1
            }
            scatter (i in range(x)) {
                Int z = i+1
                call sum {
                    input:
                        lhs = z,
                        rhs = y.ans
                }
            }
        }

        task sum {
            input {
                Int lhs
                Int rhs
            }
            command {}
            output {
                Int ans = lhs + rhs
            }
        }
        """, {"x": 3})
        self.assertEqual(outputs, { "y.ans": 4, "sum.ans": [ 5, 6, 7 ] })

        # setting optional input of call inside scatter
        txt = """
        version 1.0

        workflow inputs {
            input {
                Int x
            }
            scatter (i in range(x)) {
                call sum {
                    input:
                        lhs = i,
                        rhs = i
                }
            }
            output {
                Array[Int] ans = sum.ans
            }
        }

        task sum {
            input {
                Int lhs
                Int rhs
                Int more = 0
            }
            command {}
            output {
                Int ans = lhs + rhs + more
            }
        }
        """
        self.assertEqual(self._test_workflow(txt, {"x":3}), { "ans": [ 0, 2, 4] })
        self.assertEqual(self._test_workflow(txt, {"x":3, "sum.more": 1}), { "ans": [ 1, 3, 5] })

        txt = """
        version 1.0

        workflow x {
            input {
                Int? optional
            }
            output {
                Int ans = select_first([optional, 42])
            }
        }

        task sum {
            input {
                Int lhs
                Int rhs
            }
            command {}
            output {
                Int ans = lhs + rhs
            }
        }
        """
        self.assertEqual(self._test_workflow(txt)["ans"], 42)
        self.assertEqual(self._test_workflow(txt, {"optional": 123})["ans"], 123)

    def test_errors(self):
        exn = self._test_workflow("""
        version 1.0

        workflow bogus {
            Int y = range(4)[99]
        }
        """, expected_exception=WDL.Error.EvalError)
        self.assertEqual(exn.job_id, "decl-y")

        exn = self._test_workflow("""
        version 1.0

        workflow inputs {
            call sum {
                input:
                    lhs = 1,
                    rhs = 1
            }
        }

        task sum {
            input {
                Int lhs
                Int rhs
            }
            command {}
            output {
                Int ans = lhs + rhs
                Int y = range(4)[99]
            }
        }
        """, expected_exception=WDL.Error.EvalError)
        self.assertEqual(exn.job_id, "decl-y")

    def test_order(self):
        txt = """
        version 1.0

        workflow ooo {
            input {
                Boolean b
            }
            scatter (i in range(select_first([a1, a2]))) {
                Array[Int?] z =  [a1, a2]
            }
            if (b) {
                Int a1 = 1
            }
            if (!b) {
                Int a2 = 2
            }
            output {
                Array[Array[Int?]] z_out = z
            }
        }
        """
        self.assertEqual(self._test_workflow(txt, {"b": True})["z_out"], [[1, None]])
        self.assertEqual(self._test_workflow(txt, {"b": False})["z_out"], [[None, 2], [None, 2]])

    def test_subworkflow(self):
        subwf = """
        version 1.0

        workflow sum_sq {
            input {
                Int n
            }
            scatter (i in range(n)) {
                Int i_sq = (i+1)*(i+1)
            }
            call sum {
                input:
                    x = i_sq
            }
            output {
                Int ans = sum.ans
            }
        }

        task sum {
            input {
                Array[Int] x
            }
            command <<<
                awk 'BEGIN { s = 0 } { s += $0 } END { print s }' ~{write_lines(x)}
            >>>
            output {
                Int ans = read_int(stdout())
            }
        }
        """
        with open(os.path.join(self._dir, "sum_sq.wdl"), "w") as outfile:
            outfile.write(subwf)

        outputs = self._test_workflow("""
        version 1.0
        import "sum_sq.wdl" as lib

        workflow sum_sq_tester {
            input {
                Int n
            }
            scatter (i in range(n)) {
                call lib.sum_sq {
                    input:
                        n = i+1
                }
            }
            call lib.sum as sum_all {
                input:
                    x = sum_sq.ans
            }
            output {
                Array[Int] sums = sum_sq.ans
                Int sum = sum_all.ans
            }
        }
        """, {"n": 3})
        self.assertEqual(outputs["sums"], [1, 5, 14])
        self.assertEqual(outputs["sum"], 20)

    def test_host_file_access(self):
        exn = self._test_workflow("""
        version 1.0
        workflow hacker9000 {
            input {
            }
            File your_passwords = "/etc/passwd"
            call tweet_file { input: file = your_passwords }
        }
        task tweet_file {
            input {
                File file
            }
            command {
                cat ~{file}
            }
        }
        """, expected_exception=WDL.Error.InputError)
        self.assertTrue("inputs use unknown file" in str(exn))

        exn = self._test_workflow("""
        version 1.0
        struct Box {
            Array[String] str
        }
        workflow hacker9000 {
            input {
            }
            call sneaky
            scatter (s in sneaky.box.str) {
                call tweet_file { input: file = s }
            }
        }
        task sneaky {
            command {
                echo "/etc/passwd"
            }
            output {
                Box box = object {
                    str: read_lines(stdout())
                }
            }
        }
        task tweet_file {
            input {
                File file
            }
            command {
                cat ~{file}
            }
        }
        """, expected_exception=WDL.Error.InputError)
        self.assertTrue("inputs use unknown file" in str(exn))

        # positive control
        with open(os.path.join(self._dir, "allowed.txt"), "w") as outfile:
            outfile.write("yo")
        outputs = self._test_workflow("""
        version 1.0
        struct Box {
            Array[File] str
        }
        workflow hacker8999 {
            input {
                Box box
            }
            call hello
            scatter (b in [box, hello.box]) {
                Array[File] str = b.str
            }
            scatter (f in flatten(str)) {
                call tweet_file { input: file = f }
            }
            output {
                Array[String] tweets = tweet_file.tweet
            }
        }
        task hello {
            command {
                echo "Hello, world!"
            }
            output {
                Box box = object {
                    str: [stdout()]
                }
            }
        }
        task tweet_file {
            input {
                File file
            }
            command {
                cat ~{file}
            }
            output {
                String tweet = read_string(stdout())
            }
        }
        """, inputs={"box": { "str": [os.path.join(self._dir, "allowed.txt")] }})
        self.assertEqual(outputs["tweets"], ["yo", "Hello, world!"])

    def test_stdlib_io(self):
        with open(os.path.join(self._dir, "who.txt"), "w") as outfile:
            outfile.write("Alyssa\n")
            outfile.write("Ben\n")

        outputs = self._test_workflow("""
            version 1.0
            workflow hello {
                input {
                    File who
                }
                Array[String] who_lines = read_lines(who)
                scatter (person in who_lines) {
                    String message = "Hello, ${person}!"
                }
                output {
                    Array[String] messages = message
                }
            }
            """, {"who": os.path.join(self._dir, "who.txt")})
        self.assertEqual(outputs["messages"], ["Hello, Alyssa!", "Hello, Ben!"])

        exn = self._test_workflow("""
        version 1.0
        workflow hacker9000 {
            input {
            }
            Array[String] your_passwords = read_lines("/etc/passwd")
        }
        """, expected_exception=WDL.Error.EvalError)
        self.assertTrue("attempted read from" in str(exn))

        outputs = self._test_workflow("""
            version 1.0
            workflow hello {
                input {
                    Array[String] who
                }
                File whofile = write_lines(who)
                scatter (w in read_lines(whofile)) {
                    call say_hello {
                        input:
                            who = write_lines([w])
                    }
                }
                output {
                    Array[String] messages = say_hello.message
                    Array[String] who2 = read_lines(whofile)
                }
            }
            task say_hello {
                File who

                command {
                    echo "Hello, ~{read_string(who)}!"
                }

                output {
                    String message = read_string(stdout())
                }
            }
        """, {"who": ["Alyssa", "Ben"]})
        self.assertEqual(outputs["messages"], ["Hello, Alyssa!", "Hello, Ben!"])
        self.assertEqual(outputs["who2"], ["Alyssa", "Ben"])
