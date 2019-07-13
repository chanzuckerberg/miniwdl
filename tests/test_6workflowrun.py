import unittest
import logging
import tempfile
import os
import docker
import signal
import time
from .context import WDL

class TestWorkflowRunner(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_workflowrun_")

    def _test_workflow(self, wdl:str, inputs = None, expected_exception: Exception = None):
        try:
            doc = WDL.parse_document(wdl)
            assert doc.workflow
            doc.typecheck()
            if isinstance(inputs, dict):
                inputs = WDL.values_from_json(inputs, doc.workflow.available_inputs, doc.workflow.required_inputs)
            rundir, outputs = WDL.runtime.run_local_workflow(doc.workflow, (inputs or []), parent_dir=self._dir)
        except WDL.runtime.TaskFailure as exn:
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

    def test_hello(self):
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
            output {
                Int? x_out = x
                Int? y_out = y
                Int? z_out = z
            }
        }
        """)
        self.assertEqual(outputs, {"x_out": 2, "y_out": None, "z_out": 4})

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
