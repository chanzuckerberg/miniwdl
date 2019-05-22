import unittest
import logging
import tempfile
import os
import docker
from .context import WDL

class TestTaskRunner(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_taskrun_")

    def _test_task(self, wdl:str, inputs: WDL.Env.Values = None, expected_outputs: WDL.Env.Values = None, expected_exception: Exception = None):
        doc = WDL.parse_document(wdl)
        assert len(doc.tasks) == 1
        doc.typecheck()
        if expected_exception:
            try:
                WDL.runtime.run_local_task(doc.tasks[0], (inputs or []), parent_dir=self._dir)
            except WDL.runtime.task.TaskFailure as exn:
                self.assertIsInstance(exn.__context__, expected_exception)
                return exn.__context__
            self.assertFalse(str(expected_exception) + " not raised")
        rundir, outputs = WDL.runtime.run_local_task(doc.tasks[0], (inputs or []), parent_dir=self._dir)
        if expected_outputs is not None:
            self.assertEqual(outputs, expected_outputs)
        return outputs

    def test_docker(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command <<<
                cat /etc/issue
            >>>
            output {
                String issue = read_string(stdout())
            }
        }
        """)
        self.assertTrue("18.04" in WDL.Env.resolve(outputs, [], "issue").value)

        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command <<<
                cat /etc/issue
            >>>
            runtime {
                docker: "ubuntu:18.10"
            }
            output {
                String issue = read_string(stdout())
            }
        }
        """)
        self.assertTrue("18.10" in WDL.Env.resolve(outputs, [], "issue").value)

        self._test_task(R"""
        version 1.0
        task hello {
            command <<<
                cat /etc/issue
            >>>
            runtime {
                docker: "nonexistent:202407"
            }
        }
        """, expected_exception=docker.errors.ImageNotFound)

    def test_hello_blank(self):
        self._test_task(R"""
        version 1.0
        task hello_blank {
            input {
                String who
            }
            command <<<
                echo "Hello, ~{who}!"
            >>>
        }
        """,
        WDL.Env.bind([], [], "who", WDL.Value.String("Alyssa")))

    def test_hello_file(self):
        with open(os.path.join(self._dir, "alyssa.txt"), "w") as outfile:
            outfile.write("Alyssa")
        outputs = self._test_task(R"""
            version 1.0
            task hello_file {
                input {
                    File who
                }
                command <<<
                    echo -n "Hello, $(cat ~{who})!" > message.txt
                >>>
                output {
                    File message = "message.txt"
                }
            }
            """,
            WDL.Env.bind([], [], "who", WDL.Value.File(os.path.join(self._dir, "alyssa.txt"))))
        with open(WDL.Env.resolve(outputs, [], "message").value) as infile:
            self.assertEqual(infile.read(), "Hello, Alyssa!")

        # output an input file
        outputs = self._test_task(R"""
            version 1.0
            task hello_file {
                input {
                    File who
                }
                command <<<
                    echo -n "Hello, $(cat ~{who})!"
                >>>
                output {
                    File who2 = who
                }
            }
            """,
            WDL.Env.bind([], [], "who", WDL.Value.File(os.path.join(self._dir, "alyssa.txt"))))
        self.assertEqual(WDL.Env.resolve(outputs, [], "who2").value, os.path.join(self._dir, "alyssa.txt"))

        # stdout()
        outputs = self._test_task(R"""
            version 1.0
            task hello_file {
                input {
                    File who
                }
                command <<<
                    echo -n "Hello, $(cat ~{who})!"
                >>>
                output {
                    File message = stdout()
                }
            }
            """,
            WDL.Env.bind([], [], "who", WDL.Value.File(os.path.join(self._dir, "alyssa.txt"))))
        self.assertEqual(os.path.basename(WDL.Env.resolve(outputs, [], "message").value), "stdout.txt")
        with open(WDL.Env.resolve(outputs, [], "message").value) as infile:
            self.assertEqual(infile.read(), "Hello, Alyssa!")

        # attempt to output an existent but illegal file
        self._test_task(R"""
        version 1.0
        task hello {
            command {}
            output {
                File issue = "/etc/issue"
            }
        }
        """, expected_exception=WDL.runtime.task.OutputError)

    def test_write_lines(self):
        outputs = self._test_task(R"""
            version 1.0
            task hello_friends {
                input {
                    Array[String] friends
                }
                command <<<
                    awk '{printf(" Hello, %s!",$0)}' ~{write_lines(friends)}
                >>>
                output {
                    String messages = read_string(stdout())
                }
            }
            """,
            WDL.Env.bind([], [], "friends", WDL.Value.from_json(WDL.Type.Array(WDL.Type.String()), ["Alyssa", "Ben"])))
        self.assertEqual(WDL.Env.resolve(outputs, [], "messages").value, " Hello, Alyssa! Hello, Ben!")

        outputs = self._test_task(R"""
            version 1.0
            task hello_friends2 {
                input {
                    Array[String] friends
                }
                File friends_txt = write_lines(friends)
                command <<<
                    awk '{printf(" Hello, %s!",$0)}' ~{friends_txt}
                >>>
                output {
                    String messages = read_string(stdout())
                }
            }
            """,
            WDL.Env.bind([], [], "friends", WDL.Value.from_json(WDL.Type.Array(WDL.Type.String()), ["Alyssa", "Ben"])))
        self.assertEqual(WDL.Env.resolve(outputs, [], "messages").value, " Hello, Alyssa! Hello, Ben!")
