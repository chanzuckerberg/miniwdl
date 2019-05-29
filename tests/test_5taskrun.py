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
        self.assertTrue("18.04" in outputs["issue"])

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
        self.assertTrue("18.10" in outputs["issue"])

        outputs = self._test_task(R"""
        version 1.0
        task hello {
            String version
            command <<<
                cat /etc/issue
            >>>
            runtime {
                docker: "ubuntu:" + version
            }
            output {
                String issue = read_string(stdout())
            }
        }
        """, {"version": "18.10"})
        self.assertTrue("18.10" in outputs["issue"])

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
        """, {"who": "Alyssa"})

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
            {"who": os.path.join(self._dir, "alyssa.txt")})
        with open(outputs["message"]) as infile:
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
            {"who": os.path.join(self._dir, "alyssa.txt")})
        self.assertEqual(outputs["who2"], os.path.join(self._dir, "alyssa.txt"))

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
            {"who": os.path.join(self._dir, "alyssa.txt")})
        self.assertEqual(os.path.basename(outputs["message"]), "stdout.txt")
        with open(outputs["message"]) as infile:
            self.assertEqual(infile.read(), "Hello, Alyssa!")

    def test_weird_output_files(self):
        # nonexistent output file
        self._test_task(R"""
        version 1.0
        task hello {
            command {}
            output {
                File issue = "bogus.txt"
            }
        }
        """, expected_exception=WDL.runtime.task.OutputError)

        # attempt to output file which exists but we're not allowed to output
        self._test_task(R"""
        version 1.0
        task hello {
            command {}
            output {
                File issue = "/etc/issue"
            }
        }
        """, expected_exception=WDL.runtime.task.OutputError)

        self._test_task(R"""
        version 1.0
        task hello {
            String trick = "/etc"
            command {}
            output {
                File issue = trick + "/issue"
            }
        }
        """, expected_exception=WDL.runtime.task.OutputError)

        self._test_task(R"""
        version 1.0
        task hello {
            command {
                touch ../nono
            }
            output {
                File issue = "../nono"
            }
        }
        """, expected_exception=WDL.runtime.task.OutputError)

        # circuitously output a file using an absolute path
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command {
                echo -n $(pwd) > my_pwd
            }
            output {
                File issue = read_string("my_pwd") + "/my_pwd"
            }
        }
        """)
        with open(outputs["issue"]) as infile:
            pass

    def test_command_error(self):
        self._test_task(R"""
        version 1.0
        task hello {
            command {
                exit 1
            }
        }
        """, expected_exception=WDL.runtime.task.CommandError)

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
            {"friends": ["Alyssa", "Ben"]})
        self.assertEqual(outputs["messages"], " Hello, Alyssa! Hello, Ben!")

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
            {"friends": ["Alyssa", "Ben"]})
        self.assertEqual(outputs["messages"], " Hello, Alyssa! Hello, Ben!")

    def test_compound_files(self):
        # tests filename mappings when Files are embedded in compound types
        with open(os.path.join(self._dir, "alyssa.txt"), "w") as outfile:
            outfile.write("Alyssa\n")
        with open(os.path.join(self._dir, "ben.txt"), "w") as outfile:
            outfile.write("Ben\n")
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            Array[File] files
            command {
                while read fn; do
                    cat "$fn"
                done < ~{write_lines(files)}
                echo -n Alyssa, > alyssa.csv
                echo -n Ben, > ben.csv
            }
            output {
                File stdout = stdout()
                Array[File] friends = ["alyssa.csv", "ben.csv"]
            }
        }
        """, {"files": [ os.path.join(self._dir, "alyssa.txt"),
                         os.path.join(self._dir, "ben.txt") ]})
        with open(outputs["stdout"]) as infile:
            self.assertEqual(infile.read(), "Alyssa\nBen\n")
        self.assertEqual(len(outputs["friends"]), 2)
        with open(outputs["friends"][0]) as infile:
            self.assertEqual(infile.read(), "Alyssa,")
        with open(outputs["friends"][1]) as infile:
            self.assertEqual(infile.read(), "Ben,")

    def test_optional_inputs(self):
        code = R"""
        version 1.0
        task defaults {
            input {
                String s0
                String s1 = "ben"
                String? s2
            }
            command {
                echo "~{s0}"
                echo "~{s1}"
                echo "~{if (defined(s2)) then s2 else 'None'}"
            }
            output {
                String out = read_string(stdout())
            }
        }
        """
        outputs = self._test_task(code, {"s0": "alyssa"})
        self.assertEqual(outputs["out"], "alyssa\nben\nNone\n")

        outputs = self._test_task(code, {"s0": "alyssa", "s1": "cy"})
        self.assertEqual(outputs["out"], "alyssa\ncy\nNone\n")

        outputs = self._test_task(code, {"s0": "alyssa", "s2": "mallory"})
        self.assertEqual(outputs["out"], "alyssa\nben\nmallory\n")

        # FIXME: need some restrictions on what File inputs can default to
        self._test_task(R"""
        version 1.0
        task hacker {
            File host_passwords = "/etc/passwd"
            command {
                >&2 cat "~{host_passwords}"
            }
            output {
                String owned = read_string(stderr())
            }
        }
        """)
