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
        """, WDL.Env.bind([], [], "version", WDL.Value.String("18.10")))
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
        with open(WDL.Env.resolve(outputs, [], "issue").value) as infile:
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
        """, WDL.Env.bind([], [], "files", WDL.Value.Array(WDL.Type.Array(WDL.Type.String), [
                WDL.Value.File(os.path.join(self._dir, "alyssa.txt")),
                WDL.Value.File(os.path.join(self._dir, "ben.txt")),
            ])))
        with open(WDL.Env.resolve(outputs, [], "stdout").value) as infile:
            self.assertEqual(infile.read(), "Alyssa\nBen\n")
        friends = WDL.Env.resolve(outputs, [], "friends")
        self.assertEqual(len(friends.value), 2)
        with open(friends.value[0].value) as infile:
            self.assertEqual(infile.read(), "Alyssa,")
        with open(friends.value[1].value) as infile:
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
        outputs = self._test_task(code, WDL.Env.bind([], [], "s0", WDL.Value.String("alyssa")))
        self.assertEqual(WDL.Env.resolve(outputs, [], "out").value, "alyssa\nben\nNone\n")

        outputs = self._test_task(code,
            WDL.Env.bind(WDL.Env.bind([], [], "s1", WDL.Value.String("cy")),
                            [], "s0", WDL.Value.String("alyssa")))
        self.assertEqual(WDL.Env.resolve(outputs, [], "out").value, "alyssa\ncy\nNone\n")

        outputs = self._test_task(code,
            WDL.Env.bind(WDL.Env.bind([], [], "s2", WDL.Value.String("mallory")),
                            [], "s0", WDL.Value.String("alyssa")))
        self.assertEqual(WDL.Env.resolve(outputs, [], "out").value, "alyssa\nben\nmallory\n")

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
