import unittest
import logging
import tempfile
import os
import docker
import signal
import time
from .context import WDL
from testfixtures import log_capture

class TestTaskRunner(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_taskrun_")

    def _test_task(self, wdl:str, inputs = None, expected_exception: Exception = None):
        try:
            doc = WDL.parse_document(wdl)
            assert len(doc.tasks) == 1
            doc.typecheck()
            if isinstance(inputs, dict):
                inputs = WDL.values_from_json(inputs, doc.tasks[0].available_inputs, doc.tasks[0].required_inputs)
            rundir, outputs = WDL.runtime.run_local_task(doc.tasks[0], (inputs or []), run_dir=self._dir)
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
            self.assertTrue(False, str(expected_exception) + " not raised")
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

    @log_capture()
    def test_logging_std_err(self, capture):
        self._test_task(R"""
        version 1.0
        task std_err_log_check {
            input {}
            command <<<
                >&2 echo "Start logging"
                >&2 echo "0="$(date +"%s")
                sleep 1
                >&2 echo "1="$(date +"%s")
                sleep 1
                >&2 echo "2="$(date +"%s")
                sleep 1
                >&2 echo "3="$(date +"%s")
                sleep 1
                >&2 echo "4="$(date +"%s")
                sleep 1
                >&2 echo "End logging"
            >>>
            
        }
        """)

        std_error_msgs = [record for record in capture.records if record.msg.startswith("2|")]

        self.assertEqual(std_error_msgs.pop(0).msg, "2| Start logging")
        self.assertEqual(std_error_msgs.pop().msg, "2| End logging")
        for record in std_error_msgs:
            line_written = int(record.msg.split('=')[1])
            self.assertGreater(record.created, line_written)
            # check line logged within 2 seconds of being written
            self.assertGreater(line_written+2, record.created)

    @log_capture()
    def test_logging_std_err_captures_full_line(self, capture):
        self._test_task(R"""
                version 1.0
                task std_err_log_check {
                    input {}
                    command <<<
                        >&2 printf "Part one"
                        sleep 2
                        >&2 echo "Part two"
                        >&2 echo "1="$(date +"%s")
                        sleep 1
                        >&2 echo "2="$(date +"%s")
                        sleep 1
                        >&2 echo "3="$(date +"%s")
                        sleep 1
                        >&2 echo "4="$(date +"%s")
                        sleep 1
                        >&2 echo "End logging"
                    >>>

                }
                """)
        std_error_msgs = [record for record in capture.records if record.msg.startswith("2|")]

        self.assertEqual(len(std_error_msgs), 6)
        self.assertEqual(std_error_msgs[0].msg, "2| Part onePart two")

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
        """, expected_exception=WDL.runtime.OutputError)

        # attempt to output file which exists but we're not allowed to output
        self._test_task(R"""
        version 1.0
        task hello {
            command {}
            output {
                File issue = "/etc/issue"
            }
        }
        """, expected_exception=WDL.runtime.OutputError)

        self._test_task(R"""
        version 1.0
        task hello {
            String trick = "/etc"
            command {}
            output {
                File issue = trick + "/issue"
            }
        }
        """, expected_exception=WDL.runtime.OutputError)

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
        """, expected_exception=WDL.runtime.OutputError)

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

        # attempt to output symlink to host /etc/passwd
        outputs = self._test_task(R"""
        version 1.0
        task hacker {
            command {
                ln -s /etc/passwd host_passwords.txt
            }
            output {
                File your_passwords = "host_passwords.txt"
            }
        }
        """, expected_exception=WDL.runtime.OutputError)

        outputs = self._test_task(R"""
        version 1.0
        task hacker {
            command {
                ln -s /etc/passwd host_passwords.txt
            }
            output {
                String host_passwords = read_string("host_passwords.txt")
            }
        }
        """, expected_exception=WDL.Error.EvalError)

        outputs = self._test_task(R"""
        version 1.0
        task hacker {
            command {
                ln -s /etc your_etc
            }
            output {
                File your_passwords = "your_etc/passwd"
            }
        }
        """, expected_exception=WDL.runtime.OutputError)

    def test_command_failure(self):
        self._test_task(R"""
        version 1.0
        task hello {
            command {
                exit 1
            }
        }
        """, expected_exception=WDL.runtime.CommandFailure)

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
                File alyssa_csv = "alyssa.csv"
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
        with open(outputs["alyssa_csv"]) as infile:
            self.assertEqual(infile.read(), "Alyssa,")

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

    def test_coercion(self):
        self._test_task(R"""
        version 1.0
        task t {
            input {
                Map[String,Pair[Array[String],Float]] x = {
                    1: ([2,3],4),
                    5: ([6,7],8)
                }
            }
            command {}
        }
        """)
        outputs = self._test_task(R"""
        version 1.0
        struct Car {
            String model
            Int? year
            Int? mileage
        }
        task t {
            command {}
            output {
                Car car = object {
                    model: "Mazda",
                    year: 2017
                }
                Car car2 = {
                    "model": "Toyota"
                }
            }
        }
        """)
        self.assertEqual(outputs["car"], {"model": "Mazda", "year": 2017, "mileage": None})
        self.assertEqual(outputs["car2"], {"model": "Toyota", "year": None, "mileage": None})

    def test_errors(self):
        self._test_task(R"""
        version 1.0
        task t {
            input {
                Array[Int] x = []
            }
            Array[Int]+ y = x
            command {}
        }
        """, expected_exception=WDL.Error.EmptyArray)
        self._test_task(R"""
        version 1.0
        task t {
            input {
                Array[Int] x = []
            }
            command {}
            output {
                Array[Int]+ y = x
            }
        }
        """, expected_exception=WDL.Error.EmptyArray)

    def test_filename_collisions(self):
        os.mkdir(os.path.join(self._dir, "a"))
        with open(os.path.join(self._dir, "a", "x"), "w") as outfile:
            outfile.write("x\n")
        with open(os.path.join(self._dir, "a", "x.y"), "w") as outfile:
            outfile.write("x.y\n")
        os.mkdir(os.path.join(self._dir, "b"))
        with open(os.path.join(self._dir, "b", "x"), "w") as outfile:
            outfile.write("x\n")
        with open(os.path.join(self._dir, "b", "x.y"), "w") as outfile:
            outfile.write("x.y\n")
        outputs = self._test_task(R"""
        version 1.0
        task t {
            input {
                Array[File] files
            }
            command {
                sort "~{write_lines(files)}"
            }
            output {
                Array[String] outfiles = read_lines(stdout())
            }
        }
        """, {"files": [
            os.path.join(self._dir, "a", "x"),
            os.path.join(self._dir, "a", "x.y"),
            os.path.join(self._dir, "b", "x"),
            os.path.join(self._dir, "b", "x.y"),
            os.path.join(self._dir, "b", "x.y") # intentional duplicate
        ]})
        outfiles = outputs["outfiles"]
        self.assertEqual(len(outfiles), 5)
        self.assertEqual(os.path.basename(outfiles[0]), "x")
        self.assertEqual(os.path.basename(outfiles[1]), "x.y")
        self.assertEqual(os.path.dirname(outfiles[0]), os.path.dirname(outfiles[1]))
        self.assertEqual(os.path.basename(outfiles[2]), "x")
        self.assertEqual(os.path.basename(outfiles[3]), "x.y")
        self.assertEqual(os.path.dirname(outfiles[2]), os.path.dirname(outfiles[3]))
        self.assertNotEqual(os.path.dirname(outfiles[0]), os.path.dirname(outfiles[2]))
        self.assertEqual(outfiles[3], outfiles[4])

    def test_topsort(self):
        txt = R"""
        version 1.0
        task t {
            input {
                Int y = z-2
                Int x
            }
            Int z = x*2
            command {}
            output {
                Int yy = y
            }
        }
        """
        self.assertEqual(self._test_task(txt, {"x": 22})["yy"], 42)
        self.assertEqual(self._test_task(txt, {"x": 22, "y": 99})["yy"], 99)

    def test_signal(self):
        signal.alarm(3)
        t0 = time.time()
        self._test_task(R"""
        version 1.0
        task t {
            command {
                sleep 30
            }
        }
        """, expected_exception=WDL.runtime.Terminated)
        t1 = time.time()
        self.assertLess(t1 - t0, 15)
