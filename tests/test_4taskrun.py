import unittest
import logging
import tempfile
import os
import docker
import signal
import time
import json
import platform
import multiprocessing
from .context import WDL
from testfixtures import log_capture

class TestTaskRunner(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        cls.logger = logging.getLogger(cls.__name__)

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_taskrun_")

    def _test_task(self, wdl:str, inputs = None, expected_exception: Exception = None, cfg = None, **kwargs):
        cfg = cfg or WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        try:
            doc = WDL.parse_document(wdl)
            assert len(doc.tasks) == 1
            doc.typecheck()
            assert len(doc.tasks[0].required_inputs.subtract(doc.tasks[0].available_inputs)) == 0
            if isinstance(inputs, dict):
                inputs = WDL.values_from_json(inputs, doc.tasks[0].available_inputs, doc.tasks[0].required_inputs)
            rundir, outputs = WDL.runtime.run_local_task(cfg, doc.tasks[0], (inputs or WDL.Env.Bindings()), run_dir=self._dir, logger_prefix=[self.id()], **kwargs)
            self._rundir = rundir
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
        self.assertTrue("20.04" in outputs["issue"])

        outputs = self._test_task(R"""
        version 1.0
        # issue #327
        task longname9012345678901234567890123456789012345678901234567890123456789012345678901234567890 {
            command <<<
                cat /etc/issue
            >>>
            runtime {
                docker: "ubuntu:19.10"
            }
            output {
                String issue = read_string(stdout())
            }
        }
        """)
        self.assertTrue("19.10" in outputs["issue"])

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
        """, {"version": "19.10"})
        self.assertTrue("19.10" in outputs["issue"])

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
        """, expected_exception=WDL.Error.RuntimeError)

        # issue #232
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command <<<
                cat /etc/issue
            >>>
            output {
                String issue = read_string(stdout())
            }
            runtime {
                docker: "frolvlad/alpine-bash"
            }
        }
        """)
        self.assertTrue("Alpine Linux" in outputs["issue"])

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

        std_error_msgs = [record for record in capture.records if str(record.name).endswith("stderr")]

        self.assertEqual(std_error_msgs.pop(0).msg, "Start logging")
        self.assertEqual(std_error_msgs.pop().msg, "End logging")
        for record in std_error_msgs:
            line_written = int(record.msg.split('=')[1])
            self.assertGreater(record.created, line_written)
            # check line logged within 4 seconds of being written
            self.assertGreater(line_written+4, record.created)

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
        std_error_msgs = [record for record in capture.records if str(record.name).endswith("stderr")]

        self.assertEqual(len(std_error_msgs), 6)
        self.assertEqual(std_error_msgs[0].msg, "Part onePart two")

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
        hello = R"""
        version 1.0
        task hello_file {
            input {
                File who
            }
            command <<<
                set -e
                [ -s "~{who}" ]
                echo -n "Hello, $(cat ~{who})!" > message.txt
            >>>
            output {
                File message = "message.txt"
            }
        }
        """
        outputs = self._test_task(hello, {"who": os.path.join(self._dir, "alyssa.txt")})
        with open(outputs["message"]) as infile:
            self.assertEqual(infile.read(), "Hello, Alyssa!")

        self._test_task(hello, {"who": "/a/nonexistent/file"}, expected_exception=WDL.Error.InputError)

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
        self.assertEqual(os.path.realpath(outputs["who2"]), os.path.realpath(os.path.join(self._dir, "alyssa.txt")))

        # stdout()
        outputs = self._test_task(R"""
            version 1.0
            task hello_file {
                input {
                    File who
                }
                command <<<
                    set -e
                    [ -s "~{who}" ]
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

    def test_command_escaping(self):
        # miniwdl evaluates escape sequences in WDL string constants, but in commands it should
        # leave them for the shell to deal with
        output = self._test_task(R"""
        version 1.0
        task hello {
            command {
                echo '1\n2\n3' | wc -l > count1
                echo '${"1\n2\n3"}' | wc -l > count2
            }
            output {
                Int count1 = read_int("count1")
                Int count2 = read_int("count2")
            }
        }
        """)
        self.assertEqual(output["count1"], 1)
        self.assertEqual(output["count2"], 3)

        output = self._test_task(R"""
        version 1.0
        task hello {
            command <<<
                echo '1\n2\n3' | wc -l > count1
                echo '~{"1\n2\n3"}' | wc -l > count2
            >>>
            output {
                Int count1 = read_int("count1")
                Int count2 = read_int("count2")
            }
        }
        """)
        self.assertEqual(output["count1"], 1)
        self.assertEqual(output["count2"], 3)


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

        outputs = self._test_task(R"""
        version 1.0
        task hello {
            input {
                Array[String] who
            }
            File who_file = write_lines(who)
            Array[String] who0 = read_lines(who_file)
            command {
                echo ~{who_file}
            }
            output {
                Array[String] who1 = who0
                Array[String] who2 = read_lines(read_string(stdout()))
            }
        }
        """, {"who": ["Alyssa", "Ben"]})
        self.assertEqual(outputs["who1"], ["Alyssa", "Ben"])
        self.assertEqual(outputs["who2"], ["Alyssa", "Ben"])

    def test_command_failure(self):
        self._test_task(R"""
        version 1.0
        task hello {
            command {
                exit 1
            }
        }
        """, expected_exception=WDL.runtime.CommandFailed)

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
            String? ns
            command {
                echo "~{s0}"
                echo "~{s1}"
                echo "~{if (defined(s2)) then s2 else 'None'}"
            }
            output {
                String out = read_string(stdout())
                String? null_string = ns
            }
        }
        """
        outputs = self._test_task(code, {"s0": "alyssa"})
        self.assertEqual(outputs["out"], "alyssa\nben\nNone")
        self.assertEqual(outputs["null_string"], None)

        outputs = self._test_task(code, {"s0": "alyssa", "s1": "cy"})
        self.assertEqual(outputs["out"], "alyssa\ncy\nNone")

        outputs = self._test_task(code, {"s0": "alyssa", "s2": "mallory"})
        self.assertEqual(outputs["out"], "alyssa\nben\nmallory")

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
                    5: ([6,7],"8")
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
                    year: "2017"
                }
                Car car2 = {
                    "model": "Toyota"
                }
            }
        }
        """)
        self.assertEqual(outputs["car"], {"model": "Mazda", "year": 2017, "mileage": None})
        self.assertEqual(outputs["car2"], {"model": "Toyota", "year": None, "mileage": None})
        # bad struct init from map
        self._test_task(R"""
        version 1.0
        struct Car {
            String model
            Float mileage
        }
        task t {
            command {}
            output {
                Car car = {
                    "model": "Mazda",
                    "mileage": "bogus"
                }
            }
        }
        """, expected_exception=WDL.Error.EvalError)

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
        txt = R"""
        version 1.0
        task t {
            input {
                Array[File] files
            }
            command {
                cat "~{write_lines(files)}"
            }
            output {
                Array[String] outfiles = read_lines(stdout())
            }
        }
        """
        inp = {"files": [
            os.path.join(self._dir, "a", "x"),
            os.path.join(self._dir, "a", "x.y"),
            os.path.join(self._dir, "b", "x"),
            os.path.join(self._dir, "b", "x.y"),
            os.path.join(self._dir, "b", "x.y") # intentional duplicate
        ]}
        def chk(outfiles):
            self.assertEqual(len(outfiles), 5)
            self.assertEqual(os.path.basename(outfiles[0]), "x")
            self.assertEqual(os.path.basename(outfiles[1]), "x.y")
            self.assertEqual(os.path.dirname(outfiles[0]), os.path.dirname(outfiles[1]))
            self.assertEqual(os.path.basename(outfiles[2]), "x")
            self.assertEqual(os.path.basename(outfiles[3]), "x.y")
            self.assertEqual(os.path.dirname(outfiles[2]), os.path.dirname(outfiles[3]))
            self.assertNotEqual(os.path.dirname(outfiles[0]), os.path.dirname(outfiles[2]))
            self.assertEqual(outfiles[3], outfiles[4])

        outputs = self._test_task(txt, inp)
        chk(outputs["outfiles"])
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        cfg.override({"file_io": {"copy_input_files": True}})
        outputs = self._test_task(txt, inp, cfg=cfg)
        chk(outputs["outfiles"])

        with self.assertRaises(WDL.Error.InputError):
            self._test_task(txt, {"files": [os.path.join(self._dir, "a", "x") + "/"]})

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
        # also just covering codepaths for stderr status bar logging:
        with WDL._util.configure_logger(force_tty=True) as set_status, WDL.runtime._statusbar.enable(set_status):
            signal.alarm(10)
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
            self.assertLess(t1 - t0, 20)
        with WDL._util.configure_logger(json=True) as _:
            pass

    def test_orphan_background_process(self):
        # TODO: https://github.com/chanzuckerberg/miniwdl/issues/211
        output = self._test_task(R"""
        version 1.0
        task t {
            input {
                Int w
            }
            command {
                touch log
                echo -n wow | dd of=>(sleep ${w}; dd of=log)
            }
            output {
                Float logsize = size("log")
            }
        }
        """, {"w": 2})
        self.assertAlmostEqual(output["logsize"], 0.0)

    def test_cpu_limit(self):
        txt = R"""
        version 1.0
        task spin {
            input {
                Int n
                Int cpu
            }
            command <<<
                set -x
                source /root/.profile
                cat << EOF > spin_one_cpu_second.py
                import time
                while time.process_time() < 1.0:
                    pass
                EOF
                t0=$(date +"%s")
                for i in $(seq ~{n}); do
                    python3 spin_one_cpu_second.py &
                done
                wait
                t1=$(date +"%s")
                echo $(( t1 - t0 )) > wall_seconds
            >>>
            output {
                Int wall_seconds = read_int("wall_seconds")
            }
            runtime {
                container: ["continuumio/miniconda3"]
                cpu: cpu
            }
        }
        """
        # 4 concurrent spinners limited to 1 cpu should take 4 seconds
        outputs = self._test_task(txt, {"n": 4, "cpu": 1})
        self.assertGreaterEqual(outputs["wall_seconds"], 4)
        # 8 concurrent spinners on >1 cpus should take <8 seconds
        # (disabled on systems with <4 cpus)
        if multiprocessing.cpu_count() >= 4:
            outputs = self._test_task(txt, {"n": 8, "cpu": 4})
            self.assertLess(outputs["wall_seconds"], 8)
            # check task with overkill number of CPUs gets scheduled
            outputs = self._test_task(txt, {"n": 8, "cpu": 9999})
            self.assertLessEqual(outputs["wall_seconds"], 8)
        # check runtime_cpu_max set to 1 causes serialization
        outputs = self._test_task(txt, {"n": 8, "cpu": 9999}, cfg=WDL.runtime.config.Loader(logging.getLogger(self.id()), overrides={"task_runtime": {"cpu_max": 1}}))
        self.assertGreaterEqual(outputs["wall_seconds"], 8)

    def test_runtime_memory(self):
        txt = R"""
        version 1.0
        task nop {
            input {
                String memory
            }
            command <<<
                echo "~{memory}"
            >>>
            runtime {
                memory: "~{memory}"
                disks: "ignored"
            }
        }
        """
        self._test_task(txt, {"memory": "100000000"})
        self._test_task(txt, {"memory": "1G"})
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        cfg.override({"task_runtime": {"defaults": json.dumps({"docker":"ubuntu:18.10","cpu":1})}})
        self._test_task(txt, {"memory": "99T"}, cfg=cfg)
        cfg.override({"task_runtime": {"memory_max": " 123.45 MiB "}})
        self._test_task(txt, {"memory": "99T"}, cfg=cfg)
        self._test_task(txt, {"memory": "-1"}, expected_exception=WDL.Error.EvalError)
        self._test_task(txt, {"memory": "1Gaga"}, expected_exception=WDL.Error.EvalError)
        self._test_task(txt, {"memory": "bogus"}, expected_exception=WDL.Error.EvalError)

    def test_runtime_memory_limit(self):
        txt = R"""
        version 1.0
        task limit {
            input {
                String memory
            }
            command <<<
                cat /sys/fs/cgroup/memory/memory.limit_in_bytes \
                    || cat /sys/fs/cgroup/memory.max
            >>>
            output {
                String memory_limit_in_bytes = read_string(stdout())
            }
            runtime {
                cpu: 1
                memory: "~{memory}"
            }
        }
        """
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        outputs = self._test_task(txt, {"memory": "256MB"}, cfg=cfg)
        if outputs["memory_limit_in_bytes"] != "max":
            self.assertGreater(int(outputs["memory_limit_in_bytes"]), 300*1024*1024)
        cfg.override({"task_runtime": {"memory_limit_multiplier": 0.9}})
        outputs = self._test_task(txt, {"memory": "256MB"}, cfg=cfg)
        self.assertLess(int(outputs["memory_limit_in_bytes"]), 300*1024*1024)

    def test_runtime_returnCodes(self):
        txt = R"""
        version 1.0
        task limit {
            input {
                Int status
            }
            command <<<
                echo Hi
                exit ~{status}
            >>>
            output {
                File out = stdout()
            }
            runtime {
                returnCodes: 42
            }
        }
        """
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        self._test_task(txt, {"status": 0}, cfg=cfg, expected_exception=WDL.runtime.CommandFailed)
        self._test_task(txt, {"status": 42}, cfg=cfg)
        txt = R"""
        version 1.0
        task limit {
            input {
                Int status
            }
            command <<<
                echo Hi
                exit ~{status}
            >>>
            output {
                File out = stdout()
            }
            runtime {
                returnCodes: [0,42]
            }
        }
        """
        self._test_task(txt, {"status": 0}, cfg=cfg)
        self._test_task(txt, {"status": 42}, cfg=cfg)
        self._test_task(txt, {"status": 41}, cfg=cfg, expected_exception=WDL.runtime.CommandFailed)
        txt = R"""
        version 1.0
        task limit {
            input {
                Int status
            }
            command <<<
                echo Hi
                exit ~{status}
            >>>
            output {
                File out = stdout()
            }
            runtime {
                returnCodes: "*"
            }
        }
        """
        self._test_task(txt, {"status": 0}, cfg=cfg)
        self._test_task(txt, {"status": 42}, cfg=cfg)

    def test_input_files_rw(self):
        txt = R"""
        version 1.0
        task clobber {
            input {
                Array[File] files
            }
            command <<<
                set -x
                touch ~{sep=" " files}
                mv ~{files[0]} alyssa2.txt
                rm ~{files[1]}
            >>>
            output {
                File outfile = glob("*.txt")[0]
            }
        }
        """
        with open(os.path.join(self._dir, "alyssa.txt"), "w") as outfile:
            outfile.write("Alyssa\n")
        with open(os.path.join(self._dir, "ben.txt"), "w") as outfile:
            outfile.write("Ben\n")

        self._test_task(txt, {"files": [os.path.join(self._dir, "alyssa.txt"), os.path.join(self._dir, "ben.txt")]},
                        expected_exception=WDL.runtime.CommandFailed)

        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        cfg.override({"file_io": {"copy_input_files": True}})
        outputs = self._test_task(txt, {"files": [os.path.join(self._dir, "alyssa.txt"), os.path.join(self._dir, "ben.txt")]}, cfg=cfg)
        self.assertTrue(outputs["outfile"].endswith("alyssa2.txt"))

        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        cfg.override({"file_io": {"copy_input_files_for": ["clobber"]}})
        outputs = self._test_task(txt, {"files": [os.path.join(self._dir, "alyssa.txt"), os.path.join(self._dir, "ben.txt")]}, cfg=cfg)
        self.assertTrue(outputs["outfile"].endswith("alyssa2.txt"))

        self._test_task(R"""
        version 1.0
        task rmdir {
            input {
                Array[File] files
            }
            command <<<
                set -x
                rm -rf _miniwdl*
            >>>
        }
        """, {"files": [os.path.join(self._dir, "alyssa.txt"), os.path.join(self._dir, "ben.txt")]},
             expected_exception=WDL.runtime.CommandFailed)
        self.assertTrue(os.path.exists(os.path.join(self._dir, "alyssa.txt")))

    def test_optional_file_outputs(self):
        outputs = self._test_task(R"""
        version 1.0
        task rmdir {
            String x = "foo"
            command <<<
                touch foobar.txt
            >>>
            output {
                Array[File?] files = ["~{x}bar.txt", "~{x}bas.txt"]
            }
        }
        """)
        self.assertEqual(len(outputs["files"]), 2)
        self.assertIsNotNone(outputs["files"][0])
        self.assertIsNone(outputs["files"][1])

    def test_download_input_files(self):
        self._test_task(R"""
        version 1.0
        task lines {
            input {
                File file
            }
            command {
                cat "~{file}" | wc -l
            }
            output {
                Int count = read_int(stdout())
            }
        }
        """, {"file": "https://google.com/robots.txt"},
        cfg=WDL.runtime.config.Loader(logging.getLogger(self.id()), overrides = {"task_runtime":{"as_user": True}}))

    @log_capture()
    def test_download_cache(self, capture):
        logger = logging.getLogger(self.id())
        cfg = WDL.runtime.config.Loader(logger)
        cfg.override({
            "download_cache": {
                "put": True,
                "get": True,
                "dir": os.path.join(self._dir, "cache")
            }
        })
        txt = R"""
        version 1.0
        task lines {
            input {
                File file
            }
            command {
                cat "~{file}" | wc -l
            }
            output {
                Int count = read_int(stdout())
            }
        }
        """
        inp = {"file": "https://google.com/robots.txt"}
        self._test_task(txt, inp, cfg=cfg)
        self._test_task(txt, inp, cfg=cfg)
        logs = [str(record.msg) for record in capture.records if str(record.msg).startswith("processed input URIs")]
        self.assertTrue("downloaded: 1" in logs[0])
        self.assertTrue("cached: 1" in logs[1])

        # quick test CallCache reentrancy
        with WDL.runtime.cache.CallCache(cfg, logger) as cache:
            self.assertIsNotNone(cache.get_download("https://google.com/robots.txt", logger=logger))
            self.assertIsNotNone(cache.get_download("https://google.com/robots.txt", logger=logger))
            self.assertEqual(len(cache._flocker._flocks), 1)
            with self.assertRaises(OSError):
                cache._flocker.flock(cache.download_path("https://google.com/robots.txt"), exclusive=True)

    @unittest.skipIf(platform.system() == "Darwin", reason="https://stackoverflow.com/a/43213455")
    def test_workdir_ownership(self):
        # verify that everything within working directory is owned by the invoking user
        txt = R"""
        version 1.0
        task clobber {
            input {
                Array[File] files
            }
            command <<<
                set -euxo pipefail
                ls -alR .. > /dev/stderr
                find . | grep -Fv uids.txt | xargs -t -n 1 stat -c %u | sort | uniq > uids.txt
            >>>
            output {
                Array[Int] uids = read_lines("uids.txt")
            }
        }
        """
        with open(os.path.join(self._dir, "alyssa.txt"), "w") as outfile:
            outfile.write("Alyssa\n")
        with open(os.path.join(self._dir, "ben.txt"), "w") as outfile:
            outfile.write("Ben\n")

        outputs = self._test_task(txt, {"files": [os.path.join(self._dir, "alyssa.txt"), os.path.join(self._dir, "ben.txt")]})
        self.assertEqual(len(outputs["uids"]), 1)
        self.assertEqual(outputs["uids"][0], os.geteuid())

    def test_delete_work(self):
        txt = R"""
        version 1.0
        task xxx {
            input {
                Array[File] files
            }
            File written = write_lines(files)
            command <<<
                set -euxo pipefail
                cp "~{written}" foo.txt
                cp "~{files[0]}" bar.txt
            >>>
            output {
                Array[File] outfiles = [write_lines(files), "foo.txt", "bar.txt"]
            }
        }
        """
        with open(os.path.join(self._dir, "alyssa.txt"), "w") as outfile:
            outfile.write("Alyssa\n")
        with open(os.path.join(self._dir, "ben.txt"), "w") as outfile:
            outfile.write("Ben\n")
        inputs = {"files": [os.path.join(self._dir, "alyssa.txt"), os.path.join(self._dir, "ben.txt")]}
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        cfg.override({"file_io": {"output_hardlinks": True, "delete_work": "success"}})
        output = self._test_task(txt, inputs, cfg=cfg)
        self.assertFalse(os.path.isdir(os.path.join(self._rundir, "work")))
        self.assertFalse(os.path.isdir(os.path.join(self._rundir, "write_")))
        for fn in output["outfiles"]:
            self.assertTrue(os.path.isfile(fn) and not os.path.islink(fn))

        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        cfg.override({"file_io": {"output_hardlinks": False, "delete_work": "success"}})
        output = self._test_task(txt, inputs, cfg=cfg)
        self.assertTrue(os.path.isfile(os.path.join(self._rundir, "work", "foo.txt")))

    def test_plugins(self):
        def my_plugin(cfg, logger, task, run_id, run_dir, **recv):
            logger = logger.getChild("my_plugin")
            logger.critical("hello")
            yv = None
            try:
                xv = recv["inputs"].resolve_binding("x")
                xv.value.value += 1
                recv = yield recv
                if xv.value.value == 43:
                    raise RuntimeError("oh no you don't!")
                recv = yield recv
                yv = recv["outputs"].resolve_binding("y")
                yv.value.value += 1
                yield recv
            except WDL.runtime.error.CommandFailed as exn:
                logger.error(exn)
                exn.args = ("i was here",)
                exn.exit_status = 42
                raise
            finally:
                logger.critical("goodbye")
                if yv and yv.value.value == 43:
                    raise RuntimeError("goodbye")
        txt = R"""
        version 1.0
        task inc {
            input {
                Int x
            }
            command {}
            output {
                Int y = x+1
            }
        }
        """
        outputs = self._test_task(txt, {"x": 1}, _plugins=[my_plugin])
        self.assertEqual(outputs["y"], 4)

        try:
            self._test_task(txt, {"x": 42}, _plugins=[my_plugin])
        except RuntimeError as exn:
            assert exn.args[0] == "oh no you don't!"

        try:
            self._test_task(txt, {"x": 40}, _plugins=[my_plugin])
        except RuntimeError as exn:
            assert exn.args[0] == "goodbye"

        txt = R"""
        version 1.0
        task failer2000 {
            input {
                Int x
            }
            command {
                exit 1
            }
            output {
                Int y = x+1
            }
        }
        """
        try:
            self._test_task(txt, {"x": 1}, _plugins=[my_plugin])
        except WDL.runtime.error.CommandFailed as exn:
            self.assertEqual(exn.exit_status, 42)

    def test_runtime_privileged(self):
        txt = R"""
        version 1.0
        task xxx {
            input {
                Boolean privileged
            }
            command {
                dmesg > /dev/null
            }
            output {
            }
            runtime {
                privileged: privileged
            }
        }
        """
        self._test_task(txt, {"privileged": False}, expected_exception=WDL.runtime.CommandFailed)
        self._test_task(txt, {"privileged": True}, expected_exception=WDL.runtime.CommandFailed)
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        cfg.override({"task_runtime": {"allow_privileged": True}})
        self._test_task(txt, {"privileged": False}, cfg=cfg, expected_exception=WDL.runtime.CommandFailed)
        self._test_task(txt, {"privileged": True}, cfg=cfg)


class TestConfigLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        logger = logging.getLogger(cls.__name__)
        cfg = WDL.runtime.config.Loader(logger, [])

    # trigger various codepaths of the config loader that wouldn't be exercised otherwise
    def test_basic(self):
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        self.assertEqual(cfg["file_io"]["copy_input_files"], "false")
        self.assertEqual(cfg["file_io"].get_bool("copy_input_files"), False)

        self.assertEqual(cfg["scheduler"].get_int("call_concurrency"), 0)

        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), overrides = {"file_io":{"copy_input_files": "true"}})
        self.assertEqual(cfg["file_io"].get_bool("copy_input_files"), True)

        with self.assertRaises(WDL.runtime.config.ConfigMissing):
            cfg.get("bogus", "key")
        with self.assertRaises(WDL.runtime.config.ConfigMissing):
            cfg.get("file_io", "bogus")
        self.assertTrue(cfg.has_option("file_io", "copy_input_files"))
        self.assertFalse(cfg.has_option("bogus", "key"))
        self.assertFalse(cfg.has_option("file_io", "bogus"))

        with self.assertRaises(ValueError):
            cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), overrides = {"file_io":{"copy_input_files": "bogus123"}})
            cfg.get_bool("file_io", "copy_input_files")

    def test_env(self):
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
            assert(os.path.isabs(tmp.name))
            print("""
            [file_io]
            copy_input_files = true
            expansion = $HOME
            made_up = 42
            """, file=tmp)
            tmp.flush()
            os.environ["MINIWDL_CFG"] = tmp.name
            os.environ["MINIWDL__SCHEDULER__CALL_CONCURRENCY"] = "4"
            os.environ["MINIWDL__BOGUS__OPTION"] = "42"
            cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()))
            cfg.override({"bogus": {"option2": "42"}})
            self.assertEqual(cfg["scheduler"].get_int("call_concurrency"), 4)
            self.assertEqual(cfg["file_io"].get_bool("copy_input_files"), True)
            cfg.log_all()
            cfg.log_unused_options()
            self.assertTrue(os.path.isabs(cfg["file_io"]["expansion"]))
