import glob
import json
import logging
import os
import stat
import random
import shutil
import tempfile
import time
import unittest
import subprocess
from unittest.mock import MagicMock, patch

from .context import WDL
from WDL import values_from_json, values_to_json
from WDL.runtime.cache import CallCache


class TestCallCache(unittest.TestCase):
    test_wdl: str = R"""
        version 1.0
        task hello_blank {
            # comment1
            input  {
                String who     # comment2
                Array[String]? what

                Map[String,Map[String,String]]? where
            }     


            command <<<
                # comment3
                echo "Hello, ~{who}!"
            >>>
            output {#comment4
                Int count = 12

            }
        }#comment5
        """
    ordered_input_dict = {
        "what": ["a", "ab", "b", "bc"],
        "where": {"places": {"Minneapolis": "a", "SanFan": "b"}},
        "who": "Alyssa",
    }
    doc = WDL.parse_document(test_wdl)
    cache_dir = '/tmp/cache/'
    struct_task: str = R"""
            version 1.0
            struct Box {
                Array[File] str
            }
            task hello {
                input {
                    Box box
                }
                command {
                    echo "Hello, world!"
                }
                output {
                    Int count = 13
                }
            }
            """
    test_wdl_with_output_files: str = R"""
        version 1.0
        task hello {
            String who
            File foo = write_lines(["foo","bar","baz"])
            File tsv = write_tsv([["one", "two", "three"], ["un", "deux", "trois"]])
            File json = write_json({"key1": "value1", "key2": "value2"})

            command <<<
                echo "Hello, ~{who}!"
            >>>

            output {
                File o_json = json
                File a_tsv = tsv
                File whynot = write_lines(["foo","bar","baz"])
                Int count = 13
                String ans = stdout()
            }
        }
        """

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        cls.logger = logging.getLogger(cls.__name__)
        cls.cfg = WDL.runtime.config.Loader(cls.logger, [])
        cls.cfg.override(
            {"call_cache": {
                "put": True,
                "get": True,
                "dir": cls.cache_dir
            }
        })

    def setUp(self):
        """
        initialize docker & provision temporary directory for a test (self._dir)
        """
        self._dir = tempfile.mkdtemp(prefix=f"miniwdl_test_{self.id()}_")

    def tearDown(self):
        shutil.rmtree(self._dir)
        try:
            shutil.rmtree(self.cache_dir)
        except FileNotFoundError:
            print("No cache directory to delete")

    def _run(self, wdl: str, inputs=None, expected_exception: Exception = None, cfg=None):
        """
        run workflow/task & return outputs dict
        """
        logger = logging.getLogger(self.id())
        cfg = cfg or WDL.runtime.config.Loader(logger, [])
        try:
            with tempfile.NamedTemporaryFile(dir=self._dir, suffix=".wdl", delete=False) as outfile:
                outfile.write(wdl.encode("utf-8"))
                wdlfn = outfile.name
            doc = WDL.load(wdlfn)
            target = doc.workflow or doc.tasks[0]
            if isinstance(inputs, dict):
                inputs = WDL.values_from_json(inputs, target.available_inputs, target.required_inputs)
            rundir, outputs = WDL.runtime.run(cfg, target, (inputs or WDL.Env.Bindings()), run_dir=self._dir)

            self._rundir = rundir
        except Exception as exn:
            while isinstance(exn, WDL.runtime.RunFailed):
                exn = exn.__context__
            if expected_exception:
                self.assertIsInstance(exn, expected_exception)
                return exn
            raise
        self.assertIsNone(expected_exception, str(expected_exception) + " not raised")

        return rundir, outputs

    def test_input_digest_sorts_keys(self):
        # Note this fails if input array is reordered

        ordered_inputs = values_from_json(
            self.ordered_input_dict, self.doc.tasks[0].available_inputs)
        unordered_inputs = values_from_json(
            {
                "where": {"places": {"SanFan": "b", "Minneapolis": "a"}},
                "what": ["a", "ab", "b", "bc"],
                "who": "Alyssa"
            }, self.doc.tasks[0].available_inputs)

        ordered_digest = WDL.Value.digest_env(ordered_inputs)
        unordered_digest = WDL.Value.digest_env(unordered_inputs)
        self.assertEqual(ordered_digest, unordered_digest)

    def test_normalization(self):
        desc = self.doc.tasks[0]._digest_source()
        self.assertEqual(desc, R"""
version 1.0
task hello_blank {
input  {
String who
Array[String]? what
Map[String,Map[String,String]]? where
}
            command <<<
                # comment3
                echo "Hello, ~{who}!"
            >>>
output {
Int count = 12
}
}
        """.strip())

    def test_task_input_cache_matches_output(self):
        # run task, check output matches what was stored in run_dir
        cache = CallCache(cfg=self.cfg, logger=self.logger)
        rundir, outputs = self._run(self.test_wdl, self.ordered_input_dict, cfg=self.cfg)
        inputs = values_from_json(
            self.ordered_input_dict, self.doc.tasks[0].available_inputs)
        input_digest = WDL.Value.digest_env(inputs)
        task_digest = self.doc.tasks[0].digest
        with open(os.path.join(self.cache_dir, f"{self.doc.tasks[0].name}/{task_digest}/{input_digest}.json")) as f:
            read_data = json.loads(f.read())
        self.assertEqual(read_data, WDL.values_to_json(outputs))

    def test_cache_prevents_task_rerun(self):
        # run task twice, check _try_task not called for second run

        mock = MagicMock(side_effect=WDL.runtime.task._try_task)

        # test mock is called
        with patch('WDL.runtime.task._try_task', mock):
            self._run(self.test_wdl, self.ordered_input_dict, cfg=self.cfg)
        self.assertEqual(mock.call_count, 1)

        # call real _try_task function
        self._run(self.test_wdl, self.ordered_input_dict, cfg=self.cfg)

        # test mock is not called once cache is available
        new_mock = MagicMock(side_effect=WDL.runtime.task._try_task)

        with patch('WDL.runtime.task._try_task', new_mock):
            self._run(self.test_wdl, self.ordered_input_dict, cfg=self.cfg)

        self.assertEqual(new_mock.call_count, 0)

    def test_default_config_does_not_use_cache(self):
        # run task twice, check _try_task called for second run
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)

        # test mock is called
        with patch('WDL.runtime.task._try_task', mock):
            self._run(self.test_wdl, self.ordered_input_dict)
        self.assertEqual(mock.call_count, 1)

        # call real _try_task function
        self._run(self.test_wdl, self.ordered_input_dict)

        # test mock is not called once cache is available
        new_mock = MagicMock(side_effect=WDL.runtime.task._try_task)

        with patch('WDL.runtime.task._try_task', new_mock):
            self._run(self.test_wdl, self.ordered_input_dict)

        self.assertEqual(new_mock.call_count, 1)

    def test_get_cache_return_value_matches_outputs(self):
        cache = CallCache(cfg=self.cfg, logger=self.logger)
        rundir, outputs = self._run(self.test_wdl, self.ordered_input_dict, cfg=self.cfg)
        inputs = values_from_json(
            self.ordered_input_dict, self.doc.tasks[0].available_inputs)
        input_digest = WDL.Value.digest_env(inputs)
        task_digest = self.doc.tasks[0].digest
        cache_value = cache.get(key=f"{self.doc.tasks[0].name}/{task_digest}/{input_digest}",
                                output_types=self.doc.tasks[0].effective_outputs,
                                inputs=inputs)
        self.assertEqual(values_to_json(outputs), values_to_json(cache_value))

    def test_a_task_with_the_same_inputs_and_different_commands_doesnt_pull_from_the_cache(self):
        # run task twice, once with original wdl, once with updated wdl command, check _try_task  called for second run
        new_test_wdl: str = R"""
               version 1.0
               task hello_blank {
                   input {
                       String who
                       Array[String]? what
                       Map[String,Map[String,String]]? where
                   }
                   command <<<
                       echo "Heyyyyy, ~{who}!"
                   >>>
                   output {
                       Int count = 12
                   }
               }
               """

        #  _try_task function for original wdl
        self._run(self.test_wdl, self.ordered_input_dict, cfg=self.cfg)

        # test _try_task is called when task def changes (with same inputs)
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)

        with patch('WDL.runtime.task._try_task', mock):
            self._run(new_test_wdl, self.ordered_input_dict, cfg=self.cfg)

        self.assertEqual(mock.call_count, 1)

    def test_a_task_with_the_same_inputs_and_different_outputs_doesnt_pull_from_the_cache(self):
        # run task twice, once with original wdl, once with updated wdl command, check _try_task  called for second run
        new_test_wdl: str = R"""
                  version 1.0
                  task hello_blank {
                      input {
                          String who
                          Array[String]? what
                          Map[String,Map[String,String]]? where
                      }
                      command <<<
                          echo "Hello, ~{who}!"
                      >>>
                      output {
                          Int count = 13
                      }
                  }
                  """

        #  _try_task function for original wdl
        self._run(self.test_wdl, self.ordered_input_dict, cfg=self.cfg)

        # test _try_task is called when task def changes (with same inputs)
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)

        with patch('WDL.runtime.task._try_task', mock):
            self._run(new_test_wdl, self.ordered_input_dict, cfg=self.cfg)

        self.assertEqual(mock.call_count, 1)

    def test_struct_handling(self):
        with open(os.path.join(self._dir, "randomFile.txt"), "w") as outfile:
            outfile.write("Gotta put something here")
        inputs = {"box": {"str": [os.path.join(self._dir, "randomFile.txt")]}}
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)
        # test mock is called
        with patch('WDL.runtime.task._try_task', mock):
            self._run(self.struct_task, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 1)
        # run for real
        self._run(self.struct_task, inputs, cfg=self.cfg)

        new_mock = MagicMock(side_effect=WDL.runtime.task._try_task)

        # test mock not called for cached tasks containing a struct
        with patch('WDL.runtime.task._try_task', new_mock):
            self._run(self.struct_task, inputs, cfg=self.cfg)
        self.assertEqual(new_mock.call_count, 0)

    def test_cache_not_used_when_output_files_deleted(self):
        inputs = {"who": "Alyssa"}
        self._run(self.test_wdl_with_output_files, inputs, cfg=self.cfg)
        # test mock is not called once cache is available
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)

        with patch('WDL.runtime.task._try_task', mock):
            self._run(self.test_wdl_with_output_files, inputs, cfg=self.cfg)

        self.assertEqual(mock.call_count, 0)

        # delete files
        for x in glob.glob(f"{self._dir}/*_hello/out/a_tsv"):
            shutil.rmtree(x)

        # test mock is called now that cached file has been deleted
        with patch('WDL.runtime.task._try_task', mock):
            self._run(self.test_wdl_with_output_files, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 1)

    def test_cache_not_used_when_output_files_updated_after_cache_creation(self):
        inputs = {"who": "Bethie"}
        self._run(self.test_wdl_with_output_files, inputs, cfg=self.cfg)
        # change modified time on outputs
        time.sleep(0.1)
        for x in glob.glob(f"{self._dir}/*_hello/out/a_tsv/*"):
            os.utime(x)

        # check that mock is called now that output file is older than cache file
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)
        with patch('WDL.runtime.task._try_task', mock):

            self._run(self.test_wdl_with_output_files, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 1)

    def test_cache_not_used_when_output_files_but_not__sym_links_updated_after_cache_creation(self):
        inputs = {"who": "Bethie"}
        self._run(self.test_wdl_with_output_files, inputs, cfg=self.cfg)
        # change modified time on outputs
        time.sleep(0.1)
        for x in glob.glob(f"{self._dir}/*_hello/out/a_tsv/*"):
            os.utime(x, follow_symlinks=False)

        # check that mock is called now that output file is older than cache file
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)
        with patch('WDL.runtime.task._try_task', mock):
            self._run(self.test_wdl_with_output_files, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 1)

    def test_cache_not_used_when_file_in_array_recently_updated(self):
        filenames = ["file1", "file2", "file3", "butterfinger"]
        inputs = {"files": []}
        for fn in filenames:
            fn = os.path.join(self._dir, fn)
            with open(fn, "w") as outfile:
                print(fn, file=outfile)
            inputs["files"].append(fn)

        wdl = """
                version 1.0
                task return_file_array {
                    input {
                        Array[File] files
                    }
                    command <<<
                        set -euxo pipefail
                        mkdir files_out
                        find _miniwdl_inputs -type f -print0 | xargs -0 -iXXX cp XXX files_out/
                    >>>
                    output {
                        Array[File] files_out = glob("files_out/*")
                    }
                }
                """

        self._run(wdl, inputs, cfg=self.cfg)
        #check cache used
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)
        with patch('WDL.runtime.task._try_task', mock):
            self._run(wdl, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 0)
        # change time
        time.sleep(0.1)
        for x in glob.glob(f"{self._dir}/*_return_file_array/work/files_out/file1"):
            os.utime(x)
        # check cache not used
        with patch('WDL.runtime.task._try_task', mock):
            self._run(wdl, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 1)

    def test_cache_not_used_when_input_file_recently_updated(self):
        filenames = ["file1", "file2", "file3", "butterfinger"]
        inputs = {"files": []}
        for fn in filenames:
            fn = os.path.join(self._dir, fn)
            with open(fn, "w") as outfile:
                print(fn, file=outfile)
            inputs["files"].append(fn)

        wdl = """
                version 1.0
                task return_file_array {
                    input {
                        Array[File] files
                    }
                    command <<<
                        echo "Hello"
                    >>>
                    output {
                         Int count = 13
                    }
                }
                """

        self._run(wdl, inputs, cfg=self.cfg)
        #check cache used
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)
        with patch('WDL.runtime.task._try_task', mock):
            self._run(wdl, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 0)
        # change time on input file
        time.sleep(0.1)
        for x in glob.glob(f"{self._dir}/butterfinger"):
            os.utime(x)
        # check cache not used
        with patch('WDL.runtime.task._try_task', mock):
            self._run(wdl, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 1)

    def test_directory_coherence(self):
        # test outputting files/subdirectories inside input Directory
        wdl = R"""
        version development
        task t {
            input {
                Directory d
            }
            command {}
            output {
                Array[File] files = ["~{d}/alice.txt", "~{d}/sub/bob.txt"]
                Array[Directory] dirs = ["~{d}/sub/dir"]
            }
        }
        """
        os.makedirs(os.path.join(self._dir, "d/sub/dir"))
        with open(os.path.join(self._dir, "d/alice.txt"), mode="w") as outfile:
            print("Alice", file=outfile)
        with open(os.path.join(self._dir, "d/sub/bob.txt"), mode="w") as outfile:
            print("Bob", file=outfile)
        with open(os.path.join(self._dir, "d/sub/dir/carol.txt"), mode="w") as outfile:
            print("Carol", file=outfile)
        inp = {"d": os.path.join(self._dir, "d")}
        outp = self._run(wdl, inp, cfg=self.cfg)

        WDL.Value.rewrite_env_files(outp[1], lambda fn: fn)  # game coverage of deprecated fn

        mock = MagicMock(side_effect=WDL.runtime.task._try_task)
        with patch('WDL.runtime.task._try_task', mock):
            # control
            self._run(wdl, inp, cfg=self.cfg)
            self.assertEqual(mock.call_count, 0)

            # touch a file & check cache invalidated
            subprocess.run(["touch", os.path.join(self._dir, "d/sub/dir/carol.txt")], check=True)
            self._run(wdl, inp, cfg=self.cfg)
            self.assertEqual(mock.call_count, 1)

            # add a symlink
            time.sleep(0.1)
            os.symlink("sub/dir", os.path.join(self._dir, "d/link1"))
            self._run(wdl, inp, cfg=self.cfg)
            self.assertEqual(mock.call_count, 2)

            # delete the symlink
            time.sleep(0.1)
            os.unlink(os.path.join(self._dir, "d/link1"))
            self._run(wdl, inp, cfg=self.cfg)
            self.assertEqual(mock.call_count, 3)

            # control
            self._run(wdl, inp, cfg=self.cfg)
            self.assertEqual(mock.call_count, 3)

    test_workflow_wdl = R"""
    version development

    struct Person {
        String first
        String? middle
        String last
    }

    workflow multihello {
        input {
            Array[File] people_json
        }
        # COMMENT
        scatter (person_json in people_json) {
            call read_person {
                input:
                json = person_json
            }
            call hello {
                input:
                full_name = read_person.full_name
            }
        }

        output {
            Array[File] messages = hello.message
        }
    }

    task read_person {
        input {
            File json
        }

        Person person = read_json(json)

        command {}

        output {
            File full_name = write_lines([sep(" ", [person.first, person.last])])
        }
    }

    task hello {
        input {
            File full_name
            String? greeting = "Hello"
        }

        command <<<
            echo '~{greeting}, ~{read_string(full_name)}!'
        >>>

        output {
            File message = stdout()
        }
    }

    task uncalled {
        input {
            Int i = 0
            Person? p
        }
        command {}
    }
    """

    def test_workflow_digest(self):
        doc = WDL.parse_document(self.test_workflow_wdl)
        doc.typecheck()

        # ensure digest is sensitive to changes in the struct type and called task (but not the
        # uncalled task, or comments/whitespace)
        doc2 = WDL.parse_document(self.test_workflow_wdl.replace("String? middle", ""))
        doc2.typecheck()
        self.assertNotEqual(doc.workflow.digest, doc2.workflow.digest)

        doc2 = WDL.parse_document(self.test_workflow_wdl.replace('"Hello"', '"Hi"'))
        doc2.typecheck()
        self.assertNotEqual(doc.workflow.digest, doc2.workflow.digest)

        doc2 = WDL.parse_document(self.test_workflow_wdl.replace('i = 0', 'i = 1'))
        doc2.typecheck()
        self.assertEqual(doc.workflow.digest, doc2.workflow.digest)

        doc2 = WDL.parse_document(self.test_workflow_wdl.replace('# COMMENT', '#'))
        doc2.typecheck()
        self.assertEqual(doc.workflow.digest, doc2.workflow.digest)

        doc2 = WDL.parse_document(self.test_workflow_wdl.replace('# COMMENT', '\n\n'))
        doc2.typecheck()
        self.assertEqual(doc.workflow.digest, doc2.workflow.digest)

    def test_workflow_cache(self):
        with open(os.path.join(self._dir, "alyssa.json"), mode="w") as outfile:
            print('{"first":"Alyssa","last":"Hacker"}', file=outfile)
        with open(os.path.join(self._dir, "ben.json"), mode="w") as outfile:
            print('{"first":"Ben","last":"Bitdiddle"}', file=outfile)
        inp = {"people_json": [os.path.join(self._dir, "alyssa.json"), os.path.join(self._dir, "ben.json")]}
        rundir1, outp = self._run(self.test_workflow_wdl, inp, cfg=self.cfg)

        wmock = MagicMock(side_effect=WDL.runtime.workflow._workflow_main_loop)
        tmock = MagicMock(side_effect=WDL.runtime.task._try_task)
        with patch('WDL.runtime.workflow._workflow_main_loop', wmock), patch('WDL.runtime.task._try_task', tmock):
            # control
            rundir2, outp2 = self._run(self.test_workflow_wdl, inp, cfg=self.cfg)
            self.assertEqual(wmock.call_count, 0)
            self.assertEqual(tmock.call_count, 0)
            outp_inodes = set()
            WDL.Value.rewrite_env_paths(outp, lambda p: outp_inodes.add(os.stat(p.value)[stat.ST_INO]))
            outp2_inodes = set()
            WDL.Value.rewrite_env_paths(outp2, lambda p: outp2_inodes.add(os.stat(p.value)[stat.ST_INO]))
            self.assertEqual(outp_inodes, outp2_inodes)

            with open(os.path.join(rundir1, "outputs.json")) as outputs1:
                with open(os.path.join(rundir2, "outputs.json")) as outputs2:
                    assert outputs1.read() == outputs2.read()

            # touch a file & check cache invalidated
            with open(os.path.join(self._dir, "alyssa.json"), mode="w") as outfile:
                print('{"first":"Alyssa","last":"Hacker","middle":"P"}', file=outfile)
            _, outp2 = self._run(self.test_workflow_wdl, inp, cfg=self.cfg)
            self.assertEqual(wmock.call_count, 1)
            self.assertEqual(tmock.call_count, 2)  # reran Alyssa, cached Ben
            self.assertNotEqual(WDL.values_to_json(outp), WDL.values_to_json(outp2))
