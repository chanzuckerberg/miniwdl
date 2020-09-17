import glob
import json
import logging
import os
import random
import shutil
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

from WDL import values_from_json, values_to_json
from WDL.runtime.cache import CallCache
from .context import WDL


class TestTaskRunner(unittest.TestCase):
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

        ordered_digest = CallCache(cfg=self.cfg, logger=self.logger).get_digest_for_inputs(ordered_inputs)
        unordered_digest = CallCache(cfg=self.cfg, logger=self.logger).get_digest_for_inputs(unordered_inputs)
        self.assertEqual(ordered_digest, unordered_digest)

    def test_normalization(self):
        desc = WDL.runtime.cache._describe_task(self.doc, self.doc.tasks[0])
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
        input_digest = cache.get_digest_for_inputs(inputs)
        task_digest = cache.get_digest_for_task(task=self.doc.tasks[0])
        with open(os.path.join(self.cache_dir, f"{self.doc.tasks[0].name}_{task_digest}/{input_digest}.json")) as f:
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
        input_digest = cache.get_digest_for_inputs(inputs)
        task_digest = cache.get_digest_for_task(task=self.doc.tasks[0])
        cache_value = cache.get(key=f"{self.doc.tasks[0].name}_{task_digest}/{input_digest}",
                                output_types=self.doc.tasks[0].effective_outputs)
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
        time.sleep(2)
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
        time.sleep(2)
        for x in glob.glob(f"{self._dir}/*_hello/out/a_tsv/*"):
            os.utime(x, follow_symlinks=False)

        # check that mock is called now that output file is older than cache file
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)
        with patch('WDL.runtime.task._try_task', mock):
            self._run(self.test_wdl_with_output_files, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 1)

    def test_cache_not_used_when_file_in_array_recently_updated(self):
        chars = [c for c in (chr(i) for i in range(1, 256)) if c not in ('/')]
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
        time.sleep(2)
        #check cache used
        mock = MagicMock(side_effect=WDL.runtime.task._try_task)
        with patch('WDL.runtime.task._try_task', mock):
            self._run(wdl, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 0)
        # change time
        for x in glob.glob(f"{self._dir}/*_return_file_array/work/files_out/file1"):
            os.utime(x)
        # check cache not used
        with patch('WDL.runtime.task._try_task', mock):
            self._run(wdl, inputs, cfg=self.cfg)
        self.assertEqual(mock.call_count, 1)
