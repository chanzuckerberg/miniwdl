import json
import logging
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from WDL import values_from_json, values_to_json
from WDL.runtime.cache import CallCache
from .context import WDL


class TestTaskRunner(unittest.TestCase):
    test_wdl: str = R"""
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
                Int count = 12
            }
        }
        """
    ordered_input_dict = {
        "what": ["a", "ab", "b", "bc"],
        "where": {"places": {"Minneapolis": "a", "SanFan": "b"}},
        "who": "Alyssa",
    }
    doc = WDL.parse_document(test_wdl)
    cache_dir = '/tmp/cache/'

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        cls.logger = logging.getLogger(cls.__name__)
        cls.cfg = WDL.runtime.config.Loader(cls.logger, [])
        cls.cfg.override({"call_cache": {"dir": cls.cache_dir}})
        WDL.runtime.task.SwarmContainer.global_init(cls.cfg, cls.logger)

    def setUp(self):
        """
        initialize docker & provision temporary directory for a test (self._dir)
        """
        self._dir = tempfile.mkdtemp(prefix=f"miniwdl_test_{self.id()}_")

    def tearDown(self):
        shutil.rmtree(self._dir)
        shutil.rmtree(self.cache_dir)

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

    def test_task_input_cache_matches_output(self):

        # run task, check output matches what was stored in run_dir
        rundir, outputs = self._run(self.test_wdl, self.ordered_input_dict, cfg=self.cfg)
        inputs = values_from_json(
            self.ordered_input_dict, self.doc.tasks[0].available_inputs)
        digest = CallCache(cfg=self.cfg, logger=self.logger).get_digest_for_inputs(inputs)
        with open(os.path.join(self.cache_dir, f"{digest}.json")) as f:
            read_data = json.loads(f.read())
        self.assertEqual(read_data, WDL.values_to_json(outputs))

    def test_cache_prevents_task_rerun(self):
        # run task twice, check _try_task not called not instantiated for second run

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

    def test_get_cache_return_value_matches_outputs(self):
        rundir, outputs = self._run(self.test_wdl, self.ordered_input_dict, cfg=self.cfg)
        inputs = values_from_json(
            self.ordered_input_dict, self.doc.tasks[0].available_inputs)
        digest = CallCache(cfg=self.cfg, logger=self.logger).get_digest_for_inputs(inputs)

        cache = CallCache(cfg=self.cfg, logger=self.logger).get(key=digest, run_dir=rundir,
                                                                output_types=self.doc.tasks[0].effective_outputs)
        self.assertEqual(values_to_json(outputs), values_to_json(cache))

