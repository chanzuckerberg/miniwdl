import unittest
import logging
import tempfile
import os
import shutil
import docker
from .context import WDL


class RunnerTestCase(unittest.TestCase):
    """
    Base class for new runner test cases
    """

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        logger = logging.getLogger(cls.__name__)
        cfg = WDL.runtime.config.Loader(logger, [])
        WDL.runtime.task.LocalSwarmContainer.global_init(cfg, logger)

    def setUp(self):
        """
        initialize docker & provision temporary directory for a test (self._dir)
        """
        self._dir = tempfile.mkdtemp(prefix=f"miniwdl_test_{self.id()}_")

    def tearDown(self):
        shutil.rmtree(self._dir)

    def _run(self, wdl:str, inputs = None, expected_exception: Exception = None, cfg = None):
        """
        run workflow/task & return outputs dict
        """
        logger = logging.getLogger(self.id())
        WDL._util.install_coloredlogs(logger)
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
        except Exception as exn:
            while isinstance(exn, WDL.runtime.RunFailed):
                exn = exn.__context__
            if expected_exception:
                self.assertIsInstance(exn, expected_exception)
                return exn
            raise
        self.assertIsNone(expected_exception, str(expected_exception) + " not raised")
        return WDL.values_to_json(outputs)

class TestDownload(RunnerTestCase):

    def test_download_input_files(self):
        count = R"""
        version 1.0
        workflow count {
            input {
                Array[File] files
            }
            scatter (file in files) {
                Array[String] file_lines = read_lines(file)
            }
            output {
                Int lines = length(flatten(file_lines))
            }
        }
        """
        self._run(count, {"files": ["https://google.com/robots.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/tests/alyssa_ben.txt"]})
        self._run(count, {"files": ["https://google.com/robots.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/nonexistent12345.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/tests/alyssa_ben.txt"]},
                  expected_exception=WDL.runtime.DownloadFailed)
