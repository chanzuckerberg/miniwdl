import unittest
import logging
import tempfile
import os
import shutil
import docker
from testfixtures import log_capture
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
        WDL.runtime.task.SwarmContainer.global_init(cfg, logger)

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
    count_wdl: str = R"""
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

    def test_download_input_files(self):
        self._run(self.count_wdl, {"files": [
            "gs://gcp-public-data-landsat/LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2/LC08_L1GT_044034_20130330_20170310_01_T2_MTL.txt",
            "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/tests/alyssa_ben.txt"
        ]})
        self._run(self.count_wdl, {"files": [
            "gs://gcp-public-data-landsat/LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2/LC08_L1GT_044034_20130330_20170310_01_T2_MTL.txt",
            "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/nonexistent12345.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/tests/alyssa_ben.txt"
        ]}, expected_exception=WDL.runtime.DownloadFailed)
        self._run(self.count_wdl, {"files": ["gs://8675309"]}, expected_exception=WDL.runtime.DownloadFailed)

    @log_capture()
    def test_download_cache1(self, capture):
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()))
        cfg.override({
            "download_cache": {
                "put": True,
                "get": True,
                "dir": os.path.join(self._dir, "cache"),
                "disable_patterns": ["https://google.com/*"]
            }
        })
        inp = {"files": ["https://google.com/robots.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/tests/alyssa_ben.txt"]}
        self._run(self.count_wdl, inp, cfg=cfg)
        self._run(self.count_wdl, inp, cfg=cfg)
        logs = [str(record.msg) for record in capture.records if str(record.msg).startswith("downloaded input files")]
        self.assertTrue("downloaded: 2" in logs[0])
        # alyssa_ben.txt is cached on second run through (robots.txt not due to disable_patterns)
        self.assertTrue("downloaded: 1" in logs[1])
        self.assertTrue("cached: 1" in logs[1])

    @log_capture()
    def test_download_cache2(self, capture):
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()))
        cfg.override({
            "download_cache": {
                "put": True,
                "get": True,
                "dir": os.path.join(self._dir, "cache2"),
                "enable_patterns": ["https://raw.githubusercontent.com/chanzuckerberg/*"]
            }
        })
        inp = {"files": ["https://google.com/robots.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/tests/alyssa_ben.txt"]}
        self._run(self.count_wdl, inp, cfg=cfg)
        self._run(self.count_wdl, inp, cfg=cfg)
        logs = [str(record.msg) for record in capture.records if str(record.msg).startswith("downloaded input files")]
        self.assertTrue("downloaded: 2" in logs[0])
        # alyssa_ben.txt is cached on second run through
        self.assertTrue("downloaded: 1" in logs[1])
        self.assertTrue("cached: 1" in logs[1])

    @log_capture()
    def test_download_cache3(self, capture):
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()))
        cfg.override({
            "download_cache": {
                "put": True,
                "get": True,
                "dir": os.path.join(self._dir, "cache"),
            }
        })
        inp = {"files": ["https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/tests/alyssa_ben.txt?xxx"]}
        self._run(self.count_wdl, inp, cfg=cfg)
        self._run(self.count_wdl, inp, cfg=cfg)
        logs = [str(record.msg) for record in capture.records if str(record.msg).startswith("downloaded input files")]
        # cache isn't used due to presence of query string
        self.assertTrue("downloaded: 1" in logs[0])
        self.assertTrue("downloaded: 1" in logs[1])

    @log_capture()
    def test_download_cache4(self, capture):
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()))
        cfg.override({
            "download_cache": {
                "put": True,
                "get": True,
                "dir": os.path.join(self._dir, "cache4"),
                "ignore_query": True
            }
        })
        inp = {"files": ["https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/tests/alyssa_ben.txt?xxx"]}
        self._run(self.count_wdl, inp, cfg=cfg)
        self._run(self.count_wdl, inp, cfg=cfg)
        logs = [str(record.msg) for record in capture.records if str(record.msg).startswith("downloaded input files")]
        # cache used with ignore_query
        self.assertTrue("downloaded: 1" in logs[0])
        self.assertTrue("downloaded: 0" in logs[1])
