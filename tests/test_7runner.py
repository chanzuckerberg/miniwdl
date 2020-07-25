import unittest
import logging
import tempfile
import random
import os
import shutil
import json
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
            "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/tests/alyssa_ben.txt"
        ]})
        self._run(self.count_wdl, {"files": [
            "gs://gcp-public-data-landsat/LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2/LC08_L1GT_044034_20130330_20170310_01_T2_MTL.txt",
            "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/nonexistent12345.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/tests/alyssa_ben.txt"
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
        inp = {"files": ["https://google.com/robots.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/tests/alyssa_ben.txt"]}
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
        inp = {"files": ["https://google.com/robots.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/tests/alyssa_ben.txt"]}
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
        inp = {"files": ["s3://1000genomes/CHANGELOG", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/tests/alyssa_ben.txt?xxx"]}
        self._run(self.count_wdl, inp, cfg=cfg)
        self._run(self.count_wdl, inp, cfg=cfg)
        logs = [str(record.msg) for record in capture.records if str(record.msg).startswith("downloaded input files")]
        # cache isn't used for alyssa_ben.txt due to presence of query string
        self.assertTrue("downloaded: 2" in logs[0])
        self.assertTrue("downloaded: 1" in logs[1])
        assert next(record for record in capture.records if "AWS credentials" in str(record.msg))

    def test_download_cache4(self):
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()))
        cfg.override({
            "download_cache": {
                "put": True,
                "get": True,
                "dir": os.path.join(self._dir, "cache4"),
                "ignore_query": True
            },
            # test JSON logging:
            "logging": { "json": True }
        })
        inp = {"files": ["https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/tests/alyssa_ben.txt?xxx"]}
        self._run(self.count_wdl, inp, cfg=cfg)
        with open(os.path.join(self._rundir, "workflow.log")) as logfile:
            for line in logfile:
                line = json.loads(line)
                if "downloaded input files" in line["message"]:
                    self.assertEqual(line["downloaded"], 1)
        self._run(self.count_wdl, inp, cfg=cfg)
        # cache used with ignore_query
        with open(os.path.join(self._rundir, "workflow.log")) as logfile:
            for line in logfile:
                line = json.loads(line)
                if "downloaded input files" in line["message"]:
                    self.assertEqual(line["downloaded"], 0)

class MiscRegressionTests(RunnerTestCase):
    def test_repeated_file_rewriting(self):
        wdl = """
        version 1.0
        task t {
            input {
                Array[File] files
            }
            command <<<
                xargs cat < ~{write_lines(files)}
                echo Bob > bob.txt
            >>>
            output {
                Array[String] out = read_lines(stdout())
                File bob = "bob.txt"
                Array[File] bob2 = [bob, bob]
            }
        }
        workflow w {
            input {
                File file
            }
            call t {
                input:
                files = [file, file]
            }
        }
        """
        with open(os.path.join(self._dir, "alice.txt"), "w") as alice:
            print("Alice", file=alice)
        outp = self._run(wdl, {"file": os.path.join(self._dir, "alice.txt")})
        self.assertEqual(outp["t.out"], ["Alice", "Alice"])

    def test_weird_filenames(self):
        chars = [c for c in (chr(i) for i in range(1,256)) if c not in ('/')]
        filenames = []
        for c in chars:
            if c != '.':
                filenames.append(c)
            filenames.append(c + ''.join(random.choices(chars,k=11)))
        assert filenames == list(sorted(filenames))
        filenames.append('ThisIs{{AVeryLongFilename }}abc...}}xzy1234567890!@{{నేనుÆды.test.ext')

        inputs = {"files": []}
        for fn in filenames:
            fn = os.path.join(self._dir, fn)
            with open(fn, "w") as outfile:
                print(fn, file=outfile)
            inputs["files"].append(fn)

        wdl = """
        version 1.0
        workflow w {
            input {
                Array[File] files
            }
            call t {
                input:
                files = files
            }
            output {
                Array[File] files_out = t.files_out
            }
        }

        task t {
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

        outp = self._run(wdl, inputs)
        outp_filenames = list(sorted(os.path.basename(fn) for fn in outp["files_out"]))
        # glob will exclude dotfiles
        expctd_filenames = list(bn for bn in sorted(os.path.basename(fn) for fn in inputs["files"]) if not bn.startswith("."))
        self.assertEqual(outp_filenames, expctd_filenames)
        euid = os.geteuid()
        for fn in outp["files_out"]:
            assert os.stat(fn).st_uid == euid
