import sys
import unittest
import pytest
import os
from tempfile import NamedTemporaryFile
import argcomplete
from .context import WDL

IFS = "\013"
COMP_WORDBREAKS = " \t\n\"'><=;|&(:"


class TestArgcomplete(unittest.TestCase):
    def setUp(self):
        self._os_environ = os.environ
        os.environ = os.environ.copy()
        os.environ["_ARGCOMPLETE"] = "1"
        os.environ["IFS"] = IFS
        os.environ["_ARGCOMPLETE_COMP_WORDBREAKS"] = COMP_WORDBREAKS
        # os.environ["_ARC_DEBUG"] = "yes"
        argcomplete.debug_stream = sys.stderr

    def tearDown(self):
        os.environ = self._os_environ

    def run_completer(
        self, parser, command, point=None, completer=argcomplete.autocomplete, **kwargs
    ):
        if point is None:
            point = str(len(command))
        with NamedTemporaryFile(mode="w") as t:
            os.environ["COMP_LINE"] = command
            os.environ["COMP_POINT"] = point
            with self.assertRaises(SystemExit) as cm:
                completer(parser, output_stream=t, exit_method=sys.exit, **kwargs)
            if cm.exception.code != 0:
                raise Exception("Unexpected exit code %d" % cm.exception.code)
            with open(t.name, "r") as tr:
                return tr.read().split(IFS)

    @pytest.mark.skip(reason="must run with unittest, not pytest, due to fd capture conflict")
    def test_completion(self):
        p = WDL.CLI.create_arg_parser()

        completions = self.run_completer(p, "miniwdl r")
        self.assertEqual(set(completions), {"run", "run_self_test", "run-self-test"})

        completions = self.run_completer(
            p,
            "miniwdl run --path test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows/tasks test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows/assemble_refbased.wdl ",
        )
        completions = set(completions)
        self.assertTrue("--json" in completions)
        self.assertTrue("refine_2x_and_plot.assembly_fasta=" in completions)

        # don't suggest optional inputs (can be overwhelming)
        self.assertTrue("refine_2x_and_plot.refine2_min_coverage=" not in completions)

        # suggest optional inputs only when specifically prefixed
        completions = self.run_completer(
            p,
            "miniwdl run --path test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows/tasks test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows/assemble_refbased.wdl refine_2x_and_plot.refine2_min_",
        )
        completions = set(completions)
        self.assertTrue("refine_2x_and_plot.assembly_fasta=" not in completions)
        self.assertTrue("refine_2x_and_plot.refine2_min_coverage=" in completions)
