import unittest
import logging
import tempfile
import os
import docker
import signal
import time
from .context import WDL

class TestWorkflowRunner(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_workflowrun_")

    def _test_workflow(self, wdl:str, inputs = None, expected_exception: Exception = None):
        try:
            doc = WDL.parse_document(wdl)
            assert doc.workflow
            doc.typecheck()
            if isinstance(inputs, dict):
                inputs = WDL.values_from_json(inputs, doc.workflow.available_inputs, doc.workflow.required_inputs)
            rundir, outputs = WDL.runtime.run_local_workflow(doc.workflow, (inputs or []), parent_dir=self._dir)
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
            self.assertFalse(str(expected_exception) + " not raised")
        return WDL.values_to_json(outputs)

    def test_hello(self):
        outputs = self._test_workflow("""
        version 1.0

        workflow hellowf {
            input {
                Int x
            }
            call hello as hello1 {
                input:
                    who = ["Alice", "Bob"],
                    x = x
            }
            call hello as hello2 {
                input:
                    who = ["Alyssa", "Ben"],
                    x = x
            }
            output {
                Array[String]+ messages = flatten([hello1.messages, hello2.messages])
                Array[Int]+ meanings = [hello1.meaning_of_life, hello2.meaning_of_life]
            }
        }

        task hello {
            input {
                Array[String]+ who
                Int x = 0
            }
            command <<<
                awk '{print "Hello", $0}' "~{write_lines(who)}"
            >>>
            output {
                Array[String]+ messages = read_lines(stdout())
                Int meaning_of_life = x+1
            }
        }
        """, {"x": 41})
        self.assertEqual(outputs["messages"], ["Hello Alice", "Hello Bob", "Hello Alyssa", "Hello Ben"])
        self.assertEqual(outputs["meanings"], [42, 42])
