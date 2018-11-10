import unittest, inspect
from typing import Optional
from .context import WDL

tsk = r"""
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
            }
        }
"""

class TestCalls(unittest.TestCase):

    def test_missing_input(self):
        txt = tsk + r"""
        workflow contrived {
            call sum
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        doc = WDL._parser.parse_document(txt, imported=True)
        with self.assertRaises(WDL.Error.MissingInput):
            doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Int x
            call sum { input:
                x = x
            }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        doc = WDL._parser.parse_document(txt, imported=True)
        with self.assertRaises(WDL.Error.MissingInput):
            doc.typecheck()


        txt = tsk + r"""
        workflow contrived {
            Int w
            Int z
            call sum { input:
                x = w,
                y = z
            }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        doc = WDL._parser.parse_document(txt, imported=True)
        doc.typecheck()

    def test_duplicate_input(self):
        txt = tsk + r"""
        workflow contrived {
            Int x
            call sum { input:
                x = x,
                x = x
            }
        }
        """
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc = WDL.parse_document(txt)
