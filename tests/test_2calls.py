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

    def test_optional(self):
        txt = tsk + r"""
        workflow contrived {
            Int? x
            call sum { input: x = x }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Int? x = 0
            call sum { input: x = x }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

    def test_nonempty(self):
        txt = r"""
        task p {
            Array[Int]+ x
            command <<<
                echo "~{sep=', ' x}"
            >>>
            output {
                String z = stdout()
            }
        }
        workflow contrived {
            Array[Int] x
            Array[Int]+ y = x
            call p { input: x=x }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()

        txt = r"""
        task p {
            Array[Int]+ x
            command <<<
                echo "~{sep=', ' x}"
            >>>
            output {
                String z = stdout()
            }
        }
        workflow contrived {
            Array[Int] x
            Array[Int]+ y = x
            call p { input: x=y }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = r"""
        workflow contrived {
            Array[Int] x = []
            Array[Int]+ y = [1]
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = r"""
        workflow contrived {
            Array[Int]+ y = []
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.EmptyArray):
            doc.typecheck()

    def test_collision(self):
        tasks = tsk + r"""
        task p {
            Array[Int]+ x
            command <<<
                echo "~{sep=', ' x}"
            >>>
            output {
                String z = stdout()
            }
        }
        """
        txt = tasks + r"""
        workflow contrived {
            call sum
            call sum as sum2
        }
        """
        WDL.parse_document(txt).typecheck()
        txt = tasks + r"""
        workflow contrived {
            call sum
            call sum
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            call sum
            call p as sum
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            call sum as foo
            call p as foo
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            if (true) {
                call sum as foo
            }
            scatter (i in [1,2]) {
                call p as foo
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

    def test_if_defined(self):
        # test special case for typechecking the construct
        #   if defined(x) then EXPR_WITH_x else SOME_DEFAULT
        # where we can treat x as non-optional when typechecking EXPR_WITH_x
        txt = r"""
        workflow contrived {
            Int? x
            Int y = if defined(x) then x+1 else 42
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = r"""
        workflow contrived {
            Int? x
            Int y = if true then x+1 else 42
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.IncompatibleOperand):
            doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Boolean b
            if (b) {
                call sum
            }
            Int y = if defined(sum.z) then sum.z+1 else 42
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Boolean b
            if (b) {
                call sum
            }
            call sum as s2
            Int y = if true then sum.z else s2.z
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
