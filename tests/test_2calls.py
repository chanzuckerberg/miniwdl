import unittest, inspect, os
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
        self.assertFalse(doc.workflow.complete_calls)

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
        self.assertFalse(doc.workflow.complete_calls)

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
        self.assertTrue(doc.workflow.complete_calls)

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
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = tsk + r"""
        workflow contrived {
            Int? x = 0
            call sum { input: x = x }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = tsk + r"""
        workflow contrived {
            Int? x = 0
            String? s = "foo"
            Pair[Int,String] p = (x,s)
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = tsk + r"""
        workflow contrived {
            Int? x = 0
            Array[Int] y = [x]
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        # TODO: test quant checking in Map & other composite types

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
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

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
        txt = tasks + r"""
        workflow contrived {
            Int i
            scatter (i in [1,2]) {
                call sum
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            scatter (i in [1,2]) {
                Int i = 42
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

    def test_if_defined(self):
        # test how we typecheck a construct like
        #   if defined(x) then EXPR_WITH_x else SOME_DEFAULT
        txt = r"""
        workflow contrived {
            Int? x
            Int y = x
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = r"""
        workflow contrived {
            Int? x
            Int y = if defined(x) then x+1 else 42
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.IncompatibleOperand):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = tsk + r"""
        workflow contrived {
            Boolean b
            if (b) {
                call sum
            }
            Int y = sum.z
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = tsk + r"""
        workflow contrived {
            Boolean b
            if (b) {
                call sum
            }
            call sum as s2
            Int y = if defined(sum.z) then sum.z+1 else s2.z
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.IncompatibleOperand):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)
        self.assertEqual(len(list(doc.workflow.effective_outputs)), 2)

    def test_forward_reference(self):
        txt = tsk + r"""
        workflow contrived {
            Int y = x
            Int x
            Array[Int?] w_out = w
            scatter (z in [1,2,3]) {
                if (true) {
                    Int w = z
                }
            }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Int y = z
            scatter (z in [1,2,3]) {
                call sum
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()
        txt = tsk + r"""
        workflow contrived {
            scatter (z in [1,2,3]) {
                call sum
            }
            Int y = z
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Array[Int] s = sum.z
            scatter (z in [1,2,3]) {
                call sum { input: x = s2.z }
            }
            call sum as s2
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Array[Int]+? s = sum.z
            if (2 == 2) {
                scatter (z in [1,2,3]) {
                    call sum { input: x = s2.z }
                }
            }
            call sum as s2
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()
        assert(doc.workflow.elements[0].type.nonempty and doc.workflow.elements[0].type.optional)

        # TODO: test cycle detection

    def test_uncallable_workflow(self):
        # should not be able to call a workflow containing an incomplete call
        WDL.load("file://" + os.path.join(os.path.dirname(__file__), "../test_corpi/contrived/incomplete_import.wdl"))
        with self.assertRaises(WDL.Error.UncallableWorkflow):
            WDL.load(os.path.join(os.path.dirname(__file__), "../test_corpi/contrived/incomplete_call.wdl"))
