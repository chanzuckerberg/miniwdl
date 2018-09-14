import unittest, inspect
from typing import Optional
from .context import WDL

class TestDoc(unittest.TestCase):
    def test_task(self):
        task_str = """
        task wc {
            input {
                String in
            }
            command {
                echo "~{in}" | wc
            }
            output {
                String ans = stdout()
            }
        }
        """
        task = WDL.parse_task(task_str)
        self.assertEqual(len(task.inputs), 1)
        self.assertEqual(str(task.inputs[0]), "String in")

        self.assertEqual(len(task.outputs), 1)
        self.assertEqual(str(task.outputs[0].type), "String")
        self.assertEqual(str(task.outputs[0].name), "ans")
        self.assertIsInstance(task.outputs[0].expr, WDL.Expr.Apply)

        task.typecheck()
