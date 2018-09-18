import unittest, inspect
from typing import Optional
from .context import WDL

class TestDoc(unittest.TestCase):
    def test_wc(self):
        variants = [
        """
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
        """,
        """
        task wc {
            input {
                String in
            }
            command {
                echo "${in}" | wc
                echo "$USER" > /dev/null
                echo "$(env)" >> /dev/null
            }
            output {
                String ans = stdout()
            }
        }
        """,
        """
        task wc {
            input {
                String in
            }
            String d = in + "_foo"
            command <<<
                echo "~{in}" | wc
                echo "$USER" > /dev/null
                echo "$(env)" >> /dev/null
                echo "${HOME}" >> /dev/null
            >>>
            output {
                String ans = stdout()
            }
        }
        """]
        for task_str in variants:
            task = WDL.parse_task(task_str)
            self.assertEqual(len(task.inputs), 1)
            self.assertEqual(str(task.inputs[0]), "String in")

            self.assertEqual(len(task.command.parts), 3)
            self.assertEqual(task.command.parts[1].expr.identifier, "in")

            self.assertEqual(len(task.outputs), 1)
            self.assertEqual(str(task.outputs[0].type), "String")
            self.assertEqual(str(task.outputs[0].name), "ans")
            self.assertIsInstance(task.outputs[0].expr, WDL.Expr.Apply)

            task.typecheck()

            self.assertEqual(task.command.parts[1].eval(WDL.Expr.Env(('in', WDL.Value.String("hello")))).value, 'hello')

    def test_errors(self):
        with self.assertRaises(WDL.Error.UnknownIdentifier, msg="Unknown identifier bogus"):
            WDL.parse_task("""
            task wc {
                input {
                    String in
                }
                command {
                    echo "~{bogus}" | wc
                }
            }
            """).typecheck()
        with self.assertRaises(WDL.Error.UnknownIdentifier, msg="Unknown identifier bogus"):
            WDL.parse_task("""
            task wc {
                input {
                    String in
                }
                command {
                    echo "~{in}" | wc
                }
                output {
                    String ans = "${bogus}"
                }
            }
            """).typecheck()

    def test_placeholders(self):
        task = WDL.parse_task("""
            task wc {
                input {
                    Boolean b
                }
                command <<<
                    echo "~{true='yes' false='no' b}"
                >>>
            }
            """)
        task.typecheck()
        self.assertEqual(task.command.parts[1].eval(WDL.Expr.Env(('b', WDL.Value.Boolean(True)))).value, 'yes')
        self.assertEqual(task.command.parts[1].eval(WDL.Expr.Env(('b', WDL.Value.Boolean(False)))).value, 'no')

        task = WDL.parse_task("""
            task wc {
                input {
                    Boolean b
                }
                command {
                    echo "${if b then 'yes' else 'no'}"
                }
            }
            """)
        task.typecheck()
        self.assertEqual(task.command.parts[1].eval(WDL.Expr.Env(('b', WDL.Value.Boolean(True)))).value, 'yes')
        self.assertEqual(task.command.parts[1].eval(WDL.Expr.Env(('b', WDL.Value.Boolean(False)))).value, 'no')

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_task("""
                task wc {
                    input {
                        Int b
                    }
                    command {
                        echo "~{true='yes' false='no' b}"
                    }
                }
                """).typecheck()

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_task("""
                task wc {
                    command {
                        echo "~{true='yes' false='no' 42}"
                    }
                }
                """).typecheck()

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_task("""
                task wc {
                    input {
                        Boolean b
                    }
                    command {
                        echo "~{false='no' b}"
                    }
                }
                """).typecheck()

        task = WDL.parse_task("""
            task wc {
                input {
                    Array[String] s
                }
                command <<<
                    echo "~{sep=', ' s} baz"
                >>>
            }
            """)
        task.typecheck()
        foobar = WDL.Value.Array(WDL.Type.Array(WDL.Type.String()), [WDL.Value.String("foo"), WDL.Value.String("bar")])
        self.assertEqual(task.command.parts[1].eval(WDL.Expr.Env(('s', foobar))).value, 'foo, bar')
        foobar = WDL.Value.Array(WDL.Type.Array(WDL.Type.String()), [])
        self.assertEqual(task.command.parts[1].eval(WDL.Expr.Env(('s', foobar))).value, '')
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            task = WDL.parse_task("""
            task wc {
                input {
                    Array[String] s
                }
                command <<<
                    echo "~{s} baz"
                >>>
            }
            """).typecheck()
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_task("""
            task wc {
                input {
                    String s
                }
                command <<<
                    echo "~{sep=', ' s} baz"
                >>>
            }
            """).typecheck()

    def test_meta(self):
        task = WDL.parse_task("""
        task wc {
            input {
                Boolean b
                Int n
            }
            parameter_meta {
                b: { help: "it's a boolean" }
                n: 'x'
            }
            command {
                echo "~{true='yes' false='no' b}"
            }
            runtime {
                memory: "1 GB"
                cpu: 42
            }
        }
        """)
        task.typecheck()
        self.assertEqual(task.parameter_meta['b']['help'], "it's a boolean")
        self.assertEqual(task.runtime['cpu'], 42)
