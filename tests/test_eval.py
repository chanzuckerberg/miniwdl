import unittest
from typing import Optional
from .context import WDL

class TestEval(unittest.TestCase):

    def test_boolean(self):
        expr = WDL.parse_expr("true")
        self.assertIsInstance(expr.type, WDL.Type.Boolean)
        self.assertEqual(str(expr.type), "Boolean")
        env = WDL.Expr.Env()
        val = expr.eval(env)
        self.assertIsInstance(val, WDL.Value.Boolean)
        self.assertEqual(str(val.type), "Boolean")
        self.assertEqual(val.value, True)
        self.assertEqual(str(val), "true")
        self.assertEqual(val, WDL.Value.Boolean(True))
        self.assertNotEqual(val, WDL.Value.Boolean(False))

        expr = expr = WDL.parse_expr("false")
        self.assertEqual(str(expr.type), "Boolean")
        val = expr.eval(env)
        self.assertEqual(str(val.type), "Boolean")
        self.assertEqual(val.value, False)
        self.assertEqual(str(val), "false")
        self.assertEqual(val, WDL.Value.Boolean(False))
        self.assertNotEqual(val, WDL.Value.Boolean(True))

    def _t(self, expr : str, expected_value : str, env : WDL.Expr.Env = WDL.Expr.Env(),
           expected_type : Optional[WDL.Type.Base] = None):
        v = WDL.parse_expr(expr).eval(env).expect(expected_type)
        self.assertEqual(str(v), expected_value)

    def test_logic(self):
        self._t("true && true", "true", WDL.Type.Boolean())
        self._t("true && false", "false")
        self._t("false && true", "false")
        self._t("false && false", "false")
        self._t("true || true", "true")
        self._t("true || false", "true")
        self._t("false || true", "true")
        self._t("false || false", "false")
        self._t("false && true || true && true", "true")
        self._t("true && !false || false", "true")
        self._t("!true", "false")
        self._t("!!true", "true")
        self._t("!false", "true")
        self._t("!false && true", "true")
        self._t("!(false && true)", "true")

    def test_arithmetic(self):
        self._t("1", "1")
        self._t("-1","-1")
        self._t("0-1","-1")
        self._t("1+1", "2")
        self._t("2*3+4","10")
        self._t("2*(3+4)","14")
        self._t("2+3*4","14")
        self._t("1+6/3*4","9")
        self._t("1-4/3","0")
        self._t("1--4/3","3") # -4/3 == -2
        self._t("4%2","0")
        self._t("4%3","1")

    def test_cmp(self):
        self._t("1 == 1","true")
        self._t("1 == 0","false")
        self._t("1 != 1","false")
        self._t("1 != 0","true")
        self._t("1 < 1","false")
        self._t("1 <= 1","true")
        self._t("1<2","true")
        self._t("1<=2","true")
        self._t("1>1","false")
        self._t("1>=1","true")
        self._t("1>2","false")
        self._t("1>=0","true")

        self._t("3<2 || 1>=0","true")
        self._t("3<2&&1>=0","false")
        self._t("3<2&&1>=0||1==1","true")

    def test_if(self):
        self._t("if false then 0 else 1","1", WDL.Type.Int())
        self._t("if true then 0 else 1","0")
        self._t("if false then 0 else 1+2","3")
        self._t("(if false then 0 else 1)+2","3")
        self._t("(if 1>0 then 1+1 else 1)+1","3")
        self._t("if 1>0 then if true then 1 else 2 else 3","1")

    def test_array(self):
        expr = WDL.parse_expr("[true,false]")
        self.assertEqual(str(expr.type), "Array[Boolean]")

        env = WDL.Expr.Env()
        val = expr.eval(env)
        self.assertIsInstance(val, WDL.Value.Array)
        self.assertEqual(str(val.type), "Array[Boolean]")
        self.assertEqual(str(val), "[true, false]")

        self._t("[true, false][0]", "true")
        self._t("[true, false][1]", "false")

        self._t("[1+2, 3*4][1]", "12")

    def test_float_coercion(self):
        self._t("1 + 1.0", "2.0", WDL.Type.Float())
        self._t("1.0 + 1", "2.0", WDL.Type.Float())
        self._t("1 == 1.0", "true")
        self._t("1 == 1.1", "false")
        self._t("1 != 1.1", "true")
        self._t("1 < 1.0", "false")
        self._t("1 <= 1.0", "true")

        # TODO: test bad cases for appropriate errors
