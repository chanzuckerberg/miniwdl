import unittest, inspect, json
from .context import WDL

class TestEval(unittest.TestCase):

    def test_boolean(self):
        expr = WDL.parse_expr("true")
        expr.infer_type([])
        self.assertIsInstance(expr.type, WDL.Type.Boolean)
        self.assertEqual(str(expr.type), "Boolean")
        val = expr.eval([])
        self.assertIsInstance(val, WDL.Value.Boolean)
        self.assertEqual(str(val.type), "Boolean")
        self.assertEqual(val.value, True)
        self.assertEqual(str(val), "true")
        self.assertEqual(val, WDL.Value.Boolean(True))
        self.assertNotEqual(val, WDL.Value.Boolean(False))

        expr = WDL.parse_expr("false")
        expr.infer_type([])
        self.assertEqual(str(expr.type), "Boolean")
        val = expr.eval([])
        self.assertEqual(str(val.type), "Boolean")
        self.assertEqual(val.value, False)
        self.assertEqual(str(val), "false")
        self.assertEqual(val, WDL.Value.Boolean(False))
        self.assertNotEqual(val, WDL.Value.Boolean(True))

    def _test_tuples(self, *tuples):
        for tuple in tuples:
            assert(len(tuple) >= 2)
            expr = tuple[0]
            expected = tuple[1]
            env = None
            expected_type = None
            exn = None
            version = None
            for x in tuple[2:]:
                if isinstance(x, list):
                    env = x
                elif isinstance(x, WDL.Type.Base):
                    expected_type = x
                elif isinstance(x, str):
                    version = x
                elif inspect.isclass(x):
                    exn = x
                else:
                    assert False
            type_env = []
            if env is not None:
                for node in env:
                    if isinstance(node, WDL.Env.Binding):
                        type_env = WDL.Env.bind(type_env, [], node.name, node.rhs.type)
            if exn:
                with self.assertRaises(exn, msg=expected):
                    x = WDL.parse_expr(expr, version=version).infer_type(type_env).eval(env)
            else:
                try:
                    v = WDL.parse_expr(expr, version=version).infer_type(type_env).eval(env).expect(expected_type)
                    self.assertEqual(str(v), expected)
                except:
                    assert False, str(expr)

    def test_logic(self):
        self._test_tuples(
            ("true && true", "true", WDL.Type.Boolean()),
            ("true && false", "false"),
            ("false && true", "false"),
            ("false && false", "false"),
            ("true || true", "true"),
            ("true || false", "true"),
            ("false || true", "true"),
            ("false || false", "false"),
            ("false && true || true && true", "true"),
            ("true && !false || false", "true"),
            ("!true", "false"),
            ("!!true", "true"),
            ("!false", "true"),
            ("!false && true", "true"),
            ("!(false && true)", "true")
        )

    def test_arithmetic(self):
        self._test_tuples(
            ("1", "1"),
            ("-1","-1"),
            ("0-1","-1"),
            ("1+1", "2"),
            ("2*3+4","10"),
            ("2*(3+4)","14"),
            ("2+3*4","14"),
            ("1+6/3*4","9"),
            ("1-4/3","0"),
            ("1--4/3","3"), # -4/3 == -2, is this right?
            ("4%2","0"),
            ("4%3","1"),
            ("1 + false", "(Ln 1, Col 1) Non-numeric operand to + operator", WDL.Error.IncompatibleOperand)
        )

    def test_cmp(self):
        self._test_tuples(
            ("1 == 1","true"),
            ("1 == 0","false"),
            ("1 != 1","false"),
            ("1 != 0","true"),
            ("1 < 1","false"),
            ("1 <= 1","true"),
            ("1<2","true"),
            ("1<=2","true"),
            ("1>1","false"),
            ("1>=1","true"),
            ("1>2","false"),
            ("1>=0","true"),
            ("3<2 || 1>=0","true"),
            ("3<2&&1>=0","false"),
            ("3<2&&1>=0||1==1","true"),
            ("1 == false", "(Ln 1, Col 1) Cannot compare Int and Boolean", WDL.Error.IncompatibleOperand)
        )

    def test_str(self):
        self._test_tuples(
            ('"true"', '"true"', WDL.Type.String()),
            ('"true" == "true"', 'true', WDL.Type.Boolean()),
            ('"true" != "true"', 'false', WDL.Type.Boolean()),
            ('"true" == "foo"', 'false', WDL.Type.Boolean()),
            ('"true" != "bar"', 'true', WDL.Type.Boolean()),
            ('"foo" + "bar"', '"foobar"'),
            ('"foo" + 1', '"foo1"'),
            ('2.0 + "bar"', '"2.0bar"'),
            (""" 'foo' + "bar" """, '"foobar"'),
            ('"{"', '"{"', WDL.Type.String()),
            ('"$" + "$"', '"$$"', WDL.Type.String()))
        self._test_tuples(
            (r'''"CNN is working frantically to find their \"source.\""''',
             r'''"CNN is working frantically to find their \"source.\""'''),
            (r"""'CNN is working frantically to find their "source."'""",
             r'''"CNN is working frantically to find their \"source.\""'''),
            (r"""'The fact is that many anonymous sources don\'t even exist.'""",
             r'''"The fact is that many anonymous sources don't even exist."''')
        )

    def test_if(self):
        self._test_tuples(
            ("if false then 0 else 1","1", WDL.Type.Int()),
            ("if true then 0 else 1","0"),
            ("if false then 0 else 1+2","3"),
            ("(if false then 0 else 1)+2","3"),
            ("(if 1>0 then 1+1 else 1)+1","3"),
            ("if 1>0 then if true then 1 else 2 else 3","1"),            
            ("if 3.14 then 0 else 1", "(Ln 1, Col 1) Expected Boolean instead of Float; in if condition", WDL.Error.StaticTypeMismatch),
            ("if 0 < 1 then 0 else false", "(Ln 1, Col 1) Expected Int instead of Boolean; if consequent & alternative must have the same type", WDL.Error.StaticTypeMismatch),
            ("if true then 1 else 2.0", "1.0", WDL.Type.Float()),
            ("if false then 1 else 2.0", "2.0", WDL.Type.Float()),
            ("if true then 1.0 else 2", "1.0", WDL.Type.Float()),
            ("if false then 1.0 else 2", "2.0", WDL.Type.Float())
        )

    def test_array(self):
        expr = WDL.parse_expr("[true,false]")
        expr.infer_type([])
        self.assertEqual(str(expr.type), "Array[Boolean]+")

        env = []
        val = expr.eval(env)
        self.assertIsInstance(val, WDL.Value.Array)
        self.assertEqual(str(val.type), "Array[Boolean]+")
        self.assertEqual(str(val), "[true, false]")

        self._test_tuples(
            ("[true, false][0]", "true"),
            ("[true, false][1]", "false"),
            ("[1+2, 3*4][1]", "12"),
            ("[1,2,3,]", "[1, 2, 3]"),
            ("[1,'a']", '["1", "a"]'),
            ("[]","[]", WDL.Type.Array(WDL.Type.Any())),
            ("[] == []","true"),
            ("[1, false]", '["1", "false"]', WDL.Type.Array(WDL.Type.String(), nonempty=True)),
            ("[1, {}]", "(Ln 1, Col 1) Expected Int instead of Boolean; inconsistent types within array", WDL.Error.StaticTypeMismatch),
            ("1 + 2[3]", "(Ln 1, Col 5) Not an array", WDL.Error.NotAnArray),
            ("[1, 2, 3][true]", "(Ln 1, Col 11) Expected Int instead of Boolean; Array index", WDL.Error.StaticTypeMismatch),
            ("[1, 2, 3][4]", "(Ln 1, Col 11) Array index out of bounds", WDL.Error.OutOfBounds)
        )

    def test_float_coercion(self):
        self._test_tuples(
            ("1 + 1.0", "2.0", WDL.Type.Float()),
            ("1.0 + 1", "2.0", WDL.Type.Float()),
            ("1 == 1.0", "true"),
            ("1 == 1.1", "false"),
            ("1 != 1.1", "true"),
            ("1 < 1.0", "false"),
            ("1 <= 1.0", "true"),
            ("[1, 2.0]", "[1.0, 2.0]", WDL.Type.Array(WDL.Type.Float())),
            ("[1, 2.0][0]", "1.0", WDL.Type.Float()),
            # TODO: more sophisticated unification algo to handle this
            # ("[[1],[2.0]]", "[[1.0], [2.0]]", WDL.Type.Array(WDL.Type.Float())),
        )

    def test_ident(self):
        env = cons_env(("pi", WDL.Value.Float(3.14159)), ("e", WDL.Value.Float(2.71828)),
                        ("t", WDL.Value.Boolean(True)), ("f", WDL.Value.Boolean(False)),
                        ("true_rep_only", WDL.Value.Boolean(False)),
                        ("lefty", WDL.Value.Boolean(False)),
                        ("left_recursive", WDL.Value.Boolean(False)))
        self._test_tuples(
            ("pi", "3.14159", WDL.Type.Float(), env),
            ("bogus", "(Ln 1, Col 1) Unknown identifier", WDL.Error.UnknownIdentifier, env),
            ("pi+e", "5.85987", env),
            ("t||f", "true", WDL.Type.Boolean(), env),
            ("if t then pi else e", "3.14159", env),
            ("true_rep_only", "false", env),
            ("lefty", "false", env),
            ("left_recursive", "false", env)
        )


    def test_interpolation(self):
        env = cons_env(("pi", WDL.Value.Float(3.14159)), ("e", WDL.Value.Float(2.71828)),
                        ("t", WDL.Value.Boolean(True)), ("f", WDL.Value.Boolean(False)),
                        ("s", WDL.Value.String("foo")))
        self._test_tuples(
            ('"${pi}"', '"3.14159"', env),
            ('"pi = ${pi}!"', '"pi = 3.14159!"', env),
            ('"pi+e = ${pi+e}!"', '"pi+e = 5.85987!"', env),
            ("'This is ${t}'", '"This is true"', env),
            ("'${f} is ${f}'", '"false is false"', env),
            ("'${s}bar'", '"foobar"', env),
            ('"$"','"$"'),
            ('"$shell"','"$shell"'),
            ("'c$'",'"c$"'),
            ("'The U.$. is re$pected again!'",'"The U.$. is re$pected again!"')
        )
        self._test_tuples(
            ('"${pi} ~{pi}$"', '"3.14159 ~{pi}$"', env, "draft-2"),
            ("'${pi} ~{pi}$'", '"3.14159 ~{pi}$"', env, "draft-2"),
            ('"${pi} ~{pi}$"', '"3.14159 3.14159$"', env, "1.0"),
            ("'${pi} ~{pi}~'", '"3.14159 3.14159~"', env, "1.0"),
            ("'$${pi}$'", '"$3.14159$"', env, "draft-2"),
            ('"$${pi}$$"', '"$3.14159$$"', env, "draft-2"),
            ("'$${pi}$'", '"$3.14159$"', env, "1.0"),
            ("'$${pi}$$'", '"$3.14159$$"', env, "1.0"),
            ("'$$${pi}~'", '"$$3.14159~"', env, "1.0"),
            ("'~~{pi}~'", '"~3.14159~"', env, "1.0"),
            ('"~~{pi}~"', '"~3.14159~"', env, "1.0"),
            ("'~~${pi}~'", '"~~3.14159~"', env, "1.0"),
            ("'$~{pi}~~'", '"$3.14159~~"', env, "1.0"),
            ("'$~${pi}~~'", '"$~3.14159~~"', env, "1.0"),
        )

    def test_pair(self):
        env = cons_env(("p", WDL.Value.Pair(WDL.Type.Pair(WDL.Type.Float(), WDL.Type.Float()),
                                            (WDL.Value.Float(3.14159), WDL.Value.Float(2.71828)))),
                        ("q", WDL.Value.Pair(WDL.Type.Pair(WDL.Type.Pair(WDL.Type.Int(), WDL.Type.Int()),
                                                           WDL.Type.Float(optional=True)),
                                             (WDL.Value.Pair(WDL.Type.Pair(WDL.Type.Int(), WDL.Type.Int()),
                                                             (WDL.Value.Int(4), WDL.Value.Int(2))),
                                              WDL.Value.Null()))))
        self._test_tuples(
            ("(1,2)", "(1,2)", WDL.Type.Pair(WDL.Type.Int(), WDL.Type.Int())),
            ("(1,2).left", "1"),
            ("(1,false).right", "false"),
            ("(false,[1,2]).right[1]", "2"),
            ("[1,2].left", "", WDL.Error.NoSuchMember),
            ("false.right", "", WDL.Error.NoSuchMember),
            ("p.left", "3.14159", env),
            ("p.right", "2.71828", env),
            ("q.left.left", "4", env),
            ("q.left.right", "2", env)
        )

    def test_map(self):
        self._test_tuples(
            ("{'foo': 1, 'bar': 2}['bar']", "2"),
            ("{0: 1, 2: 3}['foo']", "", WDL.Error.StaticTypeMismatch),
            ("{'foo': 1, 'bar': 2}[3]", "", WDL.Error.OutOfBounds), # int coerces to string...
            ("{3: 1, false: 2}", "", WDL.Error.StaticTypeMismatch),
            ("{'foo': true, 'bar': 0}", "", WDL.Error.StaticTypeMismatch)
        )

    def test_errors(self):
        self._test_tuples(
            ("1 + bogus(2)", "(Ln 1, Col 5) No such function: bogus", WDL.Error.NoSuchFunction)
        )

    def test_short_circuit(self):
        self._test_tuples(
            ("true && 1/0 == 1", "", WDL.Error.IncompatibleOperand),
            ("false && 1/0 == 1", "false"),
            ("false || 1/0 == 1", "", WDL.Error.IncompatibleOperand),
            ("true || 1/0 == 1", "true"),
        )

def cons_env(*bindings):
    return [WDL.Env.Binding(x,y) for (x,y) in bindings]

class TestEnv(unittest.TestCase):
    """
    Test the trickier recursive Env operations
    """

    def test_bind(self):
        e = WDL.Env.bind([], [], "foo", "bar")
        self.assertEqual(WDL.Env.resolve(e, [], "foo"), "bar")
        e = WDL.Env.bind(e, ["fruit"], "orange", "a")
        self.assertEqual(len(e), 2)
        self.assertEqual(WDL.Env.resolve(e, [], "foo"), "bar")
        self.assertEqual(WDL.Env.resolve(e, ["fruit"], "orange"), "a")
        e = WDL.Env.bind(e, ["fruit"], "pear", "b")
        self.assertEqual(len(e), 2)
        self.assertEqual(WDL.Env.resolve(e, [], "foo"), "bar")
        self.assertEqual(WDL.Env.resolve(e, ["fruit"], "orange"), "a")
        self.assertEqual(WDL.Env.resolve(e, ["fruit"], "pear"), "b")
        e = WDL.Env.bind(e, ["fruit", "apple"], "honeycrisp", "c")
        e = WDL.Env.bind(e, ["fruit", "apple"], "macintosh", "d")
        self.assertEqual(len(e), 2)
        self.assertEqual(WDL.Env.resolve(e, [], "foo"), "bar")
        self.assertEqual(WDL.Env.resolve(e, ["fruit"], "orange"), "a")
        self.assertEqual(WDL.Env.resolve(e, ["fruit"], "pear"), "b")
        self.assertEqual(len(WDL.Env.resolve_namespace(e, ["fruit", "apple"])), 2)
        self.assertEqual(WDL.Env.resolve(e, ["fruit", "apple"], "honeycrisp"), "c")
        self.assertEqual(WDL.Env.resolve(e, ["fruit", "apple"], "macintosh"), "d")

    def test_unbind(self):
        e = WDL.Env.bind([], [], "foo", "bar")
        e = WDL.Env.bind(e, ["fruit"], "orange", "a")
        e = WDL.Env.bind(e, ["fruit"], "pear", "b")
        e = WDL.Env.bind(e, ["fruit", "apple"], "honeycrisp", "c")
        e = WDL.Env.bind(e, ["fruit", "apple"], "macintosh", "d")

        self.assertEqual(WDL.Env.resolve(e, ["fruit", "apple"], "honeycrisp"), "c")
        self.assertEqual(WDL.Env.resolve(e, ["fruit", "apple"], "macintosh"), "d")
        WDL.Env.unbind(e, [], "macintosh") # no KeyError
        self.assertEqual(WDL.Env.resolve(e, ["fruit", "apple"], "macintosh"), "d")
        e = WDL.Env.unbind(e, ["fruit", "apple"], "macintosh")
        with self.assertRaises(KeyError):
            WDL.Env.resolve(e, ["fruit", "apple"], "macintosh")
        self.assertEqual(WDL.Env.resolve(e, ["fruit", "apple"], "honeycrisp"), "c")

        self.assertEqual(len(WDL.Env.resolve_namespace(e, ["fruit"])), 3)
        e = WDL.Env.unbind(e, ["fruit", "apple"], "honeycrisp")
        self.assertEqual(len(WDL.Env.resolve_namespace(e, ["fruit"])), 2)

    def test_subtract(self):
        e = WDL.Env.bind([], [], "foo", "bar")
        e = WDL.Env.bind(e, ["fruit"], "orange", "a")
        e = WDL.Env.bind(e, ["fruit"], "pear", "b")
        e = WDL.Env.bind(e, ["fruit", "apple"], "honeycrisp", "c")
        e = WDL.Env.bind(e, ["fruit", "apple"], "macintosh", "d")
        e = WDL.Env.bind(e, ["fruit", "grape"], "red", "e")
        e = WDL.Env.bind(e, ["fruit", "grape"], "green", "f")

        rhs = WDL.Env.bind([], ["fruit"], "pear", "b")
        rhs = WDL.Env.bind(rhs, ["fruit", "apple"], "honeycrisp", "c")

        e = WDL.Env.subtract(e, rhs)
        with self.assertRaises(KeyError):
            WDL.Env.resolve(e, ["fruit"], "pear")
        with self.assertRaises(KeyError):
            WDL.Env.resolve(e, ["fruit", "apple"], "honeycrisp")
        self.assertEqual(WDL.Env.resolve(e, [], "foo"), "bar")
        self.assertEqual(WDL.Env.resolve(e, ["fruit"], "orange"), "a")
        self.assertEqual(WDL.Env.resolve(e, ["fruit", "apple"], "macintosh"), "d")
        self.assertEqual(WDL.Env.resolve(e, ["fruit", "grape"], "green"), "f")

        e = WDL.Env.subtract(e, WDL.Env.bind([], ["fruit", "apple"], "macintosh", "d"))
        with self.assertRaises(KeyError):
            WDL.Env.resolve(e, ["fruit", "apple"], "macintosh")
        with self.assertRaises(KeyError):
            WDL.Env.resolve_namespace(e, ["fruit", "apple"])
        self.assertEqual(WDL.Env.resolve(e, [], "foo"), "bar")
        self.assertEqual(WDL.Env.resolve(e, ["fruit"], "orange"), "a")
        self.assertEqual(WDL.Env.resolve(e, ["fruit", "grape"], "green"), "f")


class TestValue(unittest.TestCase):
    def test_json(self):
        pty = WDL.Type.StructInstance("Person")
        pty.members = {
            "name": WDL.Type.String(), "age": WDL.Type.Int(),
            "pets": WDL.Type.Map((WDL.Type.String(), WDL.Type.Int()), optional=True)
        }
        cases = [
            (WDL.Type.Boolean(), True),
            (WDL.Type.Boolean(), False),
            (WDL.Type.Int(), 42),
            (WDL.Type.Float(), 3.14),
            (WDL.Type.String(), 'CNN is working frantically to find their "source."'),
            (WDL.Type.String(optional=True), None),
            (WDL.Type.Array(WDL.Type.String()), ["apple", "orange"]),
            (WDL.Type.Array(WDL.Type.String(optional=True)), ["apple", "orange", None]),
            (WDL.Type.Map((WDL.Type.String(), WDL.Type.Int())), {"cats": 42, "dogs": 99}),
            (pty, {"name": "Alyssa", "age": 42, "pets": None}),
            (pty, {"name": "Alyssa", "age": 42, "pets": {"cats": 42, "dogs": 99}}),

            (WDL.Type.Boolean(), 42, WDL.Error.InputError),
            (WDL.Type.Float(), "your president", WDL.Error.InputError),
            (WDL.Type.String(), None, WDL.Error.InputError),
            (pty, {"name": "Alyssa", "age": None, "pets": None}, WDL.Error.InputError),
            (pty, {"name": "Alyssa", "age": 42}, WDL.Error.InputError),
            (pty, {"name": "Alyssa", "age": 42, "pets": None, "address": "No 4, Privet Drive"}, WDL.Error.InputError),
        ]

        for t in cases:
            if len(t) >= 3 and inspect.isclass(t[2]):
                with self.assertRaises(t[2]):
                    WDL.Value.from_json(t[0],t[1])
            else:
                self.assertEqual(t[1], WDL.Value.from_json(t[0],t[1]).json)

        self.assertEqual(
            WDL.parse_expr('object {"name": "Alyssa", "age": 42, "address": "No 4, Privet Drive"}',
                           version="1.0").infer_type([]).eval([]).json,
            {"name": "Alyssa", "age": 42, "address": "No 4, Privet Drive"}
        )
