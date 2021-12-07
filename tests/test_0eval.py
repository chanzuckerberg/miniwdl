import unittest, inspect, json, random
from .context import WDL

class TestEval(unittest.TestCase):
    def test_expr_render(self):
        # types
        self.assertEqual(str(WDL.parse_expr("false")), "false")
        self.assertEqual(str(WDL.parse_expr("1")), "1")
        self.assertEqual(str(WDL.parse_expr("1.1")), "1.1")
        self.assertEqual(str(WDL.parse_expr('"Some text with a ~{placeholder}"')), '"Some text with a ~{placeholder}"')
        self.assertEqual(str(WDL.parse_expr('["An", "Array"]')), '["An", "Array"]')
        self.assertEqual(str(WDL.parse_expr('{"A": "Map"}')), '{"A": "Map"}')
        self.assertEqual(str(WDL.parse_expr('("A", "Pair")')), '("A", "Pair")')
        self.assertEqual(str(WDL.parse_expr('object {"A": "struct"}', "1.0")), '{"A": "struct"}')

        # logic
        self.assertEqual(str(WDL.parse_expr("true && false")), "true && false")
        self.assertEqual(str(WDL.parse_expr("true || false")), "true || false")
        self.assertEqual(str(WDL.parse_expr("true && false || true")), "true && false || true")
        self.assertEqual(str(WDL.parse_expr("!true")), "!true")
        self.assertEqual(str(WDL.parse_expr("if true then 1 else 2")), "if true then 1 else 2")

        # comparisons
        self.assertEqual(str(WDL.parse_expr("1 == 2")), "1 == 2")
        self.assertEqual(str(WDL.parse_expr("1 != 2")), "1 != 2")
        self.assertEqual(str(WDL.parse_expr("1 >= 2")), "1 >= 2")
        self.assertEqual(str(WDL.parse_expr("1 <= 2")), "1 <= 2")
        self.assertEqual(str(WDL.parse_expr("1 > 2")), "1 > 2")
        self.assertEqual(str(WDL.parse_expr("1 < 2")), "1 < 2")

        # arithmetics
        self.assertEqual(str(WDL.parse_expr("1+1")), "1 + 1")
        self.assertEqual(str(WDL.parse_expr("1-1")), "1 - 1")
        self.assertEqual(str(WDL.parse_expr("1/1")), "1 / 1")
        self.assertEqual(str(WDL.parse_expr("1*1")), "1 * 1")
        self.assertEqual(str(WDL.parse_expr("1%1")), "1 % 1")
        self.assertEqual(str(WDL.parse_expr("1*1")), "1 * 1")

        # functions
        self.assertEqual(str(WDL.parse_expr("defined(value)")), "defined(value)")
        self.assertEqual(str(WDL.parse_expr("select_first([1, 2])")), "select_first([1, 2])")

        # access
        self.assertEqual(str(WDL.parse_expr("[1,2][1]")), "[1, 2][1]")
        self.assertEqual(str(WDL.parse_expr("{1:2}[1]")), "{1: 2}[1]")
        self.assertEqual(str(WDL.parse_expr("a.b")), "a.b")

        # if-then-else
        self.assertEqual(str(WDL.parse_expr("if false then 1+1 else 2+2")), "if false then 1 + 1 else 2 + 2")

        # combinations
        combo = "[treu][1] || defined(var) && !(8 == 1 * (12 + if false then 2 else 3) / 6)"
        self.assertEqual(str(WDL.parse_expr(combo)), combo)
        combo2 = "(if true then 1 else 2 * 8) % a.b - 16"
        self.assertEqual(str(WDL.parse_expr(combo2)), combo2)
        combo3 = "defined(if true then hey else hello)"
        self.assertEqual(str(WDL.parse_expr(combo3)), combo3)
        ifthenelsechain = "!if true then if false then true else false else true"
        self.assertEqual(str(WDL.parse_expr(ifthenelsechain)), ifthenelsechain)

    def test_boolean(self):
        stdlib = WDL.StdLib.Base("1.0")
        expr = WDL.parse_expr("true")
        expr.infer_type([], stdlib)
        self.assertIsInstance(expr.type, WDL.Type.Boolean)
        self.assertEqual(str(expr.type), "Boolean")
        val = expr.eval([], stdlib)
        self.assertIsInstance(val, WDL.Value.Boolean)
        self.assertEqual(str(val.type), "Boolean")
        self.assertEqual(val.value, True)
        self.assertEqual(str(val), "true")
        self.assertEqual(val, WDL.Value.Boolean(True))
        self.assertNotEqual(val, WDL.Value.Boolean(False))
        self.assertIsInstance(expr.literal, WDL.Value.Boolean)

        expr = WDL.parse_expr("false")
        expr.infer_type([], stdlib)
        self.assertEqual(str(expr.type), "Boolean")
        val = expr.eval([], stdlib)
        self.assertEqual(str(val.type), "Boolean")
        self.assertEqual(val.value, False)
        self.assertEqual(str(val), "false")
        self.assertEqual(val, WDL.Value.Boolean(False))
        self.assertNotEqual(val, WDL.Value.Boolean(True))

    def _test_tuples(self, *tuples):
        stdlib = WDL.StdLib.Base("development")
        for tuple in tuples:
            assert(len(tuple) >= 2)
            expr = tuple[0]
            expected = tuple[1]
            env = None
            expected_type = None
            exn = None
            version = None
            for x in tuple[2:]:
                if isinstance(x, WDL.Env.Bindings):
                    env = x
                elif isinstance(x, WDL.Type.Base):
                    expected_type = x
                elif isinstance(x, str):
                    version = x
                elif inspect.isclass(x):
                    exn = x
                else:
                    assert False
            type_env = WDL.Env.Bindings()
            if env is not None:
                for binding in env:
                    type_env = type_env.bind(binding.name, binding.value.type)
            if exn:
                with self.assertRaises(exn, msg=expected):
                    x = WDL.parse_expr(expr, version=version).infer_type(type_env, stdlib).eval(env, stdlib)
            else:
                ex = WDL.parse_expr(expr, version=version).infer_type(type_env, stdlib)
                v = ex.eval(env, stdlib).expect(expected_type)
                if expected:
                    self.assertEqual(str(v), expected, str(expr))
                    if ex.literal:
                        self.assertEqual(str(ex.literal), expected)

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
            ("!false && false", "false"),
            ("!true && false", "false"),
            ("!(false && false)", "true"),
            ("!(false && true)", "true"),
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
            ("min(0,1)","0"),
            ("max(1,3.14)*2","6.280000"),
            ("1 + false", "(Ln 1, Col 1) Non-numeric operand to + operator", WDL.Error.IncompatibleOperand),
            ("min(max(0,1),true)", "(Ln 1, Col 1) Non-numeric operand to min operator", WDL.Error.IncompatibleOperand),
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
            ('2.0 + "bar"', '"2.000000bar"'),
            ('17 + "42"', '"1742"'),
            ('3.141 + ""', '"3.141000"'),
            ('3.141 * 1E-10 + ""', '"0.000000"'),
            ('3.141 * 1E10 + ""', '"31410000000.000000"'),
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
        self._test_tuples(
            (r'''"\\\n\t\'\"\012\x0aనేనుÆды\u0000"''', json.dumps("\\\n\t'\"\n\nనేనుÆды\x00")),
            (r'''"\xyz"''', None, WDL.Error.SyntaxError),
            (r'''"\u"''', None, WDL.Error.SyntaxError),
            (r'''"\uvwxyz"''', None, WDL.Error.SyntaxError),
        )
        chars = [c for c in (chr(i) for i in range(1,4096)) if c not in "\"'\\\n$~"]
        junk = []
        for c in chars:
            junk.append(c)
            junk.append(c + ''.join(random.choices(chars,k=15)))
        for i in range(len(junk)):
            junk[i] = ('"' + junk[i] + '"', json.dumps(junk[i]))
        self._test_tuples(*junk)

    def test_compound_equality(self):
        self._test_tuples(
            ("[1, 2, 3] == [1,2,3]", "true"),
            ("[1, 2, 3] == [2, 1, 3]", "false"),
            ('{"a": 1, "b": 2} == {"a": 1, "b": 2}', "true"),
            ('{"a": 1, "b": 2} == {"b": 2, "a": 1}', "false"),
            ('1 == None', "false", "development"),
            ('None == None', "true", "development")
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
            ("if 0 < 1 then 0 else false", "(Ln 1, Col 1) Expected Int instead of Boolean (unable to unify consequent & alternative types)", WDL.Error.StaticTypeMismatch),
            ("if true then 1 else 2.0", "1.000000", WDL.Type.Float()),
            ("if false then 1 else 2.0", "2.000000", WDL.Type.Float()),
            ("if true then 1.0 else 2", "1.000000", WDL.Type.Float()),
            ("if false then 1.0 else 2", "2.000000", WDL.Type.Float())
        )

    def test_array(self):
        stdlib = WDL.StdLib.Base("1.0")
        expr = WDL.parse_expr("[true,false]")
        expr.infer_type([], stdlib)
        self.assertEqual(str(expr.type), "Array[Boolean]+")

        env = []
        val = expr.eval(env, stdlib)
        self.assertIsInstance(val, WDL.Value.Array)
        self.assertEqual(str(val.type), "Array[Boolean]+")
        self.assertEqual(str(val), "[true, false]")
        self.assertIsInstance(expr.literal, WDL.Value.Array)

        self._test_tuples(
            ("[true, false][0]", "true"),
            ("[true, false][1]", "false"),
            ("[1+2, 3*4][1]", "12"),
            ("[1,2,3,]", "[1, 2, 3]"),
            ("[1,'a']", '["1", "a"]'),
            ("[]","[]", WDL.Type.Array(WDL.Type.Any())),
            ("[] == []","true"),
            ("[1, false]", '["1", "false"]', WDL.Type.Array(WDL.Type.String(), nonempty=True)),
            ("[1, {}]", "(Ln 1, Col 1) Expected Int instead of Boolean; inconsistent types within array", WDL.Error.IndeterminateType),
            ("1 + 2[3]", "(Ln 1, Col 5) Not an array", WDL.Error.NotAnArray),
            ("[1, 2, 3][true]", "(Ln 1, Col 11) Expected Int instead of Boolean; Array index", WDL.Error.StaticTypeMismatch),
            ("[1, 2, 3][4]", "(Ln 1, Col 11) Array index out of bounds", WDL.Error.OutOfBounds)
        )

    def test_float_coercion(self):
        self._test_tuples(
            ("1 + 1.0", "2.000000", WDL.Type.Float()),
            ("1.0 + 1", "2.000000", WDL.Type.Float()),
            ("1 == 1.0", "true"),
            ("1 == 1.1", "false"),
            ("1 != 1.1", "true"),
            ("1 < 1.0", "false"),
            ("1 <= 1.0", "true"),
            ("[1, 2.0]", "[1.000000, 2.000000]", WDL.Type.Array(WDL.Type.Float())),
            ("[1, 2.0][0]", "1.000000", WDL.Type.Float()),
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
            ("pi", "3.141590", WDL.Type.Float(), env),
            ("bogus", "(Ln 1, Col 1) Unknown identifier", WDL.Error.UnknownIdentifier, env),
            ("pi+e", "5.859870", env),
            ("t||f", "true", WDL.Type.Boolean(), env),
            ("if t then pi else e", "3.141590", env),
            ("true_rep_only", "false", env),
            ("lefty", "false", env),
            ("left_recursive", "false", env)
        )


    def test_interpolation(self):
        env = cons_env(("pi", WDL.Value.Float(3.14159)), ("e", WDL.Value.Float(2.71828)),
                        ("t", WDL.Value.Boolean(True)), ("f", WDL.Value.Boolean(False)),
                        ("s", WDL.Value.String("foo")))
        self._test_tuples(
            ('"${pi}"', '"3.141590"', env),
            ('"pi = ${pi}!"', '"pi = 3.141590!"', env),
            ('"pi+e = ${pi+e}!"', '"pi+e = 5.859870!"', env),
            ("'This is ${t}'", '"This is true"', env),
            ("'${f} is ${f}'", '"false is false"', env),
            ("'${s}bar'", '"foobar"', env),
            ('"$"','"$"'),
            ('"$shell"','"$shell"'),
            ("'c$'",'"c$"'),
            ("'The U.$. is re$pected again!'",'"The U.$. is re$pected again!"')
        )
        self._test_tuples(
            ('"${pi} ~{pi}$"', '"3.141590 ~{pi}$"', env, "draft-2"),
            ("'${pi} ~{pi}$'", '"3.141590 ~{pi}$"', env, "draft-2"),
            ('"${pi} ~{pi}$"', '"3.141590 3.141590$"', env, "1.0"),
            ("'${pi} ~{pi}~'", '"3.141590 3.141590~"', env, "1.0"),
            ("'$${pi}$'", '"$3.141590$"', env, "draft-2"),
            ('"$${pi}$$"', '"$3.141590$$"', env, "draft-2"),
            ("'$${pi}$'", '"$3.141590$"', env, "1.0"),
            ("'$${pi}$$'", '"$3.141590$$"', env, "1.0"),
            ("'$$${pi}~'", '"$$3.141590~"', env, "1.0"),
            ("'~~{pi}~'", '"~3.141590~"', env, "1.0"),
            ('"~~{pi}~"', '"~3.141590~"', env, "1.0"),
            ("'~~${pi}~'", '"~~3.141590~"', env, "1.0"),
            ("'$~{pi}~~'", '"$3.141590~~"', env, "1.0"),
            ("'$~${pi}~~'", '"$~3.141590~~"', env, "1.0"),
            ("'~{if f then '~{pi}' else '~{e}'}'", '"2.718280"', env, "1.0"),
            ("""'~{if f then "~{pi}" else "~{e}"}'""", '"2.718280"', env, "1.0"),
            (""" "~{if f then "~{pi}" else "~{e}"}" """, '"2.718280"', env, "1.0"),
        )

    def test_pair(self):
        env = cons_env(("p", WDL.Value.Pair(WDL.Type.Float(), WDL.Type.Float(),
                                            (WDL.Value.Float(3.14159), WDL.Value.Float(2.71828)))),
                        ("q", WDL.Value.Pair(WDL.Type.Pair(WDL.Type.Int(), WDL.Type.Int()),
                                                           WDL.Type.Float(optional=True),
                                             (WDL.Value.Pair(WDL.Type.Int(), WDL.Type.Int(),
                                                             (WDL.Value.Int(4), WDL.Value.Int(2))),
                                              WDL.Value.Null()))))
        self._test_tuples(
            ("(1,2)", "(1,2)", WDL.Type.Pair(WDL.Type.Int(), WDL.Type.Int())),
            ("(1,2).left", "1"),
            ("(1,false).right", "false"),
            ("(false,[1,2]).right[1]", "2"),
            ("[1,2].left", "", WDL.Error.NoSuchMember),
            ("false.right", "", WDL.Error.NoSuchMember),
            ("p.left", "3.141590", env),
            ("p.right", "2.718280", env),
            ("q.left.left", "4", env),
            ("q.left.right", "2", env)
        )

    def test_map(self):
        self._test_tuples(
            ("{'foo': 1, 'bar': 2}['bar']", "2"),
            ("{'foo': 1, 'bar': 2, 'baz': 3.0}['bar']", "2.000000", WDL.Type.Float()),
            ("{0: 1, 2: 3}[false]", "", WDL.Error.StaticTypeMismatch),
            ("{0: 1, 2: 3}['foo']", "", WDL.Error.EvalError),
            ("{0: 1, 0: 3}", "", WDL.Error.EvalError),
            ("{'foo': 1, 'bar': 2}[3]", "", WDL.Error.OutOfBounds), # int coerces to string...
            ("{3: 1, false: 2}", "", WDL.Error.IndeterminateType),
            ("{'foo': true, 'bar': 0,}", '{"foo": true, "bar": 0}', WDL.Type.Map((WDL.Type.String(), WDL.Type.String()))),
            ("{[1,2]: true, []: false}", '{[1, 2]: true, []: false}', WDL.Type.Map((WDL.Type.Array(WDL.Type.Int()), WDL.Type.Boolean()))),
            ("{[1]: true, [1]: false}", "", WDL.Error.EvalError),
            ("{(false, false): 0, (false, true): 1}", "", WDL.Type.Map((WDL.Type.Pair(WDL.Type.Boolean(), WDL.Type.Boolean()), WDL.Type.Int()))),
        )
        with self.assertRaisesRegex(WDL.Error.EvalError, "to JSON"):
            stdlib = WDL.StdLib.Base("1.0")
            WDL.parse_expr("{(false, false): 0, (false, true): 1}").infer_type(WDL.Env.Bindings(), stdlib).eval(WDL.Env.Bindings(), stdlib).json

    def test_errors(self):
        self._test_tuples(
            ("1 + bogus(2)", "(Ln 1, Col 5) No such function: bogus", WDL.Error.NoSuchFunction)
        )

    def test_short_circuit(self):
        self._test_tuples(
            ("true && 1/0 == 1", "", WDL.Error.EvalError),
            ("false && 1/0 == 1", "false"),
            ("false || 1/0 == 1", "", WDL.Error.EvalError),
            ("true || 1/0 == 1", "true"),
        )

def cons_env(*bindings):
    b = WDL.Env.Bindings()
    for (x,y) in bindings:
        b = WDL.Env.Bindings(WDL.Env.Binding(x,y), b)
    return b

class TestEnv(unittest.TestCase):
    """
    Test the trickier recursive Env operations
    """

    def test_bind(self):
        e = WDL.Env.Bindings(WDL.Env.Binding("foo", "bar"))
        self.assertEqual(e.resolve("foo"), "bar")
        e = e.bind("fruit.orange", "a")
        self.assertEqual(len(list(e)), 2)
        self.assertEqual(e.resolve("foo"), "bar")
        self.assertEqual(e.resolve("fruit.orange"), "a")
        e = e.bind("fruit.pear", "b")
        self.assertEqual(len(list(e)), 3)
        self.assertEqual(e.resolve("foo"), "bar")
        self.assertEqual(e.resolve("fruit.orange"), "a")
        self.assertEqual(e.resolve("fruit.pear"), "b")
        e = e.bind("fruit.apple.honeycrisp", "c").bind("fruit.apple.macintosh", "d")
        self.assertEqual(len(list(e)), 5)
        self.assertEqual(e.resolve("foo"), "bar")
        self.assertEqual(e.resolve("fruit.orange"), "a")
        self.assertEqual(e.resolve("fruit.pear"), "b")
        self.assertEqual(len(list(e.enter_namespace("fruit.apple"))), 2)
        self.assertEqual(e.resolve("fruit.apple.honeycrisp"), "c")
        self.assertEqual(e.resolve("fruit.apple.macintosh"), "d")

    def test_subtract(self):
        e = WDL.Env.Bindings()
        e = e.bind("foo", "bar").bind("fruit.orange", "a").bind("fruit.pear", "b")
        e = e.bind("fruit.apple.honeycrisp", "c").bind("fruit.apple.macintosh", "d")
        e = e.bind("fruit.grape.red", "e").bind("fruit.grape.green", "f")

        rhs = WDL.Env.Bindings().bind("fruit.pear","b").bind("fruit.apple.honeycrisp","c")

        e = e.subtract(rhs)
        with self.assertRaises(KeyError):
            e.resolve("fruit.pear")
        with self.assertRaises(KeyError):
            e.resolve("fruit.apple.honeycrisp")
        self.assertEqual(e.resolve("foo"), "bar")
        self.assertEqual(e.resolve("fruit.orange"), "a")
        self.assertEqual(e.resolve("fruit.apple.macintosh"), "d")
        self.assertEqual(e.resolve("fruit.grape.green"), "f")

        e = e.subtract(WDL.Env.Bindings(WDL.Env.Binding("fruit.apple.macintosh", None)))
        with self.assertRaises(KeyError):
            e.resolve("fruit.apple.macintosh")
        self.assertFalse(e.has_namespace("fruit.apple"))
        self.assertEqual(e.resolve("foo"), "bar")
        self.assertEqual(e.resolve("fruit.orange"), "a")
        self.assertEqual(e.resolve("fruit.grape.green"), "f")

    def test_namespaces(self):
        e = WDL.Env.Bindings().bind("fruit.apple.honeycrisp", 42)
        self.assertTrue(e.has_namespace("fruit.apple"))
        self.assertTrue(e.has_namespace("fruit."))
        self.assertFalse(e.has_namespace("fruit.apple.honeycrisp"))

        e = WDL.Env.Bindings().bind("apple.macintosh", 42).wrap_namespace("fruit")
        self.assertTrue(e.has_namespace("fruit.apple"))
        self.assertTrue(e.has_namespace("fruit."))


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
            (WDL.Type.File(), '/tmp/stdout.txt'),
            (WDL.Type.Array(WDL.Type.String()), ["apple", "orange"]),
            (WDL.Type.Array(WDL.Type.String(optional=True)), ["apple", "orange", None]),
            (WDL.Type.Map((WDL.Type.String(), WDL.Type.Int())), {"cats": 42, "dogs": 99}),
            (pty, {"name": "Alyssa", "age": 42, "pets": None}),
            (pty, {"name": "Alyssa", "age": 42, "pets": {"cats": 42, "dogs": 99}}),
            (WDL.Type.Array(WDL.Type.Pair(WDL.Type.String(), WDL.Type.Int())), [{"left": "a", "right": 0},{"left": "b", "right": 1}]),

            (WDL.Type.Boolean(), 42, WDL.Error.InputError),
            (WDL.Type.Float(), "your president", WDL.Error.InputError),
            (WDL.Type.String(), None, WDL.Error.InputError),
            (pty, {"name": "Alyssa"}, WDL.Error.InputError),
            (pty, {"name": "Alyssa", "age": None, "pets": None}, WDL.Error.InputError),
            (pty, {"name": "Alyssa", "age": 42, "pets": None, "address": "No 4, Privet Drive"}, WDL.Error.InputError),
        ]

        for t in cases:
            if len(t) >= 3 and inspect.isclass(t[2]):
                with self.assertRaises(t[2]):
                    WDL.Value.from_json(t[0],t[1])
            else:
                self.assertEqual(t[1], WDL.Value.from_json(t[0],t[1]).json)

        stdlib = WDL.StdLib.Base("1.0")
        self.assertEqual(
            WDL.parse_expr('object {"name": "Alyssa", "age": 42, "address": "No 4, Privet Drive"}',
                           version="1.0").infer_type([], stdlib).eval([], stdlib).json,
            {"name": "Alyssa", "age": 42, "address": "No 4, Privet Drive"}
        )

    def test_env_json(self):
        doc = WDL.parse_document(R"""
        version 1.0

        workflow w {
            call t as s
        }

        task t {
            input {
                String who
                Int age = 42
            }
            command {}
            output {
                String message = read_string("dummy")
            }
        }
        """)
        doc.typecheck()

        def rt(exe, d):
            namespace = ""
            if isinstance(exe, WDL.Workflow):
                namespace = exe.name
            self.assertEqual(WDL.values_to_json(WDL.values_from_json(d, exe.available_inputs, exe.required_inputs, namespace=namespace), namespace=namespace), d)

        rt(doc.tasks[0], {"who": "Alyssa"})
        rt(doc.tasks[0], {"who": "Alyssa", "age": 24})
        with self.assertRaises(WDL.Error.InputError):
            rt(doc.tasks[0], {"who": "Alyssa", "bogus": "Ben"})
        with self.assertRaises(WDL.Error.InputError):
            rt(doc.tasks[0], {})

        rt(doc.workflow, {"w.s.who": "Alyssa"})
        rt(doc.workflow, {"w.s.who": "Alyssa", "w.s.age": 24})
        with self.assertRaises(WDL.Error.InputError):
            rt(doc.workflow, {})
        with self.assertRaises(WDL.Error.InputError):
            rt(doc.workflow, {".who": "a"})
        with self.assertRaises(WDL.Error.InputError):
            rt(doc.workflow, {"w.s..who": "b"})

        # misc functionality
        self.assertEqual(WDL.values_to_json(doc.workflow.required_inputs, "w"), {"w.s.who": "String"})
        self.assertEqual(WDL.values_to_json(doc.workflow._type_env), {"s.message": "String", "s._present": "Any"})
