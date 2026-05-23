from math import exp
import unittest
import logging
import tempfile
import os
import json
import docker
from .context import WDL


class TestStdLib(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG, format="%(name)s %(levelname)s %(message)s")
        logger = logging.getLogger(cls.__name__)
        cfg = WDL.runtime.config.Loader(logger, [])

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_stdlib_")

    def _test_task(self, wdl: str, inputs=None, expected_exception: Exception = None, cfg=None):
        cfg = cfg or WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        try:
            doc = WDL.parse_document(wdl)
            assert len(doc.tasks) == 1
            doc.typecheck()
            assert len(doc.tasks[0].required_inputs.subtract(doc.tasks[0].available_inputs)) == 0
            if isinstance(inputs, dict):
                inputs = WDL.values_from_json(
                    inputs, doc.tasks[0].available_inputs, doc.tasks[0].required_inputs
                )
            rundir, outputs = WDL.runtime.run(
                cfg, doc.tasks[0], (inputs or WDL.Env.Bindings()), run_dir=self._dir, max_tasks=1
            )
        except WDL.runtime.RunFailed as exn:
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

    def _test_task_file(
        self, wdl: str, inputs=None, expected_exception: Exception = None, cfg=None
    ):
        cfg = cfg or WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        try:
            wdlfn = os.path.join(self._dir, "task.wdl")
            with open(wdlfn, "w") as outfile:
                outfile.write(wdl)
            doc = WDL.load(wdlfn)
            assert len(doc.tasks) == 1
            if isinstance(inputs, dict):
                inputs = WDL.values_from_json(
                    inputs, doc.tasks[0].available_inputs, doc.tasks[0].required_inputs
                )
            rundir, outputs = WDL.runtime.run(
                cfg, doc.tasks[0], (inputs or WDL.Env.Bindings()), run_dir=self._dir, max_tasks=1
            )
        except WDL.runtime.RunFailed as exn:
            if expected_exception:
                self.assertIsInstance(exn.__context__, expected_exception)
                return exn.__context__
            raise exn.__context__
        except Exception as exn:
            if expected_exception:
                self.assertIsInstance(exn, expected_exception)
                return exn
            raise
        if expected_exception:
            self.assertFalse(str(expected_exception) + " not raised")
        return WDL.values_to_json(outputs)

    def test_parse_tsv_preserves_blank_lines(self):
        parsed = WDL.StdLib._parse_tsv("a\tb\n\nc\td\n")
        self.assertEqual(parsed.json, [["a", "b"], [""], ["c", "d"]])

    def test_parse_tsv_objects(self):
        parsed = WDL.StdLib._parse_tsv_objects("name\tlane\nAlice\t3\nBob\t4\n")
        self.assertEqual(
            parsed.json, [{"name": "Alice", "lane": "3"}, {"name": "Bob", "lane": "4"}]
        )

        parsed = WDL.StdLib._parse_tsv_objects(
            "Alice\t3\nBob\t4\n",
            header=False,
            keys=[WDL.Value.String("name"), WDL.Value.String("lane")],
            function_name="read_tsv",
        )
        self.assertEqual(
            parsed.json, [{"name": "Alice", "lane": "3"}, {"name": "Bob", "lane": "4"}]
        )

        parsed = WDL.StdLib._parse_tsv_objects(
            "",
            header=False,
            keys=[],
            function_name="read_tsv",
        )
        self.assertEqual(parsed.json, [])

    def test_read_tsv_unit_branches(self):
        class LocalStdLib(WDL.StdLib.Base):
            def _devirtualize_filename(self, filename: str, directory: bool = False) -> str:
                return filename

            def _virtualize_filename(self, filename: str) -> str:
                return filename

        def infer(expr: str, version: str = "1.2", type_env=None):
            stdlib = LocalStdLib(version)
            parsed = WDL.parse_expr(expr, version=version).infer_type(
                type_env or WDL.Env.Bindings(), stdlib
            )
            return parsed, stdlib

        with self.assertRaises(WDL.Error.WrongArity):
            infer("read_tsv()")
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            infer("read_tsv([1])")
        with self.assertRaises(WDL.Error.WrongArity):
            infer('read_tsv("x", true, ["a"], "extra")')
        with self.assertRaises(WDL.Error.WrongArity):
            infer('read_tsv("x", true)', version="1.1")
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            infer('read_tsv("x", "true")')
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            infer('read_tsv("x", false)')
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            infer(
                'read_tsv("x", header)',
                type_env=WDL.Env.Bindings().bind("header", WDL.Type.Boolean()),
            )
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            infer('read_tsv("x", true, 1)')

        table = os.path.join(self._dir, "table.tsv")
        with open(table, "w") as outfile:
            outfile.write("name\tlane\nAlice\t3\nBob\t4\n")
        no_header = os.path.join(self._dir, "no_header.tsv")
        with open(no_header, "w") as outfile:
            outfile.write("Alice\t3\nBob\t4\n")

        expr, stdlib = infer("read_tsv({})".format(json.dumps(table)))
        self.assertEqual(expr.type, WDL.Type.Array(WDL.Type.Array(WDL.Type.String())))
        self.assertEqual(
            expr.eval(WDL.Env.Bindings(), stdlib).json,
            [["name", "lane"], ["Alice", "3"], ["Bob", "4"]],
        )

        expr, stdlib = infer("read_tsv({}, true)".format(json.dumps(table)))
        self.assertEqual(
            expr.type, WDL.Type.Array(WDL.Type.Map((WDL.Type.String(), WDL.Type.String())))
        )
        self.assertEqual(
            expr.eval(WDL.Env.Bindings(), stdlib).json,
            [{"name": "Alice", "lane": "3"}, {"name": "Bob", "lane": "4"}],
        )

        expr, stdlib = infer('read_tsv({}, false, ["name", "lane"])'.format(json.dumps(no_header)))
        self.assertEqual(
            expr.type, WDL.Type.Array(WDL.Type.Map((WDL.Type.String(), WDL.Type.String())))
        )
        self.assertEqual(
            expr.eval(WDL.Env.Bindings(), stdlib).json,
            [{"name": "Alice", "lane": "3"}, {"name": "Bob", "lane": "4"}],
        )

        expr, stdlib = infer('read_tsv({}, true, ["sample", "value"])'.format(json.dumps(table)))
        self.assertEqual(
            expr.eval(WDL.Env.Bindings(), stdlib).json,
            [{"sample": "Alice", "value": "3"}, {"sample": "Bob", "value": "4"}],
        )

        expr, stdlib = infer('read_tsv("/no/such/file.tsv")')
        with self.assertRaises(WDL.Error.EvalError):
            expr.eval(WDL.Env.Bindings(), stdlib)

        duplicate = os.path.join(self._dir, "duplicate.tsv")
        with open(duplicate, "w") as outfile:
            outfile.write("name\tname\nAlice\t3\n")
        expr, stdlib = infer("read_tsv({}, true)".format(json.dumps(duplicate)))
        with self.assertRaises(WDL.Error.InputError):
            expr.eval(WDL.Env.Bindings(), stdlib)

        expr, stdlib = infer("read_tsv({}, true)".format(json.dumps(table)))
        parse_tsv_objects = WDL.StdLib._parse_tsv_objects
        try:

            def raise_eval_error(*args, **kwargs):
                raise WDL.Error.EvalError(expr, "sentinel")

            WDL.StdLib._parse_tsv_objects = raise_eval_error
            with self.assertRaisesRegex(WDL.Error.EvalError, "sentinel"):
                expr.eval(WDL.Env.Bindings(), stdlib)
        finally:
            WDL.StdLib._parse_tsv_objects = parse_tsv_objects

    def _eval_expr(self, expr: str, env=None, version: str = "development"):
        env = env or WDL.Env.Bindings()
        type_env = WDL.Env.Bindings()
        for binding in env:
            type_env = type_env.bind(binding.name, binding.value.type)
        stdlib = WDL.StdLib.Base(version)
        ex = WDL.parse_expr(expr, version=version).infer_type(type_env, stdlib)
        return ex.eval(env, stdlib)

    def _infer_expr_type(self, expr: str, env=None, type_env=None, version: str = "development"):
        env = env or WDL.Env.Bindings()
        if type_env is None:
            type_env = WDL.Env.Bindings()
            for binding in env:
                type_env = type_env.bind(binding.name, binding.value.type)
        stdlib = WDL.StdLib.Base(version)
        return WDL.parse_expr(expr, version=version).infer_type(type_env, stdlib).type

    def test_length_version_gating(self):
        self.assertEqual(str(self._infer_expr_type("length([1])", version="1.1")), "Int")
        optional_array_env = WDL.Env.Bindings().bind(
            "xs", WDL.Type.Array(WDL.Type.Int(), optional=True)
        )
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._infer_expr_type("length(xs)", type_env=optional_array_env, version="1.1")

        for version in ("1.0", "1.1"):
            for expr in ('length("abc")', 'length({"a": 1})', "length(object {a: 1})"):
                with self.subTest(version=version, expr=expr):
                    with self.assertRaises(WDL.Error.StaticTypeMismatch):
                        self._infer_expr_type(expr, version=version)

        self.assertEqual(str(self._infer_expr_type('length("abc")', version="1.2")), "Int")
        self.assertEqual(str(self._infer_expr_type('length({"a": 1})', version="1.2")), "Int")
        self.assertEqual(str(self._infer_expr_type("length(object {a: 1})", version="1.2")), "Int")

    def test_size_version_gating(self):
        for version in ("1.0", "1.1"):
            for wdl in (
                f"""version {version}
                task t {{
                    input {{
                        Map[String, File] xs
                    }}
                    command {{}}
                    output {{
                        Float x = size(xs)
                    }}
                }}
                """,
                f"""version {version}
                struct S {{
                    File f
                }}
                task t {{
                    input {{
                        S s
                    }}
                    command {{}}
                    output {{
                        Float x = size(s)
                    }}
                }}
                """,
            ):
                with self.subTest(version=version, wdl=wdl):
                    self._test_task(wdl, expected_exception=WDL.Error.StaticTypeMismatch)

        doc = WDL.parse_document(
            R"""
            version 1.2
            struct S {
                File f
            }
            task t {
                input {
                    Map[String, File] xs
                    S s
                }
                command {}
                output {
                    Float x = size(xs)
                    Float y = size(s)
                }
            }
            """
        )
        doc.typecheck()

    def test_stdlib_branch_coverage_length_contains_key(self):
        # length(): wrong arity and optional argument rejection during quantifier checks
        with self.assertRaises(WDL.Error.WrongArity):
            self._eval_expr("length([1], [2])", version="1.2")

        tenv = WDL.Env.Bindings().bind("a", WDL.Type.Array(WDL.Type.Int(), optional=True))
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_expr("length(a)", version="1.2").infer_type(tenv, WDL.StdLib.Base("1.2"))

        # length(): runtime EvalError on unsupported dynamic Any payload
        fn = os.path.join(self._dir, "scalar.json")
        with open(fn, "w") as outfile:
            json.dump(1, outfile)
        with self.assertRaises(WDL.Error.EvalError):
            self._eval_expr(f'length(read_json("{fn}"))', version="1.2")

        # collect_by_key(): wrong arity
        with self.assertRaises(WDL.Error.WrongArity):
            self._eval_expr("collect_by_key([],[1])", version="1.2")

        # contains_key() map variant: optional map & nested keys require String-keyed map
        tenv = WDL.Env.Bindings().bind(
            "m", WDL.Type.Map((WDL.Type.String(), WDL.Type.Int()), optional=True)
        )
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_expr('contains_key(m, "a")', version="1.2").infer_type(
                tenv, WDL.StdLib.Base("1.2")
            )

        tenv = WDL.Env.Bindings().bind("m", WDL.Type.Map((WDL.Type.Int(), WDL.Type.Int())))
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_expr('contains_key(m, ["a"])', version="1.2").infer_type(
                tenv, WDL.StdLib.Base("1.2")
            )

        # contains_key() struct/read_json key type checks
        tenv = WDL.Env.Bindings().bind("s", WDL.Type.StructInstance("S", optional=True))
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_expr("contains_key(s, 1)", version="1.2").infer_type(
                tenv, WDL.StdLib.Base("1.2")
            )

        tenv = WDL.Env.Bindings().bind("j", WDL.Type.Any())
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_expr("contains_key(read_json('x.json'), 1)", version="1.2").infer_type(
                tenv, WDL.StdLib.Base("1.2")
            )

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._eval_expr('contains_key({"a": 1}, [1])', version="1.2")

        # runtime nested-key traversal edges
        self.assertEqual(
            str(self._eval_expr('contains_key({"a": 1}, ["a", "b"])', version="1.2")), "false"
        )
        env = WDL.Env.Bindings().bind("path", WDL.Value.Array(WDL.Type.String(), []))
        self.assertEqual(
            str(self._eval_expr('contains_key({"a": 1}, path)', env=env, version="1.2")), "false"
        )

    def test_collect_by_key_float_keys(self):
        self.assertEqual(
            str(self._eval_expr('length(keys(collect_by_key([(1.0000001,"a"),(1.0000002,"b")])))')),
            "2",
        )
        self.assertEqual(
            str(self._eval_expr('as_map([(1.0000001,"a"),(1.0000002,"b")])[1.0000002]')),
            '"b"',
        )
        self.assertEqual(
            str(self._eval_expr('length(collect_by_key([(1.0,"a"),(1.0,"b")])[1.0])')),
            "2",
        )

    def test_zip_cross_nonempty_inference(self):
        stdlib = WDL.StdLib.Base("development")
        self.assertEqual(
            str(
                WDL.parse_expr("cross([1], range(0))", version="development")
                .infer_type([], stdlib)
                .type
            ),
            "Array[Pair[Int,Int]]",
        )
        self.assertEqual(
            str(WDL.parse_expr("zip([1], [2])", version="development").infer_type([], stdlib).type),
            "Array[Pair[Int,Int]]+",
        )

    def test_basename_empty_suffix(self):
        env = WDL.Env.Bindings().bind("sfx", WDL.Value.Null())
        self.assertEqual(str(self._eval_expr('basename("/path/to/file.txt","")')), '"file.txt"')
        self.assertEqual(str(self._eval_expr('basename("file.txt","")')), '"file.txt"')
        self.assertEqual(
            str(self._eval_expr('basename("/path/to/file.txt",sfx)', env=env)), '"file.txt"'
        )

    def test_join_paths(self):
        class LocalStdLib(WDL.StdLib.Base):
            def _join_paths_default_directory(self) -> str:
                return "/work"

        def eval_join(expr: str, env=None, type_env=None):
            env = env or WDL.Env.Bindings()
            if type_env is None:
                type_env = WDL.Env.Bindings()
                for binding in env:
                    type_env = type_env.bind(binding.name, binding.value.type)
            stdlib = LocalStdLib("1.2")
            ex = WDL.parse_expr(expr, version="1.2").infer_type(type_env, stdlib)
            self.assertEqual(str(ex.type), "String")
            return ex.eval(env, stdlib)

        self.assertEqual(str(eval_join('join_paths("/usr", "bin")')), '"/usr/bin"')
        self.assertEqual(str(eval_join('join_paths("/usr", ["bin", "echo"])')), '"/usr/bin/echo"')
        self.assertEqual(str(eval_join('join_paths(["/usr", "bin", "echo"])')), '"/usr/bin/echo"')
        self.assertEqual(str(eval_join('join_paths("usr", "bin")')), '"/work/usr/bin"')
        self.assertEqual(
            str(eval_join('join_paths(["usr", "bin", "echo"])')), '"/work/usr/bin/echo"'
        )
        self.assertEqual(str(eval_join('join_paths("/usr/", "./bin/../bin")')), '"/usr/bin"')

        env = WDL.Env.Bindings().bind("paths", WDL.Value.Array(WDL.Type.String(), []))
        type_env = WDL.Env.Bindings().bind(
            "paths", WDL.Type.Array(WDL.Type.String(), nonempty=True)
        )
        with self.assertRaises(WDL.Error.EmptyArray):
            eval_join("join_paths(paths)", env=env, type_env=type_env)

        for expr in (
            'join_paths("/usr", "/bin")',
            'join_paths("/usr", ["bin", "/echo"])',
            'join_paths(["/usr", "/bin"])',
        ):
            with self.subTest(expr=expr):
                with self.assertRaisesRegex(
                    WDL.Error.EvalError, "only the first path may be absolute"
                ):
                    eval_join(expr)

    def test_join_paths_typecheck(self):
        with self.assertRaises(WDL.Error.NoSuchFunction):
            self._infer_expr_type('join_paths("/usr", "bin")', version="1.1")
        with self.assertRaises(WDL.Error.WrongArity):
            self._infer_expr_type("join_paths()", version="1.2")
        with self.assertRaises(WDL.Error.WrongArity):
            self._infer_expr_type('join_paths("/usr", "bin", "echo")', version="1.2")
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._infer_expr_type("join_paths(1)", version="1.2")
        with self.assertRaisesRegex(
            WDL.Error.StaticTypeMismatch, "expects String or Array\\[String\\]\\+"
        ):
            self._infer_expr_type('join_paths("/usr", {"x": 1})', version="1.2")
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._infer_expr_type("join_paths(None)", version="1.2")
        optional_dir = WDL.Env.Bindings().bind("d", WDL.Type.Directory(optional=True))
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._infer_expr_type('join_paths(d, "x")', type_env=optional_dir, version="1.2")

    def test_join_paths_runtime_context_required(self):
        stdlib = WDL.StdLib.Base("1.2")
        expr = WDL.parse_expr('join_paths(["relative", "path"])', version="1.2").infer_type(
            WDL.Env.Bindings(), stdlib
        )
        with self.assertRaisesRegex(
            WDL.Error.EvalError, "relative path resolution requires runtime context"
        ):
            expr.eval(WDL.Env.Bindings(), stdlib)

        doc = WDL.parse_document(
            R"""
            version 1.2
            workflow w {
                output {
                    String path = join_paths(["relative", "path"])
                }
            }
            """
        )
        doc.typecheck()
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        state = WDL.runtime.workflow.StateMachine(
            self.id(), self._dir, doc.workflow, WDL.Env.Bindings()
        )
        stdlib = WDL.runtime.workflow._StdLib(cfg, "1.2", state, None)
        with self.assertRaisesRegex(
            NotImplementedError, "relative path resolution requires WDL source"
        ):
            stdlib._join_paths_default_directory()

    def test_workflow_allowlisted_input_directory_children(self):
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        input_dir = os.path.join(self._dir, "input")
        os.makedirs(os.path.join(input_dir, "subdir"))
        with open(os.path.join(input_dir, "alice.txt"), "w") as outfile:
            print("Alice", file=outfile)
        with open(os.path.join(input_dir, "subdir", "bob.txt"), "w") as outfile:
            print("Bob", file=outfile)
        outside = os.path.join(self._dir, "outside.txt")
        with open(outside, "w") as outfile:
            print("outside", file=outfile)
        outside_dir = os.path.join(self._dir, "outside")
        os.makedirs(outside_dir)
        os.symlink(outside, os.path.join(input_dir, "owned.txt"))
        os.symlink(outside_dir, os.path.join(input_dir, "owned_dir"))

        allowlist = {input_dir + "/"}
        resolve_workflow_path = WDL.runtime.workflow._resolve_workflow_path

        self.assertEqual(
            resolve_workflow_path(
                cfg, allowlist, "test", WDL.Value.File(os.path.join(input_dir, "alice.txt"))
            ),
            os.path.join(input_dir, "alice.txt"),
        )
        self.assertEqual(
            resolve_workflow_path(
                cfg, allowlist, "test", WDL.Value.Directory(os.path.join(input_dir, "subdir"))
            ),
            os.path.join(input_dir, "subdir"),
        )
        self.assertEqual(
            resolve_workflow_path(
                cfg,
                allowlist,
                "test",
                WDL.Value.File(os.path.join(input_dir, "subdir", "..", "alice.txt")),
            ),
            os.path.join(input_dir, "alice.txt"),
        )
        self.assertIsNone(
            resolve_workflow_path(
                cfg,
                allowlist,
                "test",
                WDL.Value.File(os.path.join(input_dir, "missing.txt")),
                null_if_missing=True,
            )
        )
        self.assertIsNone(
            resolve_workflow_path(
                cfg,
                allowlist,
                "test",
                WDL.Value.Directory(os.path.join(input_dir, "missing_dir")),
                null_if_missing=True,
            )
        )
        with self.assertRaises(WDL.Error.InputError):
            resolve_workflow_path(
                cfg, allowlist, "test", WDL.Value.File(os.path.join(input_dir, "missing.txt"))
            )
        with self.assertRaises(WDL.Error.InputError):
            resolve_workflow_path(
                cfg, allowlist, "test", WDL.Value.File(os.path.join(input_dir, "subdir"))
            )
        with self.assertRaises(WDL.Error.InputError):
            resolve_workflow_path(
                cfg,
                allowlist,
                "test",
                WDL.Value.File(os.path.join(input_dir, "subdir")),
                null_if_missing=True,
            )
        with self.assertRaises(WDL.Error.InputError):
            resolve_workflow_path(
                cfg, allowlist, "test", WDL.Value.File(os.path.join(input_dir, "owned.txt"))
            )
        with self.assertRaises(WDL.Error.InputError):
            resolve_workflow_path(
                cfg,
                allowlist,
                "test",
                WDL.Value.Directory(os.path.join(input_dir, "owned_dir")),
            )
        with self.assertRaises(WDL.Error.InputError):
            resolve_workflow_path(cfg, allowlist, "test", WDL.Value.File(outside))
        with self.assertRaises(WDL.Error.InputError):
            resolve_workflow_path(
                cfg, allowlist, "test", WDL.Value.File(outside), null_if_missing=True
            )

    def test_workflow_allowlisted_input_directory_url_children(self):
        cfg = WDL.runtime.config.Loader(logging.getLogger(self.id()), [])
        setattr(cfg, "_downloaders", ({}, {"foo": object()}))
        allowlist = {"foo://bucket/input/"}
        resolve_workflow_path = WDL.runtime.workflow._resolve_workflow_path

        self.assertEqual(
            resolve_workflow_path(
                cfg, allowlist, "test", WDL.Value.File("foo://bucket/input/alice.txt")
            ),
            "foo://bucket/input/alice.txt",
        )
        self.assertEqual(
            resolve_workflow_path(
                cfg, allowlist, "test", WDL.Value.Directory("foo://bucket/input/subdir/")
            ),
            "foo://bucket/input/subdir/",
        )
        with self.assertRaises(WDL.Error.InputError):
            resolve_workflow_path(
                cfg, allowlist, "test", WDL.Value.File("foo://bucket/input2/alice.txt")
            )

    def test_task_source_relative_stdlib_path_arguments(self):
        os.makedirs(os.path.join(self._dir, "data/subdir"))
        with open(os.path.join(self._dir, "data/input.txt"), "w") as outfile:
            outfile.write("Alice\nBob\n")
        with open(os.path.join(self._dir, "data/subdir/inside.txt"), "w") as outfile:
            outfile.write("inside\n")

        outp = self._test_task_file(
            R"""
            version 1.2
            task t {
                File f = "data/input.txt"
                Directory d = "data/subdir"
                Array[String] lines = read_lines("data/input.txt")
                Float file_size = size("data/input.txt")
                Float dir_size = size(d)
                Map[File, String] labels = {"data/input.txt": "label"}
                Array[File] files = ["data/input.txt"]
                Boolean lookup = labels["data/input.txt"] == "label"
                Boolean has_key = contains_key(labels, "data/input.txt")
                Boolean has_file = contains(files, "data/input.txt")
                Boolean equal = f == "data/input.txt"
                Boolean not_equal = f != "data/subdir/inside.txt"
                Boolean absolute_not_equal = f != "/etc/passwd"
                command {}
                output {
                    Array[String] out_lines = lines
                    Float out_file_size = file_size
                    Float out_dir_size = dir_size
                    Boolean out_lookup = lookup
                    Boolean out_has_key = has_key
                    Boolean out_has_file = has_file
                    Boolean out_equal = equal
                    Boolean out_not_equal = not_equal
                    Boolean out_absolute_not_equal = absolute_not_equal
                }
            }
            """
        )
        self.assertEqual(outp["out_lines"], ["Alice", "Bob"])
        self.assertEqual(outp["out_file_size"], 10.0)
        self.assertEqual(outp["out_dir_size"], 7.0)
        self.assertTrue(outp["out_lookup"])
        self.assertTrue(outp["out_has_key"])
        self.assertTrue(outp["out_has_file"])
        self.assertTrue(outp["out_equal"])
        self.assertTrue(outp["out_not_equal"])
        self.assertTrue(outp["out_absolute_not_equal"])

    def test_task_source_relative_stdlib_rejects_escape(self):
        with open(os.path.join(self._dir, "outside.txt"), "w") as outfile:
            outfile.write("outside\n")

        exn = self._test_task_file(
            R"""
            version 1.2
            task t {
                String contents = read_string("../outside.txt")
                command {}
            }
            """,
            expected_exception=WDL.Error.EvalError,
        )
        self.assertIn("must reside within WDL source directory", str(exn))

    def test_task_source_relative_stdlib_operator_rejects_escape(self):
        os.makedirs(os.path.join(self._dir, "data"))
        with open(os.path.join(self._dir, "data/input.txt"), "w") as outfile:
            outfile.write("input\n")

        cases = [
            'labels["../outside.txt"] == "label"',
            'contains_key(labels, "../outside.txt")',
            'contains(files, "../outside.txt")',
            'f == "../outside.txt"',
        ]
        for expr in cases:
            exn = self._test_task_file(
                R"""
                version 1.2
                task t {
                    File f = "data/input.txt"
                    Map[File, String] labels = {"data/input.txt": "label"}
                    Array[File] files = ["data/input.txt"]
                    Boolean bad = PLACEHOLDER
                    command {}
                }
                """.replace("PLACEHOLDER", expr),
                expected_exception=WDL.Error.InputError,
            )
            self.assertIn("must reside within WDL source directory", str(exn), expr)
            self.assertIn("../outside.txt", str(exn), expr)

    def test_parse_tsv_row_type(self):
        rows = WDL.StdLib._parse_tsv("alpha\tbeta\n")
        self.assertEqual(rows.json, [["alpha", "beta"]])
        self.assertEqual(str(rows.type), "Array[Array[String]]+")
        self.assertEqual(str(rows.value[0].type), "Array[String]+")

    def test_eq_opt(self):
        # regression test issue #634
        wdl = """
        version 1.1
        task test_cmp {
            input {
                Int i
                Int? j
                Int? k
            }
            command {}
            output {
                Boolean a = i == j
                Boolean b = i != j
                Boolean c = i == k
                Boolean d = i != k
                Boolean e = j == None
                Boolean f = j != None
            }
        }
        """
        out = self._test_task(wdl, {"i": 0, "k": 1})
        assert out["a"] == False
        assert out["b"] == True
        assert out["c"] == False
        assert out["d"] == True
        assert out["e"] == True
        assert out["f"] == False

        self._test_task(
            """
        version 1.1
        task test_cmp {
            input {
                Int i
                Int? j
            }
            command {}
            output {
                Boolean a = i <= j
            }
        }
        """,
            {"i": 0},
            expected_exception=WDL.Error.ValidationError,
        )

    def test_size_polytype(self):
        tmpl = """
        version 1.0
        task test_size {{
            input {{
                File file1
                File file2
            }}
            {}
            command <<<
                echo "nop"
            >>>
        }}
        """

        for case in [
            "Float sz = size(file1)",
            "Float sz = size(file1, 'GB')",
            "Float sz = size([file1,file2], 'KB')",
            "Float sz = size(['file1','file2'], 'KB')",
        ]:
            doc = WDL.parse_document(tmpl.format(case))
            doc.typecheck()

        for case in [
            ("Float sz = size()", WDL.Error.WrongArity),
            ("Float sz = size(file1,file2,'MB')", WDL.Error.WrongArity),
            ("Float sz = size(42)", WDL.Error.StaticTypeMismatch),
            ("Float sz = size([42])", WDL.Error.StaticTypeMismatch),
            ("Float sz = size(file1,file2)", WDL.Error.StaticTypeMismatch),
            ("Float sz = size(file1,[file2])", WDL.Error.StaticTypeMismatch),
        ]:
            doc = WDL.parse_document(tmpl.format(case[0]))
            with self.assertRaises(case[1]):
                doc.typecheck()

        tmpl = """
        version 1.2
        struct PathStruct {{
            File file
            Directory dir
            String name
        }}
        task test_size {{
            input {{
                File file1
                File file2
                File? nullfile
                Directory dir1
                Directory dir2
                Array[File]? maybe_files
                Map[String, Pair[Int, File?]] nested_files
                Array[Pair[String, Directory?]] nested_dirs
                Map[File, Int] file_keys
                PathStruct path_struct
                Array[Int] ints
                Map[String, Int] string_to_int
            }}
            {}
            command <<<
                echo "nop"
            >>>
        }}
        """

        for case in [
            "Float sz = size(dir1)",
            "Float sz = size(dir1, 'GB')",
            "Float sz = size([dir1,dir2], 'KB')",
            "Float sz = size(maybe_files)",
            "Float sz = size(nested_files)",
            "Float sz = size(nested_dirs)",
            "Float sz = size(file_keys)",
            "Float sz = size(path_struct)",
        ]:
            doc = WDL.parse_document(tmpl.format(case))
            doc.typecheck()

        for case in [
            ("Float sz = size([42])", WDL.Error.StaticTypeMismatch),
            ("Float sz = size([])", WDL.Error.StaticTypeMismatch),
            ("Float sz = size({})", WDL.Error.StaticTypeMismatch),
            ("Float sz = size(None)", WDL.Error.StaticTypeMismatch),
            ('Float sz = size(read_json("x.json"))', WDL.Error.StaticTypeMismatch),
            ("Float sz = size(dir1,dir2)", WDL.Error.StaticTypeMismatch),
            ("Float sz = size(dir1,[dir2])", WDL.Error.StaticTypeMismatch),
        ]:
            doc = WDL.parse_document(tmpl.format(case[0]))
            with self.assertRaises(case[1]):
                doc.typecheck()

        for case in ["Float sz = size(ints)", "Float sz = size(string_to_int)"]:
            doc = WDL.parse_document(tmpl.format(case))
            with self.assertRaisesRegex(
                WDL.Error.StaticTypeMismatch, "compound type lacking any File/Directory"
            ):
                doc.typecheck()

        with self.assertRaises(WDL.Error.InputError):
            WDL.StdLib._Size._coerce_paths_argument(WDL.Value.Int(42), WDL.Type.Int())

    def test_length_defined_range(self):
        outputs = self._test_task(
            R"""
        version 1.0
        task test_length {
            input {
                Int one
                Int? two
                Int? three
            }
            command {}
            output {
                Array[Int] lengths = [length([]), length([42]), length([42,43])]
                Array[Boolean] defineds = [defined(one), defined(two), defined(three)]
                Array[Array[Int]] ranges = [range(0), range(1), range(3)]
            }
        }
        """,
            {"one": 42, "two": 43},
        )
        self.assertEqual(
            outputs,
            {"lengths": [0, 1, 2], "defineds": [True, True, False], "ranges": [[], [0], [0, 1, 2]]},
        )
        self._test_task(
            R"""
        version 1.0
        task bogus {
            command {}
            output {
                Array[Int] bogus = range(-42)
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        with self.assertRaises(WDL.Error.WrongArity):
            self._eval_expr("length([1], [2])", version="1.2")

        # Test length() with Maps
        outputs = self._test_task(R"""
        version 1.2
        task test_length_map {
            input {
                Map[String, Int] m = {"a": 1, "b": 2, "c": 3}
            }
            command {}
            output {
                Int map_len = length(m)
            }
        }
        """)
        self.assertEqual(outputs["map_len"], 3)

        # Test length() with String
        outputs = self._test_task(R"""
        version 1.2
        task test_length_string {
            command {}
            output {
                Int string_len = length("ABCDE")
            }
        }
        """)
        self.assertEqual(outputs["string_len"], 5)

        # Test length() with Object literals
        outputs = self._test_task(R"""
        version 1.2
        task test_length_object {
            command {}
            output {
                Int object_len = length(object {a: 1, b: 2})
            }
        }
        """)
        self.assertEqual(outputs["object_len"], 2)

        # Error: Structs aren't in the WDL 1.2 length() signature
        self._test_task(
            R"""
        version 1.2
        struct Person {
            String first
            String last
            Int age
        }
        task test_length_struct {
            input {
                Person p = Person {
                    first: "John",
                    last: "Doe",
                    age: 30
                }
            }
            command {}
            output {
                Int struct_len = length(p)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Test length(read_json()) on array
        outputs = self._test_task(R"""
        version 1.2
        task test_length_json_array {
            command <<<
                echo '[1, 2, 3, 4, 5]' > data.json
            >>>
            output {
                Int len = length(read_json("data.json"))
            }
        }
        """)
        self.assertEqual(outputs["len"], 5)

        # Error: None isn't a valid dynamic length() argument
        self._test_task(
            R"""
        version 1.2
        task bad {
            command {}
            output {
                Int len = length(None)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Test length(read_json()) on object
        outputs = self._test_task(R"""
        version 1.2
        task test_length_json_object {
            command <<<
                echo '{"x": 1, "y": 2, "z": 3}' > data.json
            >>>
            output {
                Int len = length(read_json("data.json"))
            }
        }
        """)
        self.assertEqual(outputs["len"], 3)

        # Test length(read_json()) on string
        outputs = self._test_task(R"""
        version 1.2
        task test_length_json_string {
            command <<<
                echo '"hello"' > data.json
            >>>
            output {
                Int len = length(read_json("data.json"))
            }
        }
        """)
        self.assertEqual(outputs["len"], 5)

    def test_floor_ceil_round(self):
        outputs = self._test_task(R"""
        version 1.0
        task test_floor_ceil_round {
            command {}
            output {
                Array[Int] ans = [
                    floor(3.14),
                    ceil(-3.14),
                    round(42.5),
                    round(42.5000000001)
                ]
            }
        }
        """)
        self.assertEqual(outputs, {"ans": [3, -3, 43, 43]})

        # regression test issue #698
        outputs = self._test_task(
            R"""
        version 1.1

        task test_round {
            input {
                Int i1
            }

            Int i2 = i1 + 1
            Float f1 = i1 + 0.49
            Float f2 = i1 + 0.50

            command {}

            output {
                Array[Boolean] all_true = [round(f1) == i1, round(f2) == i2]
            }
        }
        """,
            {"i1": 42},
        )
        self.assertEqual(outputs, {"all_true": [True, True]})

    def test_basename_prefix(self):
        outputs = self._test_task(R"""
        version 1.0
        task test_basename_prefix {
            Array[String] env = ["key1=value1", "key2=value2", "key3=value3"]
            Array[Int] env2 = [1, 2, 3]
            command {}
            output {
                Array[String] ans = [
                    basename("file.txt"),
                    basename("to/file.txt"),
                    basename("/path/to/file.txt"),
                    basename("/path/to/file.txt", ".txt"),
                    basename("/path/to/file.sam", ".txt")
                ]
                Array[String] env_param = prefix("-e ", env)
                Array[String] env2_param = prefix("-f ", env2)
            }
        }
        """)
        self.assertEqual(
            outputs,
            {
                "ans": ["file.txt", "file.txt", "file.txt", "file", "file.sam"],
                "env_param": ["-e key1=value1", "-e key2=value2", "-e key3=value3"],
                "env2_param": ["-f 1", "-f 2", "-f 3"],
            },
        )

    def test_select(self):
        outputs = self._test_task(
            R"""
        version 1.0
        task test_select {
            input {
                Array[Int] one
                Array[Int]? two
                Array[Int]? three
            }
            command {}
            output {
                Array[Int] first1 = select_first([one, two, three])
                Array[Int] first2 = select_first([two, three])
                Array[Int] first3 = select_first([three, two])
                Array[Int] first4 = select_first([two])

                Array[Array[Int]] all1 = select_all([one, two, three])
                Array[Array[Int]] all2 = select_all([three])
            }
        }
        """,
            {"one": [1], "two": [2]},
        )
        self.assertEqual(
            outputs,
            {
                "first1": [1],
                "first2": [2],
                "first3": [2],
                "first4": [2],
                "all1": [[1], [2]],
                "all2": [],
            },
        )
        outputs = self._test_task(
            R"""
        version 1.0
        task test_select {
            input {
                Array[Int]? one
            }
            command {}
            output {
                Array[Int] bogus = select_first([one])
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )
        self.assertTrue("given empty or all-null array" in str(outputs))
        outputs = self._test_task(
            R"""
        version 1.0
        task test_select {
            input {
            }
            command {}
            output {
                Int bogus = select_first([])
            }
        }
        """,
            expected_exception=WDL.Error.IndeterminateType,
        )
        outputs = self._test_task(
            R"""
        version 1.0
        task test_select {
            input {
            }
            command {}
            output {
                Array[Int] bogus = select_all([])
            }
        }
        """,
            expected_exception=WDL.Error.IndeterminateType,
        )

    def test_select_first_default(self):
        self.assertEqual(str(self._eval_expr("select_first([], 5)", version="1.2")), "5")
        self.assertEqual(str(self._eval_expr("select_first([None], 5)", version="1.2")), "5")
        self.assertEqual(str(self._eval_expr("select_first([3], 5)", version="1.2")), "3")
        self.assertEqual(str(self._eval_expr("select_first([3], 5.0)", version="1.2")), "3.000000")
        self.assertEqual(
            str(self._eval_expr('select_first([None], "fallback")', version="1.2")),
            '"fallback"',
        )
        with self.assertRaises(WDL.Error.WrongArity):
            self._eval_expr("select_first([], 5)", version="1.1")
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._eval_expr("select_first([], None)", version="1.2")
        with self.assertRaises(WDL.Error.EvalError):
            self._eval_expr("select_first([if false then 1 else None])", version="1.2")

    def test_select_first_default_unify(self):
        self.assertEqual(str(self._infer_expr_type("select_first([1])", version="1.2")), "Int")
        with self.assertRaises(WDL.Error.WrongArity):
            self._infer_expr_type("select_first()", version="1.2")
        with self.assertRaises(WDL.Error.WrongArity):
            self._infer_expr_type("select_first([1], 2, 3)", version="1.2")
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._infer_expr_type("select_first(1)", version="1.2")
        optional_array_env = WDL.Env.Bindings().bind(
            "xs", WDL.Type.Array(WDL.Type.Int(), optional=True)
        )
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._infer_expr_type("select_first(xs)", type_env=optional_array_env, version="1.2")
        with self.assertRaises(WDL.Error.IndeterminateType):
            self._infer_expr_type("select_first([])", version="1.2")
        with self.assertRaises(WDL.Error.IndeterminateType):
            self._infer_expr_type('select_first([], read_json("x.json"))', version="1.2")

        self.assertEqual(
            str(self._infer_expr_type("select_first([1], 2.0)", version="1.2")), "Float"
        )
        self.assertEqual(
            str(self._infer_expr_type("select_first([1.0], 2)", version="1.2")), "Float"
        )
        self.assertEqual(
            str(self._infer_expr_type('select_first([], "fallback")', version="1.2")), "String"
        )
        self.assertEqual(
            str(self._infer_expr_type('select_first(["x"], 2)', version="1.2")), "String"
        )
        self.assertEqual(
            str(self._infer_expr_type("select_first([[1]], [2.0])", version="1.2")),
            "Array[Float]+",
        )
        self.assertEqual(
            str(self._eval_expr("select_first([], [2.0])", version="1.2")),
            "[2.000000]",
        )
        with self.assertRaises(WDL.Error.IndeterminateType):
            self._infer_expr_type('select_first([[1]], {"a": 1})', version="1.2")

    def test_sub(self):
        outputs = self._test_task(R"""
        version 1.0
        task test_sub {
            command {}
            output {
                String chocolike = "I like chocolate when it's late"
                String chocolove = sub(chocolike, "like", "love") # I love chocolate when it's late
                String chocoearly = sub(chocolike, "late", "early") # I like chocoearly when it's early
                String chocolate = sub(chocolike, "late$", "early") # I like chocolate when it's early
                String chocoearlylate = sub(chocolike, "[^ ]late", "early") # I like chocearly when it's late
                String choco4 = sub(chocolike, " [[:alpha:]]{4} ", " 4444 ") # I 4444 chocolate 4444 it's late
            }
        }
        """)
        self.assertEqual(
            outputs,
            {
                "chocolike": "I like chocolate when it's late",
                "chocolove": "I love chocolate when it's late",
                "chocoearly": "I like chocoearly when it's early",
                "chocolate": "I like chocolate when it's early",
                "chocoearlylate": "I like chocearly when it's late",
                "choco4": "I 4444 chocolate 4444 it's late",
            },
        )
        outputs = self._test_task(R"""
        task example {
            input {
                String input_file = "my_input_file.bam"
                String output_file_name = sub(input_file, "\\.bam$", ".index") # my_input_file.index
            }
            command {
                echo "I want an index instead" > ${output_file_name}
            }
            output {
                File outputFile = output_file_name
            }
        }
        """)
        self.assertTrue(outputs["outputFile"].endswith("my_input_file.index"))
        outputs = self._test_task(
            R"""
        task bogus {
            command {}
            output {
                String bogus = sub("foo", "(()", "bar")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

    def test_find_matches(self):
        self.assertEqual(self._eval_expr('find("hello world", "e..o")', version="1.2").json, "ello")
        self.assertIsNone(self._eval_expr('find("hello world", "goodbye")', version="1.2").json)
        self.assertEqual(
            self._eval_expr(R"""find("hello\tBob", "\\t")""", version="1.2").json, "\t"
        )
        self.assertEqual(self._eval_expr('find("aaab", "a+")', version="1.2").json, "aaa")
        self.assertEqual(
            self._eval_expr('find("abc123", "[[:digit:]]+")', version="1.2").json,
            "123",
        )

        self.assertTrue(self._eval_expr('matches("sample_R1.fastq", "_R1")', version="1.2").json)
        self.assertFalse(
            self._eval_expr('matches("sample_R1.fastq", "\\\\.(gz|zip|zstd)")', version="1.2").json
        )
        self.assertTrue(self._eval_expr('matches("abc123", "^a.+3$")', version="1.2").json)
        self.assertFalse(self._eval_expr('matches("abc123", "^a.+2$")', version="1.2").json)

        with self.assertRaises(WDL.Error.EvalError):
            self._eval_expr('find("foo", "(()")', version="1.2")
        with self.assertRaises(WDL.Error.EvalError):
            self._eval_expr('matches("foo", "(()")', version="1.2")
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._eval_expr('find("foo", {"a": 1})', version="1.2")
        with self.assertRaises(WDL.Error.NoSuchFunction):
            self._eval_expr('find("hello world", "e..o")', version="1.1")
        with self.assertRaises(WDL.Error.NoSuchFunction):
            self._eval_expr('matches("sample_R1.fastq", "_R1")', version="1.1")

    def test_flatten(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            Array[Array[Int]] ai2D = [[1, 2, 3], [1], [21, 22]]
            Array[Array[String]] af2D = [["/tmp/X.txt"], ["/tmp/Y.txt", "/tmp/Z.txt"], []]
            Array[Array[Pair[Float,String]]] aap2D = [[(0.1, "mouse")], [(3, "cat"), (15, "dog")]]
            command {}
            output {
                Array[Int] ai = flatten(ai2D)
                Array[String] af = flatten(af2D)
                Array[Pair[Float,String]] ap = flatten(aap2D)
            }
        }
        """)
        self.assertEqual(outputs["ai"], [1, 2, 3, 1, 21, 22])
        self.assertEqual(outputs["af"], ["/tmp/X.txt", "/tmp/Y.txt", "/tmp/Z.txt"])
        self.assertEqual(
            outputs["ap"],
            [
                {"left": 0.1, "right": "mouse"},
                {"left": 3, "right": "cat"},
                {"left": 15, "right": "dog"},
            ],
        )

    def test_size(self):
        with open(os.path.join(self._dir, "alyssa.txt"), "w") as outfile:
            outfile.write("Alyssa\n")
        with open(os.path.join(self._dir, "ben.txt"), "w") as outfile:
            outfile.write("Ben\n")
        os.makedirs(os.path.join(self._dir, "dir1"))
        os.makedirs(os.path.join(self._dir, "dir2", "sub"))
        with open(os.path.join(self._dir, "dir1", "alice.txt"), "w") as outfile:
            outfile.write("Alice\n")
        with open(os.path.join(self._dir, "dir1", "ignored_link"), "w") as outfile:
            outfile.write("ignored\n")
        # Directory size excludes symlink entries inside the directory, even when the symlink
        # target is a regular file that would otherwise be countable.
        os.symlink(
            os.path.join(self._dir, "dir1", "ignored_link"),
            os.path.join(self._dir, "dir1", "link"),
        )
        with open(os.path.join(self._dir, "dir2", "sub", "bob.txt"), "w") as outfile:
            outfile.write("Bob\n")
        outputs = self._test_task(
            R"""
        version 1.2
        struct PathStruct {
            File file
            Directory dir
            String name
        }
        task hello {
            Array[File] files
            File? nullfile
            Directory dir1
            Directory dir2
            Directory? nulldir
            Float dir_child_size = size(join_paths(dir1, "alice.txt"), "B")
            Directory input_subdir = join_paths(dir2, "sub")
            Float input_subdir_size = size(input_subdir, "B")
            File? missing_input_child = join_paths(dir1, "missing.txt")
            Directory? missing_input_subdir = join_paths(dir2, "missing_subdir")
            Array[File]? maybe_files = None
            Map[String, Pair[Int, File?]] nested_files = {
                "a": (10, files[0]),
                "b": (50, nullfile)
            }
            Array[Pair[String, Directory?]] nested_dirs = [("a", dir1), ("b", nulldir)]
            Map[File, Int] file_keys = {files[0]: 1, files[1]: 2}
            PathStruct path_struct = PathStruct {
                file: files[0],
                dir: dir2,
                name: "paths"
            }
            Array[Float] sizes_ = [
                size(files[0]),
                size(files),
                size(files[0], "MB"),
                size(files[0], "MiB")
            ]
            command {
                cat ~{sep=' ' files} > alyssa_ben.txt
            }
            output {
                Array[Float] sizes = flatten([sizes_, [size(files, "GB"), size(files, "Gi")]])
                Float size2 = size("alyssa_ben.txt", "KiB")
                Float legacy_array_string_size = size(["alyssa_ben.txt"], "B")
                Float nosize1 = size(nullfile)
                Float nosize2 = size([files[0], nullfile])
                Float dirsize1 = size(dir1, "B")
                Float dirsizes = size([dir1, dir2], "B")
                Float nodirsize = size(nulldir)
                Float nested_files_size = size(nested_files, "B")
                Float nested_dirs_size = size(nested_dirs, "B")
                Float file_keys_size = size(file_keys, "B")
                Float maybe_files_size = size(maybe_files)
                Float path_struct_size = size(path_struct, "B")
                Float duplicate_file_size = size([files[0], files[0]], "B")
                Float duplicate_dir_size = size([dir1, dir1], "B")
                Float dir_child_size_out = dir_child_size
                Float input_subdir_size_out = input_subdir_size
                File? missing_input_child_out = missing_input_child
                Directory? missing_input_subdir_out = missing_input_subdir
            }
        }
        """,
            {
                "files": [
                    os.path.join(self._dir, "alyssa.txt"),
                    os.path.join(self._dir, "ben.txt"),
                ],
                "dir1": os.path.join(self._dir, "dir1"),
                "dir2": os.path.join(self._dir, "dir2"),
            },
        )
        self.assertEqual(len(outputs["sizes"]), 6)
        self.assertEqual(outputs["sizes"][0], 7)
        self.assertEqual(outputs["sizes"][1], 11)
        self.assertAlmostEqual(outputs["sizes"][2], 7 / 1000000)
        self.assertAlmostEqual(outputs["sizes"][3], 7 / 1048576)
        self.assertAlmostEqual(outputs["sizes"][4], 11 / 1000000000)
        self.assertAlmostEqual(outputs["sizes"][5], 11 / 1073741824)
        self.assertAlmostEqual(outputs["size2"], 11 / 1024)
        self.assertEqual(outputs["legacy_array_string_size"], 11)
        self.assertEqual(outputs["nosize1"], 0)
        self.assertEqual(outputs["nosize2"], 7)
        # 6 bytes from alice.txt + 8 bytes from ignored_link; dir1/link itself contributes 0.
        self.assertEqual(outputs["dirsize1"], 14)
        self.assertEqual(outputs["dirsizes"], 18)
        self.assertEqual(outputs["nodirsize"], 0)
        self.assertEqual(outputs["nested_files_size"], 7)
        self.assertEqual(outputs["nested_dirs_size"], 14)
        self.assertEqual(outputs["file_keys_size"], 11)
        self.assertEqual(outputs["maybe_files_size"], 0)
        self.assertEqual(outputs["path_struct_size"], 11)
        # size() sums path occurrences in the WDL value; it doesn't deduplicate equal paths.
        self.assertEqual(outputs["duplicate_file_size"], 14)
        self.assertEqual(outputs["duplicate_dir_size"], 28)
        self.assertEqual(outputs["dir_child_size_out"], 6)
        self.assertEqual(outputs["input_subdir_size_out"], 4)
        self.assertIsNone(outputs["missing_input_child_out"])
        self.assertIsNone(outputs["missing_input_subdir_out"])

        outputs = self._test_task(
            R"""
        version 1.2
        task hello {
            input {
                Directory dir1
                Directory dir2
                File? missing_input_child = join_paths(dir1, "missing.txt")
                Directory? missing_input_subdir = join_paths(dir2, "missing_subdir")
            }
            File? missing_private_child = join_paths(dir1, "missing_private.txt")
            Directory? missing_private_subdir = join_paths(dir2, "missing_private_subdir")
            command {}
            output {
                File? missing_input_child_out = missing_input_child
                Directory? missing_input_subdir_out = missing_input_subdir
                File? missing_private_child_out = missing_private_child
                Directory? missing_private_subdir_out = missing_private_subdir
            }
        }
        """,
            {
                "dir1": os.path.join(self._dir, "dir1"),
                "dir2": os.path.join(self._dir, "dir2"),
            },
        )
        self.assertIsNone(outputs["missing_input_child_out"])
        self.assertIsNone(outputs["missing_input_subdir_out"])
        self.assertIsNone(outputs["missing_private_child_out"])
        self.assertIsNone(outputs["missing_private_subdir_out"])

        exn = self._test_task(
            R"""
        version 1.2
        task hello {
            input {
                Directory dir1
                File missing_input_child = join_paths(dir1, "missing.txt")
            }
            command {}
        }
        """,
            {"dir1": os.path.join(self._dir, "dir1")},
            expected_exception=WDL.Error.InputError,
        )
        self.assertEqual(getattr(exn, "job_id", None), "decl-missing_input_child")

        exn = self._test_task(
            R"""
        version 1.2
        task hello {
            Directory dir1
            File missing_input_child = join_paths(dir1, "missing.txt")
            command {}
        }
        """,
            {"dir1": os.path.join(self._dir, "dir1")},
            expected_exception=WDL.Error.InputError,
        )
        self.assertEqual(getattr(exn, "job_id", None), "decl-missing_input_child")

        self._test_task(
            R"""
        version 1.0
        task hello {
            Float x = size("/etc/passwd")
            command {}
        }
        """,
            expected_exception=WDL.Error.InputError,
        )

        self._test_task(
            R"""
        version 1.0
        task hello {
            command {}
            output {
                Float x = size("/etc/passwd")
            }
        }
        """,
            expected_exception=WDL.runtime.task.OutputError,
        )

        self._test_task(
            R"""
        version 1.0
        task hello {
            command {
                touch foo
            }
            output {
                Float x = size("foo", "bogus")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

    def test_glob(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command {
                mkdir stuff
                cd stuff
                touch foo bar bas baz bat
            }
            output {
                Array[Array[File]] globs = [
                    glob("stuff/foo"),
                    glob("./stuff/b*"),
                    glob("*/*"),
                    glob("bogus")
                ]
                File f1 = glob("stuff/foo")[0]
                String s1 = read_string(f1)
            }
        }
        """)
        self.assertEqual(len(outputs["globs"][0]), 1)
        self.assertTrue(outputs["globs"][0][0].endswith("/foo"))
        self.assertEqual(len(outputs["globs"][1]), 4)
        self.assertTrue(outputs["globs"][1][0].endswith("/bar"))
        self.assertTrue(outputs["globs"][1][1].endswith("/bas"))
        self.assertTrue(outputs["globs"][1][2].endswith("/bat"))
        self.assertTrue(outputs["globs"][1][3].endswith("/baz"))
        self.assertEqual(len(outputs["globs"][2]), 5)
        self.assertTrue(outputs["globs"][2][4].endswith("/foo"))
        self.assertEqual(len(outputs["globs"][3]), 0)
        for g in outputs["globs"] + [[outputs["f1"]]]:
            for fn in g:
                assert os.path.isfile(fn), fn
        self.assertTrue(outputs["f1"].endswith("/foo"))

        self._test_task(
            R"""
        version 1.0
        task hello {
            command {}
            output {
                Array[File] filez = glob("/etc/passwd")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        self._test_task(
            R"""
        version 1.0
        task hello {
            command {}
            output {
                Array[File] filez = glob("../../etc/passwd")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

    def test_concat(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            String foo = "foo"
            String? bar = "bar"
            String? none
            Int i = 42
            Int? i_none
            command {
                echo ~{foo + bar}
                echo ~{foo + bar + i}
                echo ~{foo + none}
                echo ~{none + bar}
                echo ~{foo + none + bar}
                echo ~{foo + bar + none}
                echo ~{foo + bar + i_none}
                echo ~{sep='::' [foo,bar,none]}
                echo ~{sep='::' [foo,none,bar]}
            }
            output {
                String s = read_string(stdout())
            }
        }
        """)
        self.assertEqual(outputs["s"], "foobar\nfoobar42\n\n\n\n\n\nfoo::bar::\nfoo::::bar")

    def test_read(self):
        with open(os.path.join(self._dir, "strings.txt"), "w") as outfile:
            outfile.write("foo\nbar\nbas\n")
        outputs = self._test_task(
            R"""
        version 1.0
        task hello {
            input {
                File strings
            }
            String i1 = read_string(strings)
            Array[String] i2 = read_lines(strings)
            File strings2 = write_lines(i2)

            command {
                echo Alyssa
                echo Ben
                echo 42 > fortytwo.txt
                echo 6.02e23 > mole.txt
                echo true > true.txt
                echo false > false.txt
                echo -e "key1\tvalue1" > map.txt
                echo -e "key2\tvalue2" >> map.txt
                echo -e "..\ttricky" >> map.txt
                printf 'line1\r\n\r\nline3\r\n' > crlf_lines.txt
                printf 'k1\tv1\r\nk2\tv2\r\n' > crlf_map.txt
            }
            output {
                String i_strings_string = i1
                String o_strings_string = read_string(strings)
                String o_names_string = read_string(stdout())
                Array[String] i_strings_lines = i2
                Array[String] o_strings_lines = read_lines(strings2)
                Array[String] o_names_lines = read_lines(stdout())
                Array[String] o_crlf_lines = read_lines("crlf_lines.txt")
                Array[Array[String]] o_crlf_tsv = read_tsv("crlf_map.txt")
                Map[String,String] o_crlf_map = read_map("crlf_map.txt")
                Int o_fortytwo = read_int("fortytwo.txt")
                Float o_mole = read_float("mole.txt")
                Array[Boolean] o_boolean = [read_boolean("true.txt"), read_boolean("false.txt")]
                Map[String,String] o_map = read_map("map.txt")
            }
        }
        """,
            {"strings": os.path.join(self._dir, "strings.txt")},
        )
        self.assertEqual(outputs["i_strings_string"], "foo\nbar\nbas")
        self.assertEqual(outputs["o_strings_string"], "foo\nbar\nbas")
        self.assertEqual(outputs["o_names_string"], "Alyssa\nBen")
        self.assertEqual(outputs["i_strings_lines"], ["foo", "bar", "bas"])
        self.assertEqual(outputs["o_strings_lines"], ["foo", "bar", "bas"])
        self.assertEqual(outputs["o_names_lines"], ["Alyssa", "Ben"])
        self.assertEqual(outputs["o_crlf_lines"], ["line1", "", "line3"])
        self.assertEqual(outputs["o_crlf_tsv"], [["k1", "v1"], ["k2", "v2"]])
        self.assertEqual(outputs["o_crlf_map"], {"k1": "v1", "k2": "v2"})
        self.assertEqual(outputs["o_fortytwo"], 42)
        self.assertEqual(outputs["o_boolean"], [True, False])
        self.assertEqual(outputs["o_map"], {"key1": "value1", "key2": "value2", "..": "tricky"})

    def test_read_tsv_headers(self):
        outputs = self._test_task(R"""
        version 1.2
        struct Sample {
            String name
            Int lane
            String barcode
        }
        task test {
            command <<<
                echo -e "name\tlane\tbarcode" >> samples.tsv
                echo -e "Alice\t3\tGATTACA" >> samples.tsv
                echo -e "Bob\t4\tTGTAATC" >> samples.tsv
                echo -e "Alice\t3\tGATTACA" >> no_header.tsv
                echo -e "Bob\t4\tTGTAATC" >> no_header.tsv
                touch empty.tsv
            >>>
            output {
                Array[Array[String]] table1 = read_tsv("samples.tsv")
                Array[Sample] samples1 = read_tsv("samples.tsv", true)
                Array[Sample] samples2 = read_tsv("no_header.tsv", false, ["name", "lane", "barcode"])
                Array[Sample] samples3 = read_tsv("samples.tsv", true, ["name", "lane", "barcode"])
                Map[String, String] first = read_tsv("samples.tsv", true)[0]
                Array[Sample] empty = read_tsv("empty.tsv", true)
            }
        }
        """)
        samples = [
            {"name": "Alice", "lane": 3, "barcode": "GATTACA"},
            {"name": "Bob", "lane": 4, "barcode": "TGTAATC"},
        ]
        self.assertEqual(
            outputs["table1"],
            [["name", "lane", "barcode"], ["Alice", "3", "GATTACA"], ["Bob", "4", "TGTAATC"]],
        )
        self.assertEqual(outputs["samples1"], samples)
        self.assertEqual(outputs["samples2"], samples)
        self.assertEqual(outputs["samples3"], samples)
        self.assertEqual(outputs["first"], {"name": "Alice", "lane": "3", "barcode": "GATTACA"})
        self.assertEqual(outputs["empty"], [])

        self._test_task(
            R"""
        version 1.1
        task test {
            command {}
            output {
                Array[Array[String]] table = read_tsv("samples.tsv", false)
            }
        }
        """,
            expected_exception=WDL.Error.WrongArity,
        )

        self._test_task(
            R"""
        version 1.2
        task test {
            command {}
            output {
                Array[Array[String]] table = read_tsv("samples.tsv", false)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        self._test_task(
            R"""
        version 1.2
        task test {
            input {
                Boolean header
            }
            command {}
            output {
                Array[Array[String]] table = read_tsv("samples.tsv", header)
            }
        }
        """,
            {"header": False},
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        self._test_task(
            R"""
        version 1.2
        task test {
            command <<<
                echo -e "Alice\t3" > samples.tsv
            >>>
            output {
                Array[Map[String, String]] samples = read_tsv("samples.tsv", false, ["name", "name"])
            }
        }
        """,
            expected_exception=WDL.Error.InputError,
        )

    def test_write_tsv_unit_branches(self):
        class LocalStdLib(WDL.StdLib.Base):
            def _devirtualize_filename(self, filename: str, directory: bool = False) -> str:
                return filename

            def _virtualize_filename(self, filename: str) -> str:
                return filename

        def infer(expr: str, version: str = "1.2"):
            stdlib = LocalStdLib(version, write_dir=self._dir)
            parsed = WDL.parse_expr(expr, version=version).infer_type(WDL.Env.Bindings(), stdlib)
            return parsed, stdlib

        with self.assertRaises(WDL.Error.WrongArity):
            infer("write_tsv()")
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            infer("write_tsv([1])")
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            infer('write_tsv(["x"], true, ["h"])')
        with self.assertRaises(WDL.Error.WrongArity):
            infer('write_tsv([["x"]], true)')
        with self.assertRaises(WDL.Error.WrongArity):
            infer('write_tsv([["x"]], true, ["h"])', version="1.1")
        with self.assertRaises(WDL.Error.WrongArity):
            infer('write_tsv([["x"]], true, ["h"], "extra")')
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            infer('write_tsv([["x"]], false, ["h"])')
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            infer('write_tsv([["x"]], "true", ["h"])')
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            infer('write_tsv([["x"]], true, [object {x: "y"}])')

        expr, stdlib = infer(
            'write_tsv([["one", "two"], ["un", "deux"]], true, ["first", "second"])'
        )
        out = expr.eval(WDL.Env.Bindings(), stdlib)
        with open(out.value) as infile:
            self.assertEqual(infile.read(), "first\tsecond\none\ttwo\nun\tdeux\n")

        expr, stdlib = infer('write_tsv([["one"], ["un", "deux"]], true, ["first"])')
        with self.assertRaises(WDL.Error.EvalError):
            expr.eval(WDL.Env.Bindings(), stdlib)

    def test_write_tsv_headers(self):
        class LocalStdLib(WDL.StdLib.Base):
            def _devirtualize_filename(self, filename: str, directory: bool = False) -> str:
                return filename

            def _virtualize_filename(self, filename: str) -> str:
                return filename

        def eval_task_outputs(wdl: str, inputs: dict):
            doc = WDL.parse_document(wdl)
            doc.typecheck()
            task = doc.tasks[0]
            env = WDL.values_from_json(inputs, task.available_inputs, task.required_inputs)
            stdlib = LocalStdLib(doc.effective_wdl_version, write_dir=self._dir)
            ans = {}
            for decl in task.outputs:
                ans[decl.name] = decl.expr.eval(env, stdlib).coerce(decl.type)
            outputs = WDL.Env.Bindings()
            for name, value in ans.items():
                outputs = outputs.bind(name, value)
            return WDL.values_to_json(outputs)

        outputs = eval_task_outputs(
            R"""
        version 1.2
        struct Sample {
            String name
            Int lane
            Boolean pass
        }
        task test {
            input {
                Array[Sample] samples
            }
            command {}
            output {
                Array[Array[String]] structs_default = read_tsv(write_tsv(samples))
                Array[Array[String]] structs_no_header = read_tsv(write_tsv(samples, false))
                Array[Array[String]] structs_header = read_tsv(write_tsv(samples, true))
                Array[Array[String]] structs_user_header = read_tsv(
                    write_tsv(samples, true, ["sample_name", "lane_no", "qc"])
                )
                Array[Array[String]] structs_ignored_header = read_tsv(
                    write_tsv(samples, false, ["ignored"])
                )
            }
        }
        """,
            {
                "samples": [
                    {"name": "Alice", "lane": 3, "pass": True},
                    {"name": "Bob", "lane": 4, "pass": False},
                ]
            },
        )
        rows = [["Alice", "3", "true"], ["Bob", "4", "false"]]
        self.assertEqual(outputs["structs_default"], rows)
        self.assertEqual(outputs["structs_no_header"], rows)
        self.assertEqual(outputs["structs_header"], [["name", "lane", "pass"]] + rows)
        self.assertEqual(
            outputs["structs_user_header"],
            [["sample_name", "lane_no", "qc"]] + rows,
        )
        self.assertEqual(outputs["structs_ignored_header"], rows)

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_document(R"""
        version 1.1
        struct Sample {
            String name
        }
        task test {
            input {
                Array[Sample] samples
            }
            command {}
            output {
                File table = write_tsv(samples)
            }
        }
        """).typecheck()

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_document(R"""
        version 1.2
        struct Sample {
            String name
        }
        task test {
            input {
                Array[Sample] samples
            }
            command {}
            output {
                File table = write_tsv(samples, "true")
            }
        }
        """).typecheck()

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_document(R"""
        version 1.2
        struct Sample {
            String name
        }
        task test {
            input {
                Array[Sample] samples
            }
            command {}
            output {
                File table = write_tsv(samples, true, [object {x: "y"}])
            }
        }
        """).typecheck()

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_document(R"""
        version 1.2
        struct Bad {
            Map[String, String] xs
        }
        task test {
            input {
                Array[Bad] rows
            }
            command {}
            output {
                File table = write_tsv(rows)
            }
        }
        """).typecheck()

        with self.assertRaises(WDL.Error.EvalError):
            eval_task_outputs(
                R"""
        version 1.2
        struct Sample {
            String name
            Int lane
        }
        task test {
            input {
                Array[Sample] samples
            }
            command {}
            output {
                Array[Array[String]] table = read_tsv(write_tsv(samples, true, ["name"]))
            }
        }
        """,
                {"samples": [{"name": "Alice", "lane": 3}]},
            )

    def test_read_json(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command <<<
                echo '{"foo": "bar", "bas": "baz"}' > object.json
                echo '[1, 2, 3, 4, 5]' > list.json
                echo 42 > int.json
                echo 3.14159 > float.json
                echo true > bool.json
                echo null > null.json
                echo '{"out": ["Element 1", "Element 2"]}' > out.txt
            >>>
            output {
                Map[String,String] map = read_json("object.json")
                Array[Int] array = read_json("list.json")
                Int int = read_json("int.json")
                Float float = read_json("float.json")
                Boolean bool = read_json("bool.json")
                String? null = read_json("null.json")

                # issue #320
                String baz1 = read_json("object.json")["bas"]
                Int three = read_json("list.json")[2]
                Array[String] out1 = read_json('out.txt')["out"]
                String out2 = read_json('out.txt')["out"][1]
            }
        }
        """)
        self.assertEqual(outputs["map"], {"foo": "bar", "bas": "baz"})
        self.assertEqual(outputs["array"], [1, 2, 3, 4, 5])
        self.assertEqual(outputs["int"], 42)
        self.assertAlmostEqual(outputs["float"], 3.14159)
        self.assertEqual(outputs["bool"], True)
        self.assertEqual(outputs["null"], None)
        self.assertEqual(outputs["baz1"], "baz")
        self.assertEqual(outputs["three"], 3)
        self.assertEqual(outputs["out1"], ["Element 1", "Element 2"])
        self.assertEqual(outputs["out2"], "Element 2")

        outputs = self._test_task(R"""
        version 1.0
        task test {
            command <<<
                echo '["foo", "bar"]'
            >>>
            output {
                Array[String] my_array = read_json(stdout())
            }
        }
        """)
        self.assertEqual(outputs["my_array"], ["foo", "bar"])

        self._test_task(
            R"""
        version 1.0
        task test {
            command <<<
                echo '{"foo":"bar"}'
            >>>
            output {
                Array[String] my_array = read_json(stdout())
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        self._test_task(
            R"""
        version 1.0
        struct Point {
            Int x
            Int y
        }
        task test {
            input {
                String json = '{"foo": "bar"}'
            }
            Map[Point, String] my_map = read_json(write_lines([json]))
            command <<<
            >>>
            output {
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        outputs = self._test_task(R"""
        version 1.0
        task test {
            command <<<
                echo '{"foo":"bar"}'
            >>>
            output {
                Map[String, String] my_map = read_json(stdout())
            }
        }
        """)
        self.assertEqual(outputs["my_map"], {"foo": "bar"})

        self._test_task(
            R"""
        version 1.0
        task test {
            command <<<
                echo '["foo", "bar"]'
            >>>
            output {
                Map[String, String] my_map = read_json(stdout())
            }
        }
        """,
            expected_exception=WDL.Error.InputError,
        )

        self._test_task(
            R"""
        version 1.0
        task test {
            command <<<
                echo '{"foo":"bar"}'
            >>>
            output {
                String baz = read_json(stdout())["baz"]
            }
        }
        """,
            expected_exception=WDL.Error.OutOfBounds,
        )

    def test_read_map_ints(self):
        outputs = self._test_task(R"""
        version 1.0
        task test {
            command <<<
                python <<CODE
                for i in range(3):
                    print("key_{idx}\t{idx}".format(idx=i))
                CODE
            >>>
            output {
                Map[String, Int] my_ints = read_map(stdout())
            }
            runtime {
                docker: "continuumio/miniconda3"
            }
        }
        """)
        self.assertEqual(outputs["my_ints"], {"key_0": 0, "key_1": 1, "key_2": 2})

    def test_struct_from_read(self):
        # initialize struct from read_{map,object[s],json}

        alice = {"name": "Alice", "lane": 3, "barcode": "GATTACA"}
        bob = {"name": "Bob", "lane": 4, "barcode": "TGTAATC"}
        samplesheet2 = [alice, bob]

        outputs = self._test_task(R"""
        version 1.0
        struct Sample {
            String name
            Int lane
            String barcode
        }
        task test {
            command <<<
                echo -e "name\tAlice" >> alice.txt
                echo -e "lane\t3" >> alice.txt
                echo -e "barcode\tGATTACA" >> alice.txt
            >>>
            output {
                Sample alice = read_map("alice.txt")
            }
        }
        """)
        self.assertEqual(outputs["alice"], alice)

        outputs = self._test_task(R"""
        version 1.0
        struct Sample {
            String name
            Int lane
            String barcode
        }
        task test {
            command <<<
                echo -e "name\tlane\tbarcode" >> alice.txt
                echo -e "Alice\t3\tGATTACA" >> alice.txt
                cp alice.txt samplesheet2.txt
                echo -e "Bob\t4\tTGTAATC" >> samplesheet2.txt
                touch empty
            >>>
            output {
                Sample alice = read_object("alice.txt")
                Array[Sample] samplesheet2 = read_objects("samplesheet2.txt")
                Array[Sample] empty = read_objects("empty")
            }
        }
        """)
        self.assertEqual(outputs["alice"], alice)
        self.assertEqual(outputs["samplesheet2"], samplesheet2)
        self.assertEqual(outputs["empty"], [])

        # optional field coercion
        outputs = self._test_task(R"""
        version 1.0
        struct Sample {
            String name
            Int lane
            String barcode
            String? lab
        }
        task test {
            command <<<
                echo '{"name":"Alice","lane":3,"barcode":"GATTACA"}' >> alice.txt
                echo '[' >> samplesheet2.txt
                cat alice.txt >> samplesheet2.txt
                echo ',{"name":"Bob","lane":4,"barcode":"TGTAATC"}]' >> samplesheet2.txt
            >>>
            output {
                Sample alice = read_json("alice.txt")
                Array[Sample] samplesheet2 = read_json("samplesheet2.txt")
            }
        }
        """)
        self.assertEqual(dict(**alice, lab=None), outputs["alice"])
        self.assertEqual([dict(**it, lab=None) for it in samplesheet2], outputs["samplesheet2"])

        # struct-array-struct JSON nesting
        outputs = self._test_task(R"""
        version 1.0
        struct Sample {
            String name
            Int lane
            String barcode
            String? lab
        }
        struct MultiSample {
            Array[Sample] samples
        }
        task test {
            command <<<
                echo '{"name":"Alice","lane":3,"barcode":"GATTACA"}' >> alice.txt
                echo '{"samples":[' >> samplesheet2.txt
                cat alice.txt >> samplesheet2.txt
                echo ',{"name":"Bob","lane":4,"barcode":"TGTAATC","lab":"Biohub"}]}' >> samplesheet2.txt
            >>>
            output {
                Sample alice = read_json("alice.txt")
                MultiSample samplesheet2 = read_json("samplesheet2.txt")
            }
        }
        """)
        self.assertEqual(dict(**alice, lab=None), outputs["alice"])
        self.assertEqual(
            {"samples": [dict(**alice, lab=None), dict(**bob, lab="Biohub")]},
            outputs["samplesheet2"],
        )

    def test_struct_from_read_json_with_extra_keys(self):
        outputs = self._test_task(R"""
        version 1.0
        struct Sample {
            String name
        }
        task test {
            command <<<
                echo '[
                    {"name": "Alice"},
                    {"name": "Rishi", "address": "10 Downing St", "city": "Westminster"},
                    {"name": "Harry", "address": "4 Privet Drive"}
                ]' > samples.json
            >>>
            output {
                Array[Sample] samples = read_json("samples.json")
            }
        }
        """)
        self.assertEqual(
            outputs["samples"], [{"name": "Alice"}, {"name": "Rishi"}, {"name": "Harry"}]
        )

        outputs = self._test_task(
            R"""
        version 1.0
        struct Sample {
            String name
        }
        task test {
            input {
                Array[Sample] samples
            }
            command {}
            output {
                Array[Sample] samples2 = read_json(write_json(samples))
            }
        }
        """,
            {
                "samples": [
                    {"name": "Alice"},
                    {"name": "Rishi", "address": "10 Downing St", "city": "Westminster"},
                    {"name": "Harry", "address": "4 Privet Drive"},
                ]
            },
        )
        self.assertEqual(
            outputs["samples2"], [{"name": "Alice"}, {"name": "Rishi"}, {"name": "Harry"}]
        )

    def test_issue524(self):
        # additional cases for struct initialization from read_json(), motivated by issue #524

        # explicit null value should be acceptable initializer for optional struct field
        outp = self._test_task(R"""
        version 1.0

        struct MyStruct {
            Int x
            String? y
        }

        task mytask {
            input {
            }

            command <<<
                cat > data.json <<EOL
                {
                    "x": 123,
                    "y": null
                }
                EOL
            >>>

            output {
                MyStruct data = read_json("data.json")
            }
        }
        """)
        self.assertEqual(outp["data"], {"x": 123, "y": None})
        # elaboration with a heterogeneous unification:
        outp = self._test_task(R"""
        version 1.0

        struct MyStruct {
            Float x
            String? y
            Array[Int?] z
        }

        task mytask {
            input {
            }

            command <<<
                cat > data.json <<EOL
                {
                    "x": 3.14159,
                    "y": null,
                    "z": [4,2,null]
                }
                EOL
            >>>

            output {
                MyStruct data = read_json("data.json")
            }
        }
        """)
        self.assertEqual(outp["data"], {"x": 3.14159, "y": None, "z": [4, 2, None]})
        # unusable null
        self._test_task(
            R"""
        version 1.0

        struct MyStruct {
            Float x
            String? y
            Array[Int?] z
        }

        task mytask {
            input {
            }

            command <<<
                cat > data.json <<EOL
                {
                    "x": "bogus",
                    "y": null,
                    "z": [4,2,null]
                }
                EOL
            >>>

            output {
                MyStruct data = read_json("data.json")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )
        # top-level null
        outp = self._test_task(R"""
        version 1.0

        struct MyStruct {
            Float x
            String? y
            Array[Int?] z
        }

        task mytask {
            input {
            }

            command <<<
                echo null > data.json
            >>>

            output {
                MyStruct? data = read_json("data.json")
            }
        }
        """)
        self.assertEqual(outp, {"data": None})
        # coercion failure -- required member missing
        self._test_task(
            R"""
        version 1.0

        struct MyStruct {
            Float x
            String? y
            Array[Int?] z
        }

        task mytask {
            input {
            }

            command <<<
                cat > data.json <<EOL
                {
                    "y": null,
                    "z": [4,2,null]
                }
                EOL
            >>>

            output {
                MyStruct data = read_json("data.json")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )
        # bad coercion to Map (key type)
        self._test_task(
            R"""
        version 1.0

        struct MyStruct {
            Float x
            String? y
            Array[Int?] z
        }

        task mytask {
            input {
            }

            command <<<
                cat > data.json <<EOL
                {
                    "x": 3.14159,
                    "z": [4,2,null]
                }
                EOL
            >>>

            output {
                Map[Float,String] data = read_json("data.json")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )
        # bad coercion to Map (value type)
        self._test_task(
            R"""
        version 1.0

        struct MyStruct {
            Float x
            String? y
            Array[Int?] z
        }

        task mytask {
            input {
            }

            command <<<
                cat > data.json <<EOL
                {
                    "x": 3.14159,
                    "z": [4,2,null]
                }
                EOL
            >>>

            output {
                Map[String,Float] data = read_json("data.json")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

    def test_issue538(self):
        # more struct init regression
        outp = self._test_task(R"""
        version 1.0
        struct StructWithMap { Map[String, String] map }
        task RoundTripJson {
            input {}
            StructWithMap example = object {
                map: { "foo":"bar", "fizz":"buzz" }
            }
            String dbg = read_string(write_json(example))
            command {}
            output {
                StructWithMap result = read_json(write_json(example))
            }
        }
        """)
        self.assertEqual(outp["result"], {"map": {"foo": "bar", "fizz": "buzz"}})

    def test_issue563(self):
        # still more struct init regression
        outp = self._test_task(
            R"""
        version 1.0
        struct Foo {
            Int x
        }
        task test {
            input {
                Foo? i
                Foo? j
            }
            command {}
            output {
                Array[Foo] out = select_all([i, j])
            }
        }
        """,
            {"j": {"x": 0}},
        )
        self.assertEqual(outp, {"out": [{"x": 0}]})

    def test_issue580(self):
        # error in nested struct
        try:
            self._test_task(R"""
            version 1.0

            struct Readgroup {
                String id
                String lib_id
                File R1
                File? R2
            }

            struct Sample {
                String id
                String? control
                String? gender
                Array[Readgroup] readgroups
            }

            struct SampleConfig {
                Array[Sample] samples
            }

            task mytask {
                input {
                }

                command <<<
                    cat > data.json <<EOL
                    {
                        "samples": [
                            {
                            "readgroups": [
                                {
                                "id": "rg1",
                                "R1": "tests/data/wgs1/R1.fq.gz",
                                "R1_md5": "b859d6dd76a6861ce7e9a978ae2e530e",
                                "R2": "tests/data/wgs1/R2.fq.gz",
                                "R2_md5": "986acc7bda0bf2ef55c52431f54fe3a9",
                                "lib_id": "lib1"
                                }
                            ],
                            "id": "wgs1-paired-end",
                            "control": null
                            },
                            {
                            "readgroups": [
                                {
                                "id": "rg1",
                                "R1": "tests/data/wgs2/wgs2-lib1_R1.fq.gz",
                                "R1_md5": "6fb02af910026041f9ea76cd28968732",
                                "R2": "tests/data/wgs2/wgs2-lib1_R2.fq.gz",
                                "R2_md5": "537ffc52342314d839e7fdd91bbdccd0",
                                "lib_id": "lib1"
                                },
                                {
                                "id": "rg2",
                                "R1": "tests/data/wgs2/wgs2-lib2_R1.fq.gz",
                                "R1_md5": "df64e84fdc9a2d7a9301f2aac0071aee",
                                "R2": "tests/data/wgs2/wgs2-lib2_R2.fq.gz",
                                "R2_md5": "47a65ad648ac08e802c07669629054ea",
                                "lib_id": "lib1"
                                }
                            ],
                            "id": "wgs2-paired-end",
                            "control": "wgs1-paired-end"
                            }
                        ]
                    }
                    EOL
                >>>

                output {
                    SampleConfig data = read_json("data.json")
                }
            }""")
        except Exception as exn:
            self.assertTrue(
                "unusable runtime struct initializer, no such member(s) in struct Readgroup: R1_md5 R2_md5"
                in str(exn)
            )

        # slightly simpler version covering a different exception handling path (#1)
        try:
            self._test_task(R"""
            version 1.0

            struct Readgroup {
                String id
                String lib_id
                File R1
                File? R2
            }

            struct Sample {
                String id
                String? control
                String? gender
                Readgroup readgroup
            }

            struct SampleConfig {
                Array[Sample] samples
            }

            task mytask {
                input {
                }

                command <<<
                    cat > data.json <<EOL
                    {
                        "samples": [
                            {
                            "readgroup":
                                {
                                "id": "rg1",
                                "R1": "tests/data/wgs1/R1.fq.gz",
                                "R2": "tests/data/wgs1/R2.fq.gz",
                                "lib_id": "lib1"
                                },
                            "id": "wgs1-paired-end",
                            "control": null
                            },
                            {
                            "readgroup":
                                {
                                "id": "rg1",
                                "R1": "tests/data/wgs2/wgs2-lib1_R1.fq.gz",
                                "R1_md5": "6fb02af910026041f9ea76cd28968732",
                                "R2": "tests/data/wgs2/wgs2-lib1_R2.fq.gz",
                                "R2_md5": "537ffc52342314d839e7fdd91bbdccd0",
                                "lib_id": "lib1"
                                },
                            "id": "wgs2-paired-end",
                            "control": "wgs1-paired-end"
                            }
                        ]
                    }
                    EOL
                >>>

                output {
                    SampleConfig data = read_json("data.json")
                }
            }""")
        except Exception as exn:
            self.assertTrue(
                "unusable runtime struct initializer, no such member(s) in struct Readgroup: R1_md5 R2_md5"
                in str(exn)
            )

        # slightly simpler version covering a different exception handling path (#2)
        try:
            self._test_task(R"""
            version 1.0

            struct Sample {
                String id
                String? control
                String? gender
                Int count
            }

            struct SampleConfig {
                Array[Sample] samples
            }

            task mytask {
                input {
                }

                command <<<
                    cat > data.json <<EOL
                    {
                        "samples": [
                            {
                            "id": "wgs1-paired-end",
                            "control": null,
                            "count": {"not":"a number"}
                            },
                            {
                            "id": "wgs2-paired-end",
                            "control": "wgs1-paired-end",
                            "count": 100
                            }
                        ]
                    }
                    EOL
                >>>

                output {
                    SampleConfig data = read_json("data.json")
                }
            }""")
        except Exception as exn:
            self.assertTrue("to initialize Int count member of struct Sample" in str(exn))

        # slightly simpler version covering a different exception handling path (#3)
        try:
            self._test_task(R"""
            version 1.0

            struct Sample {
                String id
                String? control
                String? gender
                Int count
            }

            struct SampleConfig {
                Array[Sample] samples
            }

            task mytask {
                input {
                }

                command <<<
                    cat > data.json <<EOL
                    {
                        "samples": [
                            {
                            "id": "wgs1-paired-end",
                            "control": null,
                            "count": "not a number"
                            },
                            {
                            "id": "wgs2-paired-end",
                            "control": "wgs1-paired-end",
                            "count": 100
                            }
                        ]
                    }
                    EOL
                >>>

                output {
                    SampleConfig data = read_json("data.json")
                }
            }""")
        except Exception as exn:
            self.assertTrue(
                "runtime type mismatch initializing Int count member of struct Sample" in str(exn)
            )

        # unifying arrays of structs with optional members
        outp = self._test_task(R"""
            version 1.0

            struct Readgroup {
                String id
                String lib_id
                String R1
                String R1_md5
                String? R2
                String? R2_md5
            }

            struct Sample {
                String id
                String? control
                String? gender
                Array[Readgroup] readgroups
            }

            struct SampleConfig {
                Array[Sample] samples
            }

            task mytask {
                input {
                }

                command <<<
                    cat > data.json <<EOL
                    {
                        "samples": [
                            {
                            "readgroups": [
                                {
                                "id": "rg1",
                                "R1": "tests/data/wgs1/R1.fq.gz",
                                "R1_md5": "b859d6dd76a6861ce7e9a978ae2e530e",
                                "R2": "tests/data/wgs1/R2.fq.gz",
                                "R2_md5": "986acc7bda0bf2ef55c52431f54fe3a9",
                                "lib_id": "lib1"
                                }
                            ],
                            "id": "wgs1-paired-end",
                            "control": null
                            },
                            {
                            "readgroups": [
                                {
                                "id": "rg1",
                                "R1": "tests/data/wgs2/wgs2-lib1_R1.fq.gz",
                                "R1_md5": "6fb02af910026041f9ea76cd28968732",
                                "R2": "tests/data/wgs2/wgs2-lib1_R2.fq.gz",
                                "R2_md5": "537ffc52342314d839e7fdd91bbdccd0",
                                "lib_id": "lib1"
                                },
                                {
                                "id": "rg2",
                                "R1": "tests/data/wgs2/wgs2-lib2_R1.fq.gz",
                                "R1_md5": "df64e84fdc9a2d7a9301f2aac0071aee",
                                "R2": "tests/data/wgs2/wgs2-lib2_R2.fq.gz",
                                "lib_id": "lib1"
                                }
                            ],
                            "id": "wgs2-paired-end",
                            "control": "wgs1-paired-end"
                            }
                        ]
                    }
                    EOL
                >>>

                output {
                    SampleConfig data = read_json("data.json")
                    String? control0 = data.samples[0].control
                    String? control1 = data.samples[1].control
                    String? gender0 = data.samples[0].gender
                    String? gender1 = data.samples[1].gender
                }
            }""")
        self.assertEqual(None, outp["control0"])
        self.assertEqual("wgs1-paired-end", outp["control1"])
        self.assertEqual(None, outp["gender0"])
        self.assertEqual(None, outp["gender1"])
        self.assertEqual(
            "537ffc52342314d839e7fdd91bbdccd0",
            outp["data"]["samples"][1]["readgroups"][0]["R2_md5"],
        )
        self.assertEqual(None, outp["data"]["samples"][1]["readgroups"][1]["R2_md5"])

    def test_bad_object(self):
        self._test_task(
            R"""
        version 1.0
        task bad_map {
            command <<<
                touch empty
            >>>
            output {
                Map[String,String] map = read_object("empty")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        self._test_task(
            R"""
        version 1.0
        task bad_map {
            command <<<
                echo -e "one\tone\ttwo" > dup
            >>>
            output {
                Map[String,String] map = read_object("dup")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        self._test_task(
            R"""
        version 1.0
        task bad_map {
            command <<<
                echo -e "one\ttwo\tthree" > ragged
                echo -e "1\t2\t3\t4" >> ragged
            >>>
            output {
                Map[String,String] map = read_object("ragged")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        outputs = self._test_task(
            R"""
        version 1.0
        struct Sample {
            String name
            Int lane
            String barcode
        }
        task test {
            command <<<
                echo -e "name\tlane" >> samplesheet2.txt
                echo -e "Alice\t3" >> samplesheet2.txt
                echo -e "Bob\t4" >> samplesheet2.txt
            >>>
            output {
                Array[Sample] samplesheet2 = read_objects("samplesheet2.txt")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

    def test_boolean(self):
        self._test_task(
            R"""
        version 1.0
        task bad_map {
            command <<<
                echo foo > bool
            >>>
            output {
                Boolean b = read_boolean("bool")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        outp = self._test_task(R"""
        version 1.0

        task read_bool {
            command <<<
            printf "  true  \n" > true_file
            printf "  FALSE  \n" > false_file
            >>>

            output {
                Boolean b1 = read_boolean("true_file")
                Boolean b2 = read_boolean("false_file")
            }
        }
        """)
        self.assertEqual(outp["b1"], True)
        self.assertEqual(outp["b2"], False)

    def test_write(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            File foo = write_lines(["foo","bar","baz"])
            File tsv = write_tsv([["one", "two", "three"], ["un", "deux", "trois"]])
            File json = write_json({"key1": "value1", "key2": "value2"})
            File map = write_map({"key1": "value1", "key2": "value2"})

            command <<<
                foo_sha=$(sha256sum < ~{foo} | cut -f1 -d ' ')
                if [ "$foo_sha" != "b1b113c6ed8ab3a14779f7c54179eac2b87d39fcebbf65a50556b8d68caaa2fb" ]; then
                    exit 1
                fi
                tsv_sha=$(sha256sum < ~{tsv} | cut -f1 -d ' ')
                if [ "$tsv_sha" != "a7124e688203195cd674cf147bbf965eda49e8df581d01c05944330fab096084" ]; then
                    exit 1
                fi
            >>>

            output {
                File o_json = json
                Array[Array[String]] o_tsv = read_tsv(tsv)
                Map[String,String] o_map = read_map(map)
                File whynot = write_lines(["foo","bar","baz"])
            }
        }
        """)
        with open(outputs["o_json"]) as infile:
            self.assertEqual(json.load(infile), {"key1": "value1", "key2": "value2"})
        self.assertEqual(outputs["o_tsv"], [["one", "two", "three"], ["un", "deux", "trois"]])
        self.assertEqual(outputs["o_map"], {"key1": "value1", "key2": "value2"})

    def test_bad_map(self):
        self._test_task(
            R"""
        version 1.0
        task bad_map {
            File map = write_map({"foo": "bar\t"})
            command {}
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        self._test_task(
            R"""
        version 1.0
        task bad_map {
            command <<<
                echo -e "a\tb" > map.txt
                echo -e "c\td\te" >> map.txt
            >>>
            output {
                Map[String,String] map = read_map("map.txt")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        self._test_task(
            R"""
        version 1.0
        task bad_map {
            command <<<
                echo -e "a\tb" > map.txt
                echo -e "a\tc" >> map.txt
            >>>
            output {
                Map[String,String] map = read_map("map.txt")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

    def test_transpose(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            command {}
            output {
                Array[Array[Int]] mat = transpose([[0, 1, 2], [3, 4, 5]])
            }
        }
        """)
        self.assertEqual(outputs["mat"], [[0, 3], [1, 4], [2, 5]])

        outputs = self._test_task(
            R"""
        version 1.0
        task hello {
            command {}
            output {
                Array[Array[Int]] mat = transpose([[0, 1, 2], [3, 4, 5], []])
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

    def test_zip_cross(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            Array[Int] xs = [ 1, 2, 3 ]
            Array[String] ys = [ "a", "b", "c" ]
            Array[String] zs = [ "d", "e" ]
            command {}
            output {
                Array[Pair[Int, String]] zipped = zip(xs, ys)
                Array[Pair[Int, String]] crossed = cross(xs, zs)
            }
        }
        """)
        self.assertEqual(
            outputs["zipped"],
            [{"left": 1, "right": "a"}, {"left": 2, "right": "b"}, {"left": 3, "right": "c"}],
        )
        self.assertEqual(
            outputs["crossed"],
            [
                {"left": 1, "right": "d"},
                {"left": 1, "right": "e"},
                {"left": 2, "right": "d"},
                {"left": 2, "right": "e"},
                {"left": 3, "right": "d"},
                {"left": 3, "right": "e"},
            ],
        )

        outputs = self._test_task(
            R"""
        version 1.0
        task hello {
            Array[Int] xs = [ 1, 2, 3 ]
            Array[String] ys = [ "a", "b", "c" ]
            Array[String] zs = [ "d", "e" ]
            command {}
            output {
                Array[Pair[Int, String]] zipped = zip(xs, zs)
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

    def test_unzip(self):
        outputs = self._test_task(R"""
        version 1.0
        task hello {
            Array[Int] xs = [ 1, 2, 3 ]
            Array[String] ys = [ "a", "b", "c" ]
            Array[String] zs = [ "d", "e" ]
            command {}
            output {
                Pair[Array[Int], Array[String]] unzipped = unzip(zip(xs, ys))
                Pair[Array[Int], Array[String]] uncrossed = unzip(cross(xs, zs))
            }
        }
        """)
        self.assertEqual(outputs["unzipped"], {"left": [1, 2, 3], "right": ["a", "b", "c"]})
        self.assertEqual(
            outputs["uncrossed"],
            {"left": [1, 1, 2, 2, 3, 3], "right": ["d", "e", "d", "e", "d", "e"]},
        )

        outputs = self._test_task(
            R"""
        version 1.0
        task hello {
            input {
                Array[Array[Int]] x
            }
            command {}
            output {
                Array[Pair[Int, Int]] zipped = unzip(x)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

    def test_sep(self):
        outputs = self._test_task(R"""
        version development
        task SepTest {
            input {
                Array[String] inp = ["value1", "value2", "value3"]
            }
            command {}
            output {
                String out = sep(",", inp)
            }
        }
        """)
        self.assertEqual("value1,value2,value3", outputs["out"])

        outputs = self._test_task(R"""
        version development
        task SepTest {
            input {
                Array[String] inp = ["value1", "value2", "value3"]
            }
            command <<<
                echo ~{sep(",", inp)}
            >>>

            output {
                String out = read_string(stdout())
            }
        }
        """)
        self.assertEqual("value1,value2,value3", outputs["out"])

        # deprecated, not removed in WDL 1.1:
        outputs = self._test_task(R"""
        version development
        task SepTest {
            input {
                Array[String] inp = ["value1", "value2", "value3"]
            }
            command <<<
                echo ~{sep="," inp}
            >>>
            output {
                String out = read_string(stdout())
            }
        }
        """)  # , expected_exception=WDL.Error.SyntaxError)
        self.assertEqual("value1,value2,value3", outputs["out"])

    def test_suffix(self):
        outputs = self._test_task(R"""
        version 1.0
        task test_suffix {
            command {}
            output {
                String chocolike = "I like chocolate when it's late"
                Array[String] chocolove = suffix(" and early",[chocolike]) # ["I like chocolate when it's late and early"]
                Array[String] chocoearly = suffix(" and early", [chocolike,chocolike]) # ["I like chocolate when it's late and early","I like chocolate when it's late and early"]
            }
        }
        """)

        # Check to make sure suffix added to each element in array
        self.assertEqual(
            outputs,
            {
                "chocolike": "I like chocolate when it's late",
                "chocolove": ["I like chocolate when it's late and early"],
                "chocoearly": [
                    "I like chocolate when it's late and early",
                    "I like chocolate when it's late and early",
                ],
            },
        )

        # check to make sure coercible type returns appropriate suffix
        outputs = self._test_task(R"""
                version 1.0
                task test_suffix {
                    command {}
                    output {
                        Array[Int] integers = [1,2,3,4]
                        Array[String] integers_with_suffix = suffix(".0", integers) # ["1.0","2.0","3.0","4.0"]
                    }
                }
                """)

        # Missing Suffix
        self._test_task(
            R"""
        version 1.0
        task test_suffix {
            command {}
            output {
                Array[Int] integers = suffix([1,2,3,4])
            }
        }
        """,
            expected_exception=WDL.Error.WrongArity,
        )

        # Suffix should require an array
        self._test_task(
            R"""
                version 1.0
                task test_suffix {
                    command {}
                    output {
                        Array[Int] integers = suffix("not-allowed","s")
                    }
                }
                """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        self.assertEqual(outputs["integers_with_suffix"], ["1.0", "2.0", "3.0", "4.0"])

    def test_quote(self):
        outputs = self._test_task(R"""
        version development
        task test_quote {
            command {}
            output {
                Array[String] arguments = ["foo","bar","baz"]
                Array[String] quoted_args = quote(arguments) # ["\"foo\"","\"bar\"","\"baz\""]
            }
        }
        """)
        # Check to make sure each element has be quoted appropriately
        self.assertEqual(
            outputs,
            {"arguments": ["foo", "bar", "baz"], "quoted_args": ['"foo"', '"bar"', '"baz"']},
        )

        outputs = self._test_task(R"""
        version development
        task test_quote {
            command {}
            output {
                Array[Int] arguments = [1,2,3]
                Array[String] quoted_args = quote(arguments) # ["\"1\"","\"2\"","\"3\""]
            }
        }
        """)

        # Check to make sure each element has been coerced and quoted appropriately
        self.assertEqual(outputs, {"arguments": [1, 2, 3], "quoted_args": ['"1"', '"2"', '"3"']})

        # Check invalid type does not work
        self._test_task(
            R"""
        version development
        task test_quote {
            command {}
            output {
                String arguments = "some argument"
                Array[String] quoted_args = quote(arguments)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # check unavailable in WDL draft-2 and 1.0
        self._test_task(
            R"""
        version 1.0
        task test_quote {
            command {}
            output {
                Array[Int] arguments = [1,2,3]
                Array[String] quoted_args = quote(arguments) # ["\"1\"","\"2\"","\"3\""]
            }
        }
        """,
            expected_exception=WDL.Error.NoSuchFunction,
        )

    def test_squote(self):
        outputs = self._test_task(R"""
        version development
        task test_squote {
            command {}
            output {
                Array[String] arguments = ["foo","bar","baz"]
                Array[String] quoted_args = squote(arguments) # ["'foo'","'bar'","'baz'"]
            }
        }
        """)
        # Check to make sure each element has be quoted appropriately
        self.assertEqual(
            outputs,
            {"arguments": ["foo", "bar", "baz"], "quoted_args": ["'foo'", "'bar'", "'baz'"]},
        )

        outputs = self._test_task(R"""
        version development
        task test_squote {
            command {}
            output {
                Array[Int] arguments = [1,2,3]
                Array[String] quoted_args = squote(arguments) # ["'1'","'2'","'3'"]
            }
        }
        """)

        # Check to make sure each element has been coerced and quoted appropriately
        self.assertEqual(outputs, {"arguments": [1, 2, 3], "quoted_args": ["'1'", "'2'", "'3'"]})

        # Check invalid type does not work
        outputs = self._test_task(
            R"""
        version development
        task test_squote {
            command {}
            output {
                String arguments = "some argument"
                Array[String] quoted_args = squote(arguments)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

    def test_keys(self):
        outputs = self._test_task(R"""
        version development
        task test_keys {
            input {
                Map[String,String] m1 = {"a": "b", "c": "d"}
                Map[Int,Boolean] m2 = {1: true, -1: false}
                Map[Int,Float]? m3
            }
            command {}
            output {
                Array[String] k1 = keys(m1)
                Array[Int] k2 = keys(m2)
                Array[Boolean] k4 = keys({})
                Array[Pair[Int,Boolean]] k5 = keys({(1,false): "foo", (3,true): "bar"})
            }
        }
        """)
        self.assertEqual(outputs["k1"], ["a", "c"])
        self.assertEqual(outputs["k2"], [1, -1])
        self.assertEqual(outputs["k4"], [])
        self.assertEqual(outputs["k5"], [{"left": 1, "right": False}, {"left": 3, "right": True}])

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._test_task(R"""
            version development
            task test_keys {
                input {
                    Array[Int] a = keys([1,2,3])
                }
                command {}
                output {}
            }
            """)

        # Test keys() with structs (WDL 1.2+)
        outputs = self._test_task(R"""
        version 1.2
        struct Person {
            String first
            String last
            Int age
        }
        task test_keys_struct {
            input {
                Person p = Person {
                    first: "John",
                    last: "Doe",
                    age: 30
                }
            }
            command {}
            output {
                Array[String] person_keys = keys(p)
            }
        }
        """)
        # Keys should be in the order they appear in the struct definition
        self.assertEqual(outputs["person_keys"], ["first", "last", "age"])

        # Test keys() with struct including optional members
        outputs = self._test_task(R"""
        version 1.2
        struct Contact {
            String name
            String? email
            String? phone
        }
        task test_keys_optional {
            input {
                Contact c = Contact {
                    name: "Alice",
                    email: "alice@example.com"
                }
            }
            command {}
            output {
                Array[String] contact_keys = keys(c)
            }
        }
        """)
        # Should include all members, even optional ones that are None
        self.assertEqual(outputs["contact_keys"], ["name", "email", "phone"])

        # Error: keys(Struct) not available in WDL 1.1
        self._test_task(
            R"""
        version 1.1
        struct Person {
            String first
            String last
        }
        task bad {
            input {
                Person p = Person {
                    first: "John",
                    last: "Doe"
                }
            }
            command {}
            output {
                Array[String] k = keys(p)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: optional Map argument
        self._test_task(
            R"""
        version 1.2
        task bad {
            input {
                Map[String, Int]? m = {"a": 1}
            }
            command {}
            output {
                Array[String] k = keys(m)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: optional Struct argument
        self._test_task(
            R"""
        version 1.2
        struct Person {
            String first
            String last
        }
        task bad {
            input {
                Person? p = Person {
                    first: "John",
                    last: "Doe"
                }
            }
            command {}
            output {
                Array[String] k = keys(p)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Test keys() with read_json coerced to Map
        outputs = self._test_task(R"""
        version 1.2
        task test_keys_from_json_map {
            command <<<
                echo '{"x": 1, "y": 2, "z": 3}' > data.json
            >>>
            output {
                Map[String, Int] data = read_json("data.json")
                Array[String] json_keys = keys(data)
            }
        }
        """)
        # Map keys may not be in guaranteed order depending on implementation
        self.assertEqual(sorted(outputs["json_keys"]), ["x", "y", "z"])

        # Test keys() with read_json coerced to Struct
        outputs = self._test_task(R"""
        version 1.2
        struct Data {
            Int x
            Int y
            Int z
        }
        task test_keys_from_json_struct {
            command <<<
                echo '{"x": 1, "y": 2, "z": 3}' > data.json
            >>>
            output {
                Data data = read_json("data.json")
                Array[String] json_keys = keys(data)
            }
        }
        """)
        # Struct keys are in definition order
        self.assertEqual(outputs["json_keys"], ["x", "y", "z"])

        # Test keys(read_json()) directly without type coercion - now works!
        outputs = self._test_task(R"""
        version 1.2
        task test_keys_direct_json {
            command <<<
                echo '{"x": 1, "y": 2, "z": 3}' > data.json
            >>>
            output {
                Array[String] json_keys = keys(read_json("data.json"))
            }
        }
        """)
        self.assertEqual(sorted(outputs["json_keys"]), ["x", "y", "z"])

        # Error: direct keys(read_json()) is WDL 1.2+ behavior
        self._test_task(
            R"""
        version 1.1
        task bad {
            command <<<
                echo '{"x": 1, "y": 2, "z": 3}' > data.json
            >>>
            output {
                Array[String] json_keys = keys(read_json("data.json"))
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: arbitrary Any expressions aren't accepted; only direct read_json()
        self._test_task(
            R"""
        version 1.2
        task bad {
            command <<<
                echo '{"x": 1, "y": 2, "z": 3}' > data.json
            >>>
            output {
                Array[String] json_keys = keys(if true then read_json("data.json") else read_json("data.json"))
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: keys(read_json()) with non-object JSON raises EvalError
        self._test_task(
            R"""
        version 1.2
        task bad {
            command <<<
                echo '42' > data.json
            >>>
            output {
                Array[String] json_keys = keys(read_json("data.json"))
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )

        # Note: The fallback path for Type.Object (line 1125-1126 in StdLib.py) is defensive code
        # that may be hit during coercion from read_json, though it's hard to isolate in testing.
        # The runtime error path (unexpected argument type) should be prevented by static type checking.

    def test_values(self):
        """Test the values() function from WDL 1.2"""

        # Basic functionality with different value types
        outputs = self._test_task(R"""
        version 1.2
        task test_values {
            input {
                Map[String, Int] m1 = {"a": 1, "b": 2, "c": 3}
                Map[Int, String] m2 = {1: "one", 2: "two"}
                Map[String, Pair[Int, Int]] m3 = {
                    "a": (1, 2),
                    "b": (3, 4)
                }
            }
            command {}
            output {
                Array[Int] v1 = values(m1)
                Array[String] v2 = values(m2)
                Array[Pair[Int, Int]] v3 = values(m3)
                Array[Boolean] v4 = values({})
            }
        }
        """)
        self.assertEqual(outputs["v1"], [1, 2, 3])
        self.assertEqual(outputs["v2"], ["one", "two"])
        self.assertEqual(outputs["v3"], [{"left": 1, "right": 2}, {"left": 3, "right": 4}])
        self.assertEqual(outputs["v4"], [])

        # Complex nested types
        outputs = self._test_task(R"""
        version 1.2
        task test_values_complex {
            input {
                Map[String, Array[Int]] nested = {
                    "x": [1, 2],
                    "y": [3, 4, 5]
                }
            }
            command {}
            output {
                Array[Array[Int]] vals = values(nested)
            }
        }
        """)
        self.assertEqual(outputs["vals"], [[1, 2], [3, 4, 5]])

        # Error: wrong arity (too few arguments)
        self._test_task(
            R"""
        version 1.2
        task bad {
            command {}
            output {
                Array[Int] x = values()
            }
        }
        """,
            expected_exception=WDL.Error.WrongArity,
        )

        # Error: wrong arity (too many arguments)
        self._test_task(
            R"""
        version 1.2
        task bad {
            command {}
            output {
                Array[Int] x = values({"a": 1}, {"b": 2})
            }
        }
        """,
            expected_exception=WDL.Error.WrongArity,
        )

        # Error: not available in WDL 1.1
        self._test_task(
            R"""
        version 1.1
        task bad {
            command {}
            output {
                Array[Int] x = values({"a": 1})
            }
        }
        """,
            expected_exception=WDL.Error.NoSuchFunction,
        )

        # Error: not available in WDL 1.0
        self._test_task(
            R"""
        version 1.0
        task bad {
            command {}
            output {
                Array[Int] x = values({"a": 1})
            }
        }
        """,
            expected_exception=WDL.Error.NoSuchFunction,
        )

        # Error: first argument not a map
        self._test_task(
            R"""
        version 1.2
        task bad {
            command {}
            output {
                Array[Int] x = values([1, 2, 3])
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

    def test_map_pairs(self):
        outputs = self._test_task(R"""
        version development
        task test_map_pairs {
            input {
                Array[Pair[String,Int]] x = [("b", 1), ("a", 2), ("c", 3)]
                Array[Pair[String,Pair[String,String]]] y = [("a", ("a_1.bam", "a_1.bai")), ("b", ("b.bam", "b.bai")), ("a", ("a_2.bam", "a_2.bai"))]
                Array[Pair[String,Pair[String,String]]] y2 = [("a", ("a.bam", "a.bai")), ("b", ("b.bam", "b.bai"))]
            }

            Map[String,Int] xmap = as_map(x)
            Map[String,Pair[String,String]] ymap = as_map(y2)

            command {}

            output {
                Map[String,Int] xmap_out = xmap
                Map[String,Pair[String,String]] ymap_out = ymap
                Map[String,Array[Int]] xmulti = collect_by_key(x)
                Map[String,Array[Pair[String,String]]] ymulti = collect_by_key(y)
                Array[Pair[String,Int]] x_roundtrip = as_pairs(xmap)
                Array[Pair[String,Pair[String,String]]] y_roundtrip = as_pairs(ymap)
            }
        }
        """)
        self.assertEqual(outputs["xmap_out"], {"b": 1, "a": 2, "c": 3})
        self.assertEqual(
            outputs["ymap_out"],
            {"a": {"left": "a.bam", "right": "a.bai"}, "b": {"left": "b.bam", "right": "b.bai"}},
        )
        self.assertEqual(outputs["xmulti"], {"b": [1], "a": [2], "c": [3]})
        self.assertEqual(
            outputs["ymulti"],
            {
                "a": [
                    {"left": "a_1.bam", "right": "a_1.bai"},
                    {"left": "a_2.bam", "right": "a_2.bai"},
                ],
                "b": [{"left": "b.bam", "right": "b.bai"}],
            },
        )
        self.assertEqual(
            outputs["x_roundtrip"],
            [{"left": "b", "right": 1}, {"left": "a", "right": 2}, {"left": "c", "right": 3}],
        )
        self.assertEqual(
            outputs["y_roundtrip"],
            [
                {"left": "a", "right": {"left": "a.bam", "right": "a.bai"}},
                {"left": "b", "right": {"left": "b.bam", "right": "b.bai"}},
            ],
        )

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._test_task(R"""
            version development
            task test_keys {
                input {
                    Map[String,Int]? optmap
                }
                command {}
                output {
                    Array[Pair[String,Int]] x = as_pairs(optmap)
                }
            }
            """)

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._test_task(R"""
            version development
            task test_keys {
                input {
                    Array[Array[Int]] a2
                }
                command {}
                output {
                    Map[Int,Int] x = collect_by_key(a2)
                }
            }
            """)

        with self.assertRaisesRegex(WDL.Error.EvalError, "duplicate"):
            outputs = self._test_task(R"""
            version development
            task test_map_pairs {
                input {
                    Array[Pair[String,Pair[String,String]]] y = [("a", ("a_1.bam", "a_1.bai")), ("b", ("b.bam", "b.bai")), ("a", ("a_2.bam", "a_2.bai"))]
                }
                command {}
                output {
                    Map[String,Pair[String,String]] ymap = as_map(y)
                }
            }
            """)

    def test_contains(self):
        """Test the contains() function from WDL 1.2"""

        # Basic functionality
        outputs = self._test_task(R"""
        version 1.2
        task test_contains {
            command {}
            output {
                Boolean has_2 = contains([1, 2, 3], 2)
                Boolean has_5 = contains([1, 2, 3], 5)
                Boolean has_foo = contains(["foo", "bar"], "foo")
                Boolean has_baz = contains(["foo", "bar"], "baz")
                Boolean empty = contains([], 42)
            }
        }
        """)
        self.assertEqual(outputs["has_2"], True)
        self.assertEqual(outputs["has_5"], False)
        self.assertEqual(outputs["has_foo"], True)
        self.assertEqual(outputs["has_baz"], False)
        self.assertEqual(outputs["empty"], False)

        # Type coercion - Int in Float array
        outputs = self._test_task(R"""
        version 1.2
        task test_contains_coercion {
            command {}
            output {
                Boolean int_in_float_array = contains([1.0, 2.0], 1)
                Boolean float_in_float_array = contains([1.5, 2.5], 1.5)
            }
        }
        """)
        self.assertEqual(outputs["int_in_float_array"], True)
        self.assertEqual(outputs["float_in_float_array"], True)

        # Pair and complex types
        outputs = self._test_task(R"""
        version 1.2
        task test_contains_pairs {
            command {}
            output {
                Boolean has_pair = contains([(1, "a"), (2, "b")], (2, "b"))
                Boolean no_pair = contains([(1, "a"), (2, "b")], (3, "c"))
            }
        }
        """)
        self.assertEqual(outputs["has_pair"], True)
        self.assertEqual(outputs["no_pair"], False)

        # Error: wrong arity (too few arguments)
        self._test_task(
            R"""
        version 1.2
        task bad {
            command {}
            output {
                Boolean x = contains([1, 2])
            }
        }
        """,
            expected_exception=WDL.Error.WrongArity,
        )

        # Error: wrong arity (too many arguments)
        self._test_task(
            R"""
        version 1.2
        task bad {
            command {}
            output {
                Boolean x = contains([1, 2], 1, 2)
            }
        }
        """,
            expected_exception=WDL.Error.WrongArity,
        )

        # Error: not available in WDL 1.1
        self._test_task(
            R"""
        version 1.1
        task bad {
            command {}
            output {
                Boolean x = contains([1, 2], 1)
            }
        }
        """,
            expected_exception=WDL.Error.NoSuchFunction,
        )

        # Error: not available in WDL 1.0
        self._test_task(
            R"""
        version 1.0
        task bad {
            command {}
            output {
                Boolean x = contains([1, 2], 1)
            }
        }
        """,
            expected_exception=WDL.Error.NoSuchFunction,
        )

        # Error: type mismatch (String in Int array)
        self._test_task(
            R"""
        version 1.2
        task bad {
            command {}
            output {
                Boolean x = contains([1, 2], "string")
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: first argument not an array
        self._test_task(
            R"""
        version 1.2
        task bad {
            command {}
            output {
                Boolean x = contains("not an array", "x")
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Optional element types: Array[T?] with T, T?, and None
        outputs = self._test_task(R"""
        version 1.2
        task test_optional_elements {
            input {
                Array[String?] samples = ["foo", "bar", None]
                String name = "bar"
                String? maybe_baz = "baz"
            }
            command {}
            output {
                # String in Array[String?] is allowed
                Boolean has_bar = contains(samples, name)
                # String? in Array[String?] is allowed
                Boolean has_baz = contains(samples, maybe_baz)
                # None in Array[String?] is allowed
                Boolean has_null = contains(samples, None)
            }
        }
        """)
        self.assertEqual(outputs["has_bar"], True)
        self.assertEqual(outputs["has_baz"], False)
        self.assertEqual(outputs["has_null"], True)

        # Error: optional value in non-optional array
        self._test_task(
            R"""
        version 1.2
        task bad {
            input {
                Int? maybe_num = 5
            }
            command {}
            output {
                Boolean x = contains([1, 2, 3], maybe_num)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

    def test_chunk(self):
        """Test the chunk() function from WDL 1.2"""

        self.assertEqual(
            str(self._infer_expr_type("chunk([1, 2, 3], 2)", version="1.2")),
            "Array[Array[Int]+]",
        )
        self.assertEqual(
            str(self._infer_expr_type("chunk([], 2)", version="1.2")),
            "Array[Array[Any]+]",
        )

        self.assertEqual(
            self._eval_expr('chunk(["a", "b", "c", "d", "e", "f"], 3)', version="1.2").json,
            [["a", "b", "c"], ["d", "e", "f"]],
        )
        self.assertEqual(
            self._eval_expr('chunk(["a", "b", "c", "d", "e"], 3)', version="1.2").json,
            [["a", "b", "c"], ["d", "e"]],
        )
        self.assertEqual(
            self._eval_expr('chunk(["a", "b"], 3)', version="1.2").json,
            [["a", "b"]],
        )
        self.assertEqual(
            self._eval_expr("chunk([], 3)", version="1.2").json,
            [],
        )
        nonempty_chunks = self._eval_expr("chunk([1, 2, 3], 2)", version="1.2")
        self.assertEqual(str(nonempty_chunks.type), "Array[Array[Int]+]+")
        self.assertEqual(str(nonempty_chunks.value[0].type), "Array[Int]+")
        empty_chunks = self._eval_expr("chunk([], 2)", version="1.2")
        self.assertEqual(str(empty_chunks.type), "Array[Array[Any]+]")
        self.assertEqual(
            self._eval_expr("flatten(chunk([1, 2, 3, 4, 5], 2))", version="1.2").json,
            [1, 2, 3, 4, 5],
        )

        for length in range(11):
            array = list(range(length))
            array_literal = "[" + ", ".join(str(i) for i in array) + "]"
            for chunk_size in range(1, length + 2):
                expected = [array[i : i + chunk_size] for i in range(0, length, chunk_size)]
                chunks = self._eval_expr(f"chunk({array_literal}, {chunk_size})", version="1.2")
                self.assertEqual(chunks.json, expected)
                self.assertEqual(chunks.type.nonempty, bool(expected))
                for chunk in chunks.value:
                    self.assertIsInstance(chunk, WDL.Value.Array)
                    self.assertTrue(chunk.type.nonempty)
                    self.assertGreater(len(chunk.value), 0)

        env = WDL.Env.Bindings().bind(
            "xs",
            WDL.Value.Array(
                WDL.Type.String(optional=True),
                [WDL.Value.String("foo"), WDL.Value.Null(), WDL.Value.String("bar")],
            ),
        )
        self.assertEqual(
            self._eval_expr("chunk(xs, 2)", env=env, version="1.2").json,
            [["foo", None], ["bar"]],
        )

        with self.assertRaises(WDL.Error.EvalError):
            self._eval_expr("chunk([1, 2], 0)", version="1.2")
        with self.assertRaises(WDL.Error.EvalError):
            self._eval_expr("chunk([1, 2], -1)", version="1.2")
        with self.assertRaises(WDL.Error.WrongArity):
            self._eval_expr("chunk([1, 2])", version="1.2")
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            self._eval_expr('chunk("not an array", 2)', version="1.2")
        with self.assertRaises(WDL.Error.EvalError):
            self._eval_expr('chunk([1, 2], "two")', version="1.2")
        with self.assertRaises(WDL.Error.NoSuchFunction):
            self._eval_expr("chunk([1, 2], 1)", version="1.1")

    def test_contains_key(self):
        """Test the contains_key() function from WDL 1.2"""

        # Basic Map key lookup
        outputs = self._test_task(R"""
        version 1.2
        task test_contains_key_map {
            input {
                Map[String, Int] m = {"a": 1, "b": 2, "c": 3}
            }
            command {}
            output {
                Boolean has_a = contains_key(m, "a")
                Boolean has_d = contains_key(m, "d")
            }
        }
        """)
        self.assertEqual(outputs["has_a"], True)
        self.assertEqual(outputs["has_d"], False)

        # Map with Int keys
        outputs = self._test_task(R"""
        version 1.2
        task test_contains_key_int_map {
            input {
                Map[Int, String] m = {1: "one", 2: "two"}
            }
            command {}
            output {
                Boolean has_1 = contains_key(m, 1)
                Boolean has_3 = contains_key(m, 3)
            }
        }
        """)
        self.assertEqual(outputs["has_1"], True)
        self.assertEqual(outputs["has_3"], False)

        # Struct key lookup
        outputs = self._test_task(R"""
        version 1.2
        struct Person {
            String name
            Int age
            String? email
        }
        task test_contains_key_struct {
            input {
                Person p = Person {
                    name: "Alice",
                    age: 30
                }
            }
            command {}
            output {
                Boolean has_name = contains_key(p, "name")
                Boolean has_email = contains_key(p, "email")
                Boolean has_phone = contains_key(p, "phone")
            }
        }
        """)
        self.assertEqual(outputs["has_name"], True)
        self.assertEqual(outputs["has_email"], True)  # Optional members are present
        self.assertEqual(outputs["has_phone"], False)

        # Nested key lookup
        outputs = self._test_task(R"""
        version 1.2
        task test_contains_key_nested {
            input {
                Map[String, Map[String, Int]] nested = {
                    "a": {"x": 1, "y": 2},
                    "b": {"z": 3}
                }
            }
            command {}
            output {
                Boolean has_a_x = contains_key(nested, ["a", "x"])
                Boolean has_a_z = contains_key(nested, ["a", "z"])
                Boolean has_b_z = contains_key(nested, ["b", "z"])
                Boolean has_c_x = contains_key(nested, ["c", "x"])
            }
        }
        """)
        self.assertEqual(outputs["has_a_x"], True)
        self.assertEqual(outputs["has_a_z"], False)
        self.assertEqual(outputs["has_b_z"], True)
        self.assertEqual(outputs["has_c_x"], False)

        # Nested key lookup with Struct
        outputs = self._test_task(R"""
        version 1.2
        struct Details {
            String? phone
            String? email
        }
        struct Person {
            String name
            Details? details
        }
        task test_contains_key_nested_struct {
            input {
                Person p1 = Person {
                    name: "John",
                    details: Details {
                        phone: "123-456-7890"
                    }
                }
                Person p2 = Person {
                    name: "Jane"
                }
            }
            command {}
            output {
                Boolean p1_has_details_phone = contains_key(p1, ["details", "phone"])
                Boolean p1_has_details_email = contains_key(p1, ["details", "email"])
                Boolean p2_has_details_phone = contains_key(p2, ["details", "phone"])
            }
        }
        """)
        self.assertEqual(outputs["p1_has_details_phone"], True)
        self.assertEqual(outputs["p1_has_details_email"], True)  # contains_key tests presence
        self.assertEqual(outputs["p2_has_details_phone"], False)  # p2.details is None

        # contains_key(read_json()) direct usage
        outputs = self._test_task(R"""
        version 1.2
        task test_contains_key_json {
            command <<<
                echo '{"x": 1, "y": 2, "z": 3}' > data.json
            >>>
            output {
                Boolean has_x = contains_key(read_json("data.json"), "x")
                Boolean has_w = contains_key(read_json("data.json"), "w")
            }
        }
        """)
        self.assertEqual(outputs["has_x"], True)
        self.assertEqual(outputs["has_w"], False)

        # contains_key(read_json()) with nested keys
        outputs = self._test_task(R"""
        version 1.2
        task test_contains_key_json_nested {
            command <<<
                echo '{"a": {"x": 1, "y": 2}, "b": {"z": 3}}' > data.json
            >>>
            output {
                Boolean has_a_x = contains_key(read_json("data.json"), ["a", "x"])
                Boolean has_a_z = contains_key(read_json("data.json"), ["a", "z"])
            }
        }
        """)
        self.assertEqual(outputs["has_a_x"], True)
        self.assertEqual(outputs["has_a_z"], False)

        # Error: arbitrary Any expressions aren't accepted; only direct read_json()
        self._test_task(
            R"""
        version 1.2
        task bad {
            command <<<
                echo '{"x": 1}' > data.json
            >>>
            output {
                Boolean x = contains_key(if true then read_json("data.json") else read_json("data.json"), "x")
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: wrong arity
        self._test_task(
            R"""
        version 1.2
        task bad {
            command {}
            output {
                Boolean x = contains_key({"a": 1})
            }
        }
        """,
            expected_exception=WDL.Error.WrongArity,
        )

        # Error: not available in WDL 1.1
        self._test_task(
            R"""
        version 1.1
        task bad {
            command {}
            output {
                Boolean x = contains_key({"a": 1}, "a")
            }
        }
        """,
            expected_exception=WDL.Error.NoSuchFunction,
        )

        # Error: key type mismatch
        self._test_task(
            R"""
        version 1.2
        task bad {
            input {
                Map[Int, String] m = {1: "one"}
            }
            command {}
            output {
                Boolean x = contains_key(m, "one")
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: None key only allowed when the map key type is optional
        optional_key_wdl = R"""
        version 1.2
        task bad {
            input {
                Map[String, Int] m = {"a": 1}
                String? k = None
            }
            command {}
            output {
                Boolean x = contains_key(m, k)
            }
        }
        """
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_document(optional_key_wdl).typecheck()
        WDL.parse_document(optional_key_wdl).typecheck(check_quant=False)

        # OK: optional map key type permits an optional lookup key
        type_env = (
            WDL.Env.Bindings()
            .bind("m", WDL.Type.Map((WDL.Type.String(optional=True), WDL.Type.Int())))
            .bind("k", WDL.Type.String(optional=True))
        )
        expr = WDL.parse_expr("contains_key(m, k)", version="1.2").infer_type(
            type_env, WDL.StdLib.Base("1.2")
        )
        self.assertIsInstance(expr.type, WDL.Type.Boolean)

        # Error: optional map not allowed when check_quant=True
        optional_map_wdl = R"""
        version 1.2
        task bad {
            input {
                Map[String, Int]? m = {"a": 1}
            }
            command {}
            output {
                Boolean x = contains_key(m, "a")
            }
        }
        """
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_document(optional_map_wdl).typecheck()

        # Error: nested key paths on maps require String keys
        self._test_task(
            R"""
        version 1.2
        task bad {
            input {
                Map[Int, Int] m = {1: 10}
            }
            command {}
            output {
                Boolean x = contains_key(m, ["1"])
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: struct/object lookup key must be String or Array[String]
        self._test_task(
            R"""
        version 1.2
        struct Box {
            Int v
        }
        task bad {
            input {
                Box b = Box {v: 1}
            }
            command {}
            output {
                Boolean x = contains_key(b, 1)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: contains_key(read_json()) key must be String or Array[String]
        self._test_task(
            R"""
        version 1.2
        task bad {
            command <<<
                echo '{"x": 1}' > data.json
            >>>
            output {
                Boolean x = contains_key(read_json("data.json"), 1)
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: nested key array must be Array[String]
        self._test_task(
            R"""
        version 1.2
        task bad {
            input {
                Map[String, Int] m = {"a": 1}
            }
            command {}
            output {
                Boolean x = contains_key(m, [1])
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Runtime: nested lookup falls through non-collection intermediates
        outputs = self._test_task(R"""
        version 1.2
        task test_contains_key_nested_scalar {
            command {}
            output {
                Boolean has_a_b = contains_key({"a": 1}, ["a", "b"])
            }
        }
        """)
        self.assertEqual(outputs["has_a_b"], False)

        # Runtime: empty nested key path always returns false
        outputs = self._test_task(R"""
        version 1.2
        task test_contains_key_nested_empty {
            input {
                Map[String, Int] m = {"a": 1}
                Array[String] path = []
            }
            command {}
            output {
                Boolean has_empty = contains_key(m, path)
            }
        }
        """)
        self.assertEqual(outputs["has_empty"], False)

        # Error: first argument not a collection
        self._test_task(
            R"""
        version 1.2
        task bad {
            command {}
            output {
                Boolean x = contains_key("not a map", "key")
            }
        }
        """,
            expected_exception=WDL.Error.StaticTypeMismatch,
        )

        # Error: contains_key(read_json()) with non-collection JSON
        self._test_task(
            R"""
        version 1.2
        task bad {
            command <<<
                echo '42' > data.json
            >>>
            output {
                Boolean x = contains_key(read_json("data.json"), "key")
            }
        }
        """,
            expected_exception=WDL.Error.EvalError,
        )
