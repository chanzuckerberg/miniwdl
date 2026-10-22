import logging
import unittest

import WDL.Lint
from WDL import Env, StdLib, Tree, Type, Value
from WDL.runtime.task import _eval_task_hints

from .context import WDL

# The two task examples from the WDL 1.2 spec's Hints section, verbatim
SPEC_TEST_HINTS = """version 1.2

task test_hints {
  input {
    File foo
  }

  command <<<
  wc -l < ~{foo}
  >>>

  output {
    Int num_lines = read_int(stdout())
  }

  requirements {
    container: "ubuntu:latest"
  }

  hints {
    max_memory: "36 GB"
    max_cpu: 24
    short_task: true
    localization_optional: false
    inputs: input {
      foo: hints {
        localization_optional: true
      }
    }
  }
}
"""

SPEC_INPUT_HINT = """version 1.2

struct Person {
  String name
  File? cv
}

task input_hint {
  input {
    Person person
  }

  command <<<
  if ~{defined(person.cv)}; then
    grep "WDL" ~{person.cv}
  fi
  >>>

  output {
    Array[String] experience = read_lines(stdout())
  }

  hints {
    inputs: input {
      person.name: hints {
        min_length: 3
      },
      person.cv: hints {
        localization_optional: true
      }
    }
    outputs: output {
      experience: hints {
        max_length: 5
      }
    }
  }
}
"""


class TestWorkflowHintsAST(unittest.TestCase):
    def test_ast_representation(self):
        # workflow hints are retained as ASTs in the same representation as
        # task hints (no conversion to plain values at parse time)
        doc = WDL.parse_document(
            """version 1.2
            workflow w {
                hints {
                    allow_nested_inputs: true
                    labels: {"team": "genomics", "priority": 3}
                    retry_backoff_sec: [1, 2, 4]
                    group: hints { contact: "ops@example.org" }
                }
            }
            """
        )
        hints = doc.workflow.hints
        self.assertEqual(
            set(hints), {"allow_nested_inputs", "labels", "retry_backoff_sec", "group"}
        )
        self.assertIsInstance(hints["allow_nested_inputs"], WDL.Expr.Boolean)
        self.assertTrue(hints["allow_nested_inputs"].value)
        self.assertIsInstance(hints["labels"], WDL.Expr.Map)
        self.assertIsInstance(hints["retry_backoff_sec"], WDL.Expr.Array)
        group = hints["group"]
        self.assertIsInstance(group, Tree.HintsObject)
        self.assertIsInstance(group.values["contact"], WDL.Expr.String)
        self.assertIsNotNone(doc.workflow.hints_section_pos)
        # the AST nodes are linked into the tree (parents set by the parser)
        self.assertIsNotNone(hints["allow_nested_inputs"].parent)

    def test_nonliteral_values(self):
        # expressions of any kind are rejected in workflow hints, including
        # function calls, name lookups, arithmetic, and string interpolation
        for value in ('length(names) > 0', "names", "1 + 2", '"hello ~{names[0]}"'):
            doc = """version 1.2
            workflow w {
                input {
                    Array[String] names
                }
                hints {
                    allow_nested_inputs: %s
                }
            }
            """ % (
                value,
            )
            with self.assertRaises(WDL.Error.SyntaxError, msg=value):
                WDL.parse_document(doc)

    def test_nonliteral_key(self):
        # map keys are expressions too: a bare identifier is a name lookup (it
        # does NOT fold to its name), and compound keys are not literals
        for key in ("team", "a.b", "[1]"):
            doc = """version 1.2
            workflow w {
                hints {
                    labels: {%s: "x"}
                }
            }
            """ % (
                key,
            )
            with self.assertRaises(WDL.Error.SyntaxError, msg=key):
                WDL.parse_document(doc)

    def test_io_block_rejected(self):
        doc = """version 1.2
        workflow w {
            hints {
                inputs: input { x: hints { } }
            }
        }
        """
        with self.assertRaises(WDL.Error.SyntaxError):
            WDL.parse_document(doc)

    def test_hints_node_parents(self):
        # SetParents links HintsObject/IOHints nodes (and the expressions
        # within them) to their containing node, not clear up to the
        # Task/Workflow
        doc = WDL.parse_document(
            """version 1.2
            task t {
                input {
                    File f
                }
                command <<< true >>>
                hints {
                    aws: hints {
                        instance_type: "p5.48xlarge"
                    }
                    inputs: input {
                        f: hints {
                            localization_optional: true
                        }
                    }
                }
            }
            workflow w {
                hints {
                    group: hints {
                        contact: "ops@example.org"
                    }
                }
            }
            """
        )
        task = doc.tasks[0]
        aws = task.hints["aws"]
        self.assertIsInstance(aws, Tree.HintsObject)
        self.assertIs(aws.parent, task)
        self.assertIs(aws.values["instance_type"].parent, aws)
        inputs = task.hints["inputs"]
        self.assertIsInstance(inputs, Tree.IOHints)
        self.assertIs(inputs.parent, task)
        f_hints = inputs.values["f"]
        self.assertIsInstance(f_hints, Tree.HintsObject)
        self.assertIs(f_hints.parent, inputs)
        self.assertIs(f_hints.values["localization_optional"].parent, f_hints)
        wf = doc.workflow
        group = wf.hints["group"]
        self.assertIsInstance(group, Tree.HintsObject)
        self.assertIs(group.parent, wf)
        self.assertIs(group.values["contact"].parent, group)


class TestWorkflowHintsLint(unittest.TestCase):
    def test_lint_workflow_hints_with_array_and_map(self):
        # regression: workflow hint expressions must have their types
        # inferred during typecheck, or Lint rules reading Expr.type crash
        # with AssertionError (e.g. on an array literal)
        doc = WDL.parse_document(
            """version 1.2
            workflow w {
                hints {
                    tags: ["a", "b"]
                    counts: [1, 2, 3]
                    labels: {"team": "genomics", "org": "czi"}
                    group: hints {
                        contact: "ops@example.org"
                    }
                }
            }
            """
        )
        doc.typecheck()
        tags_ty = doc.workflow.hints["tags"].type
        self.assertIsInstance(tags_ty, Type.Array)
        self.assertIsInstance(tags_ty.item_type, Type.String)
        counts_ty = doc.workflow.hints["counts"].type
        self.assertIsInstance(counts_ty, Type.Array)
        self.assertIsInstance(counts_ty.item_type, Type.Int)
        labels_ty = doc.workflow.hints["labels"].type
        self.assertIsInstance(labels_ty, Type.Map)
        self.assertIsInstance(labels_ty.item_type[0], Type.String)
        self.assertIsInstance(labels_ty.item_type[1], Type.String)
        # runs to completion (previously: AssertionError in Lint)
        WDL.Lint.collect(WDL.Lint.lint(doc, descend_imports=False))

    def test_duplicate_keys_io_block(self):
        txt = """
        task t {
            input {
                File f
            }
            command <<< true >>>
            hints {
                inputs: input {
                    f: hints { localization_optional: true },
                    f: hints { localization_optional: false }
                }
            }
        }
        """
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            WDL.parse_tasks(txt, version="1.2")


class TestHintsTypecheck(unittest.TestCase):
    def test_spec_examples(self):
        for txt in (SPEC_TEST_HINTS, SPEC_INPUT_HINT):
            doc = WDL.parse_document(txt)
            doc.typecheck()

    def test_unknown_identifier(self):
        doc = WDL.parse_document(
            """version 1.2
            task t {
                command <<< true >>>
                hints {
                    max_cpu: no_such_name + 1
                }
            }
            """
        )
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()

    def test_ill_typed_expression(self):
        doc = WDL.parse_document(
            """version 1.2
            task t {
                command <<< true >>>
                hints {
                    max_cpu: true + 1
                }
            }
            """
        )
        with self.assertRaises(WDL.Error.IncompatibleOperand):
            doc.typecheck()

    def test_environment_parity_with_requirements(self):
        # hints expressions see task inputs & postinputs, but neither the
        # task's outputs nor task.*
        doc = WDL.parse_document(
            """version 1.2
            task t {
                input {
                    Int n = 4
                }
                command <<< true >>>
                output {
                    Int m = n
                }
                hints {
                    priority: m
                }
            }
            """
        )
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()

        doc = WDL.parse_document(
            """version 1.2
            task t {
                command <<< true >>>
                hints {
                    max_cpu: task.max_retries
                }
            }
            """
        )
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()

    def test_reserved_key_violations(self):
        violations = [
            'short_task: "yes"',  # not Boolean
            'localization_optional: 1',  # not Boolean
            'max_cpu: "lots"',  # String does not satisfy Int|Float here
            "max_memory: 1.5",  # Float is not Int|String
            "maxCpu: true",  # camelCase alias, not Int|Float
            "disks: 42",  # not String|Map[String,String]
            'disks: {"/mnt": 3}',  # map values must be String
            'gpu: [1]',  # not Int|String
        ]
        for hint in violations:
            doc = WDL.parse_document(
                """version 1.2
                task t {
                    command <<< true >>>
                    hints {
                        %s
                    }
                }
                """
                % hint
            )
            with self.assertRaises(WDL.Error.StaticTypeMismatch, msg=hint):
                doc.typecheck()

    def test_reserved_key_violations_nested(self):
        # members of a nested hints object follow the same reserved-key rules
        doc = WDL.parse_document(
            """version 1.2
            task t {
                command <<< true >>>
                hints {
                    aws: hints {
                        max_cpu: "lots"
                    }
                }
            }
            """
        )
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()

    def test_reserved_key_acceptances(self):
        doc = WDL.parse_document(
            """version 1.2
            task t {
                input {
                    Int n
                }
                command <<< true >>>
                hints {
                    max_cpu: n * 2
                    max_cpu_f: 2.5
                    max_memory: 1073741824
                    disks: {"/mnt/data": "SSD", "/mnt/scratch": "HDD 500 GB"}
                    gpu: "A100"
                    fpga: 1
                    short_task: false
                    custom_anything: [1, "two", true]
                }
            }
            """
        )
        doc.typecheck()

    def test_io_block_validation(self):
        header = """version 1.2
        struct Person {
            String name
            File? cv
        }
        task t {
            input {
                Person person
                File data
            }
            command <<< true >>>
            output {
                File report = "report.txt"
            }
            hints {
                %s
            }
        }
        """
        # unknown input name
        doc = WDL.parse_document(
            header % "inputs: input { bogus: hints { localization_optional: true } }"
        )
        with self.assertRaises(WDL.Error.NoSuchMember):
            doc.typecheck()
        # unknown struct member in a dotted path
        doc = WDL.parse_document(
            header % "inputs: input { person.nme: hints { min_length: 3 } }"
        )
        with self.assertRaises(WDL.Error.NoSuchMember):
            doc.typecheck()
        # outputs block may not name an input
        doc = WDL.parse_document(header % "outputs: output { data: hints { } }")
        with self.assertRaises(WDL.Error.NoSuchMember):
            doc.typecheck()
        # inputs must be an input block (not an output block, nor an expression)
        for bad in ("inputs: output { data: hints { } }", "inputs: 42"):
            doc = WDL.parse_document(header % bad)
            with self.assertRaises(WDL.Error.ValidationError, msg=bad):
                doc.typecheck()
        # valid dotted paths into struct members & outputs
        doc = WDL.parse_document(
            header
            % """
                inputs: input {
                    person.cv: hints { localization_optional: true },
                    data: hints { localization_optional: false }
                }
                outputs: output {
                    report: hints { max_length: 5 }
                }
            """
        )
        doc.typecheck()

    def test_io_block_struct_output_member(self):
        # outputs block keys name output declarations; members of a
        # struct-typed output are addressed by dotted paths (report.summary),
        # not by bare member names (summary)
        src = """version 1.2
        struct Report {
            File summary
            Directory bundle
        }
        task t {
            command <<< true >>>
            output {
                Report report = Report { summary: "s.txt", bundle: "b" }
            }
            hints {
                outputs: output {
                    %s
                }
            }
        }
        """
        doc = WDL.parse_document(src % "report.summary: hints { max_length: 5 }")
        doc.typecheck()
        doc = WDL.parse_document(src % "summary: hints { max_length: 5 }")
        with self.assertRaises(WDL.Error.NoSuchMember):
            doc.typecheck()

    def test_io_block_rejects_private_declarations(self):
        # the keys of an inputs hint block must name parameters in the task's
        # input{} section; a private (postinput) declaration doesn't qualify
        header = """version 1.2
        task t {
            input {
                File data
            }
            File priv = data
            command <<< true >>>
            hints {
                inputs: input {
                    %s: hints { localization_optional: true }
                }
            }
        }
        """
        doc = WDL.parse_document(header % "priv")
        with self.assertRaises(WDL.Error.NoSuchMember):
            doc.typecheck()
        # a parameter in the input{} section is accepted
        doc = WDL.parse_document(header % "data")
        doc.typecheck()

    def test_runtime_hints_mutually_exclusive(self):
        # the runtime section is mutually exclusive with the requirements and
        # hints sections (WDL 1.2 spec, Runtime section)
        doc = WDL.parse_document(
            """version 1.2
            task t {
                command <<< true >>>
                runtime {
                    docker: "ubuntu:latest"
                }
                hints {
                    short_task: true
                }
            }
            """
        )
        with self.assertRaises(WDL.Error.ValidationError):
            doc.typecheck()
        # an empty hints{} section still counts as declaring hints
        doc = WDL.parse_document(
            """version 1.2
            task t {
                command <<< true >>>
                runtime {
                    docker: "ubuntu:latest"
                }
                hints {
                }
            }
            """
        )
        with self.assertRaises(WDL.Error.ValidationError):
            doc.typecheck()
        # requirements + hints together are fine, as is runtime alone
        for sections in (
            'requirements { container: "ubuntu:latest" } hints { short_task: true }',
            'runtime { docker: "ubuntu:latest" }',
        ):
            doc = WDL.parse_document(
                """version 1.2
                task t {
                    command <<< true >>>
                    %s
                }
                """
                % sections
            )
            doc.typecheck()

    def test_nested_hints_rejected(self):
        # the hints type cannot be nested (WDL 1.2 spec, Hints section): a
        # hints object may appear at the top level of the hints section
        # (compute environments) and as an input/output entry's value, but a
        # member of a hints object may not itself have a hints value
        header = """version 1.2
        task t {
            input {
                File data
            }
            command <<< true >>>
            hints {
                %s
            }
        }
        """
        for bad in (
            # nested within a compute-environment block
            "aws: hints { nested: hints { x: 1 } }",
            # nested within an input hints entry
            "inputs: input { data: hints { nested: hints { x: 1 } } }",
        ):
            doc = WDL.parse_document(header % bad)
            with self.assertRaises(WDL.Error.ValidationError, msg=bad):
                doc.typecheck()
        # the non-nested forms remain legal
        for good in (
            "aws: hints { instance_type: \"p5.48xlarge\" }",
            "inputs: input { data: hints { localization_optional: true } }",
        ):
            doc = WDL.parse_document(header % good)
            doc.typecheck()

    def test_io_block_only_under_inputs_outputs(self):
        # input/output blocks are accepted only under the inputs/outputs
        # keys (the block kind follows from the key); anywhere else -- under
        # another top-level key, or nested within an io block entry -- is an
        # error
        header = """version 1.2
        task t {
            input {
                File data
            }
            command <<< true >>>
            hints {
                %s
            }
        }
        """
        for bad in (
            "aws: input { data: hints { localization_optional: true } }",
            "aws: output { }",
            "inputs: input { data: output { } }",
        ):
            doc = WDL.parse_document(header % bad)
            with self.assertRaises(WDL.Error.ValidationError, msg=bad):
                doc.typecheck()


class TestHintsEval(unittest.TestCase):
    def _eval(self, task, inputs, env, stdlib):
        return _eval_task_hints(logging.getLogger(self.id()), task, inputs, env, stdlib)

    def test_spec_example_end_to_end(self):
        # spec test_hints task: evaluate the hints, with a JSON override
        doc = WDL.parse_document(SPEC_TEST_HINTS)
        doc.typecheck()
        task = doc.tasks[0]
        inputs = WDL.values_from_json(
            {
                "test_hints.foo": "data/greetings.txt",
                "test_hints.hints.max_cpu": 12,
            },
            task.available_inputs,
            task.required_inputs,
            namespace="test_hints",
        )
        env: Env.Bindings[Value.Base] = Env.Bindings()
        env = env.bind("foo", inputs["foo"])
        ans = self._eval(task, inputs, env, StdLib.Base(task.effective_wdl_version))
        self.assertEqual(ans["max_cpu"], Value.Int(12))  # override superseded 24
        self.assertEqual(ans["max_memory"], Value.String("36 GB"))
        self.assertEqual(ans["short_task"], Value.Boolean(True))
        self.assertEqual(ans["localization_optional"], Value.Boolean(False))
        # the inputs block evaluates to a Struct of Object; its kind follows
        # from the "inputs" key
        inputs_val = ans["inputs"]
        self.assertIsInstance(inputs_val, Value.Struct)
        self.assertIsInstance(inputs_val.type, Type.Object)
        self.assertEqual(
            inputs_val.value["foo"].value, {"localization_optional": Value.Boolean(True)}
        )

    def _fixture(self):
        # task with expression hints, a compute-environment block, and
        # input/output hints blocks; called from a workflow
        txt = """version 1.2

        struct Person {
            String name
            File? cv
        }

        task analyze {
            input {
                Person person
                Int n_threads = 4
                File data
            }
            command <<<
                analyze --threads ~{n_threads} ~{data}
            >>>
            output {
                File report = "report.txt"
            }
            hints {
                max_cpu: n_threads * 2
                max_memory: if n_threads > 4 then "32 GB" else "16 GB"
                aws: hints {
                    instance_type: "p5.48xlarge"
                    max_cpu: 96
                }
                inputs: input {
                    person.cv: hints {
                        localization_optional: true
                    },
                    data: hints {
                        localization_optional: false,
                        streaming_min_size_mb: if defined(person.cv) then 10 else 0
                    }
                }
                outputs: output {
                    report: hints {
                        max_length: 5
                    }
                }
            }
        }

        workflow wf {
            input {
                Person p
                File d
            }
            call analyze {
                input: person = p, data = d
            }
            output {
                File report = analyze.report
            }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()
        task = doc.tasks[0]
        wf = doc.workflow
        assert wf is not None
        stdlib = StdLib.Base(task.effective_wdl_version)
        return doc, task, wf, stdlib

    def _call_env(self, task, wf, stdlib, inputs_json):
        wf_inputs = WDL.values_from_json(
            inputs_json, wf.available_inputs, wf.required_inputs, namespace="wf"
        )
        call_inputs = wf_inputs.enter_namespace("analyze")
        wf_env: Env.Bindings[Value.Base] = Env.Bindings()
        wf_env = wf_env.bind("p", wf_inputs["p"]).bind("d", wf_inputs["d"])
        call = next(c for c in wf.body if isinstance(c, Tree.Call))
        env: Env.Bindings[Value.Base] = Env.Bindings()
        for name, expr in call.inputs.items():
            env = env.bind(name, expr.eval(wf_env, stdlib))
        env = env.bind("n_threads", call_inputs["n_threads"])
        return call_inputs, env

    def test_eval_declared(self):
        _, task, wf, stdlib = self._fixture()
        call_inputs, env = self._call_env(
            task,
            wf,
            stdlib,
            {
                "wf.p": {"name": "Joe", "cv": "data/cv.pdf"},
                "wf.d": "data/reads.bam",
                "wf.analyze.n_threads": 8,
            },
        )
        ans = self._eval(task, call_inputs, env, stdlib)
        self.assertEqual(ans["max_cpu"], Value.Int(16))
        self.assertEqual(ans["max_memory"], Value.String("32 GB"))
        aws_val = ans["aws"]
        self.assertIsInstance(aws_val, Value.Struct)
        self.assertIsInstance(aws_val.type, Type.Object)
        self.assertEqual(
            aws_val.value,
            {"instance_type": Value.String("p5.48xlarge"), "max_cpu": Value.Int(96)},
        )
        inputs_val = ans["inputs"]
        self.assertIsInstance(inputs_val, Value.Struct)
        self.assertIsInstance(inputs_val.type, Type.Object)
        self.assertEqual(
            inputs_val.value["person.cv"].value,
            {"localization_optional": Value.Boolean(True)},
        )
        self.assertEqual(
            inputs_val.value["data"].value["streaming_min_size_mb"], Value.Int(10)
        )
        outputs_val = ans["outputs"]
        self.assertIsInstance(outputs_val, Value.Struct)
        self.assertEqual(outputs_val.value["report"].value, {"max_length": Value.Int(5)})

    def test_eval_overrides_replace(self):
        # JSON overrides replace the declared value of their top-level key
        # wholesale (they do not deep-merge into it), and may supply hints
        # the task doesn't declare
        _, task, wf, stdlib = self._fixture()
        call_inputs, env = self._call_env(
            task,
            wf,
            stdlib,
            {
                "wf.p": {"name": "Joe", "cv": "data/cv.pdf"},
                "wf.d": "data/reads.bam",
                "wf.analyze.n_threads": 8,
                "wf.analyze.hints.max_cpu": 32,
                "wf.analyze.hints.custom_new": "hello",
                "wf.analyze.hints.disk_spec": {"/mnt/data": "SSD"},
            },
        )
        ans = self._eval(task, call_inputs, env, stdlib)
        self.assertEqual(ans["max_cpu"], Value.Int(32))
        self.assertEqual(ans["custom_new"], Value.String("hello"))
        # an object-valued override arrives in the same Struct-of-Object
        # shape as an evaluated declared hints object (cf. ans["aws"])
        disk_spec = ans["disk_spec"]
        self.assertIsInstance(disk_spec, Value.Struct)
        self.assertIsInstance(disk_spec.type, Type.Object)
        self.assertEqual(disk_spec.value, {"/mnt/data": Value.String("SSD")})
        # untouched keys still come from the WDL declarations
        self.assertEqual(ans["max_memory"], Value.String("32 GB"))
        self.assertEqual(
            ans["aws"].value,
            {"instance_type": Value.String("p5.48xlarge"), "max_cpu": Value.Int(96)},
        )

    def test_eval_override_errors(self):
        # Overrides addressing a value below the top-level key, and overrides
        # of keys declared with hints/input/output blocks, are input errors
        # citing openwdl/wdl#773 (the spec is ambiguous about nested hint
        # overrides; see the "Specifying / Overriding" spec section). The
        # dotted-path overrides are rejected while the inputs are parsed
        # (values_from_json); the block-key overrides parse into bindings,
        # and are rejected when the hints are evaluated (for workflow runs,
        # once at workflow startup instead -- see TestProcessHints).
        _, task, wf, stdlib = self._fixture()
        base_json = {
            "wf.p": {"name": "Joe", "cv": "data/cv.pdf"},
            "wf.d": "data/reads.bam",
            "wf.analyze.n_threads": 8,
        }
        dotted_overrides = [
            {"wf.analyze.hints.aws.instance_type": "p5.24xlarge"},
            {"wf.analyze.hints.inputs.data.localization_optional": True},
        ]
        for bad in dotted_overrides:
            inputs_json = dict(base_json)
            inputs_json.update(bad)
            with self.assertRaises(WDL.Error.InputError, msg=str(bad)) as ctx:
                self._call_env(task, wf, stdlib, inputs_json)
            self.assertIn("openwdl/wdl#773", str(ctx.exception), msg=str(bad))
        block_overrides = [
            # top-level key declared with a hints {} block
            {"wf.analyze.hints.aws": {"instance_type": "p5.24xlarge"}},
            # top-level key declared with an input {} block
            {"wf.analyze.hints.inputs": {"data": {"localization_optional": True}}},
        ]
        for bad in block_overrides:
            inputs_json = dict(base_json)
            inputs_json.update(bad)
            call_inputs, env = self._call_env(task, wf, stdlib, inputs_json)
            with self.assertRaises(WDL.Error.InputError, msg=str(bad)) as ctx:
                self._eval(task, call_inputs, env, stdlib)
            self.assertIn("openwdl/wdl#773", str(ctx.exception), msg=str(bad))

    def test_cli_hints_parity(self):
        # command-line t.hints.k=v inputs (WDL.CLI.runner_input) are handled
        # exactly like JSON input hints overrides: accepted against the
        # task's _hints placeholder, applied identically by _eval_task_hints,
        # and rejected identically (at input-read time) when dotted
        doc = WDL.parse_document(
            """version 1.2
            task t {
                input {
                    Int n = 4
                }
                command <<< true >>>
                hints {
                    max_cpu: n * 2
                    aws: hints {
                        instance_type: "p5.48xlarge"
                    }
                }
            }
            workflow wf {
                call t
            }
            """
        )
        doc.typecheck()
        task = doc.tasks[0]
        wf = doc.workflow
        assert wf is not None
        stdlib = StdLib.Base(task.effective_wdl_version)
        env: Env.Bindings[Value.Base] = Env.Bindings()
        env = env.bind("n", Value.Int(4))
        logger = logging.getLogger(self.id())

        def json_call_inputs(key, value):
            inputs = WDL.values_from_json(
                {"wf." + key: value}, wf.available_inputs, namespace="wf"
            )
            return inputs.enter_namespace("t")

        def cli_call_inputs(arg):
            _, input_env, _ = WDL.CLI.runner_input(doc, [arg], None, None, None)
            return input_env.enter_namespace("t")

        # an ordinary top-level override: same binding, same evaluated result
        # (compared via JSON, as Struct-of-Object values have no equality)
        json_inputs = json_call_inputs("t.hints.max_cpu", 32)
        cli_inputs = cli_call_inputs("t.hints.max_cpu=32")
        self.assertEqual(cli_inputs["hints.max_cpu"], Value.Int(32))
        self.assertEqual(
            {
                k: v.json
                for k, v in _eval_task_hints(logger, task, cli_inputs, env, stdlib).items()
            },
            {
                k: v.json
                for k, v in _eval_task_hints(logger, task, json_inputs, env, stdlib).items()
            },
        )
        self.assertEqual(
            _eval_task_hints(logger, task, cli_inputs, env, stdlib)["max_cpu"],
            Value.Int(32),
        )

        # a dotted (below-top-level) override is rejected identically on
        # both routes, while the inputs are being read: by values_from_json
        # for JSON, and by runner_input for the command line
        with self.assertRaises(WDL.Error.InputError) as ctx:
            json_call_inputs("t.hints.aws.instance_type", "p5.24xlarge")
        self.assertIn("openwdl/wdl#773", str(ctx.exception))
        with self.assertRaises(WDL.Error.InputError) as ctx:
            cli_call_inputs("t.hints.aws.instance_type=p5.24xlarge")
        self.assertIn("openwdl/wdl#773", str(ctx.exception))

    def test_eval_per_key_drop(self):
        # a hint that typechecks but fails to evaluate at runtime (division
        # by zero) is dropped individually, with a warning naming the key;
        # sibling hints are unaffected. A failure within a nested hints
        # object drops that top-level key as a whole.
        doc = WDL.parse_document(
            """version 1.2
            task t {
                input {
                    Int n = 0
                }
                command <<< true >>>
                hints {
                    max_cpu: 10 / n
                    short_task: true
                    bad_env: hints {
                        instance_count: 1 / n
                    }
                    aws: hints {
                        instance_type: "p5.48xlarge"
                    }
                }
            }
            """
        )
        doc.typecheck()
        task = doc.tasks[0]
        inputs = WDL.values_from_json(
            {"t.n": 0}, task.available_inputs, task.required_inputs, namespace="t"
        )
        env: Env.Bindings[Value.Base] = Env.Bindings()
        env = env.bind("n", inputs["n"])
        logger = logging.getLogger(self.id())
        with self.assertLogs(logger, level="WARNING") as logs:
            ans = _eval_task_hints(
                logger, task, inputs, env, StdLib.Base(task.effective_wdl_version)
            )
        self.assertEqual(set(ans), {"short_task", "aws"})
        self.assertEqual(ans["short_task"], Value.Boolean(True))
        self.assertEqual(ans["aws"].value, {"instance_type": Value.String("p5.48xlarge")})
        warnings = "\n".join(logs.output)
        self.assertIn("max_cpu", warnings)
        self.assertIn("bad_env", warnings)


if __name__ == "__main__":
    unittest.main()
