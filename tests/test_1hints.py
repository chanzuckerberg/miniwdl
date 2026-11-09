import unittest

import WDL.Lint
from WDL import Tree, Type

from .context import WDL


class TestWorkflowHintsAST(unittest.TestCase):
    def test_ast_representation(self):
        # workflow hints are retained as ASTs in the same representation as
        # task hints
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

    def test_none_values(self):
        # None is a literal workflow hint value, on its own and inside
        # compound literals and nested hints blocks
        doc = WDL.parse_document(
            """version 1.2
            workflow w {
                hints {
                    fallback: None
                    retries: [1, None]
                    group: hints { contact: None }
                }
            }
            """
        )
        doc.typecheck()
        self.assertEqual(doc.workflow.hints["fallback"].literal, WDL.Value.Null())

    def test_nonliteral_values(self):
        # expressions of any kind are rejected in workflow hints, including
        # function calls, name lookups, arithmetic, and string interpolation;
        # the rejection happens at typecheck, not parse
        for value in ('length(names) > 0', "names", "1 + 2", '"hello ~{names[0]}"'):
            doc = f"""version 1.2
            workflow w {{
                input {{
                    Array[String] names
                }}
                hints {{
                    allow_nested_inputs: {value}
                }}
            }}
            """
            with self.assertRaises(WDL.Error.ValidationError, msg=value):
                WDL.parse_document(doc).typecheck()

    def test_nonliteral_key(self):
        # map keys are expressions too: a bare identifier is a name lookup (it
        # does NOT fold to its name), and so is member access; both are
        # rejected at typecheck, like non-literal values
        for key in ("team", "a.b"):
            doc = f"""version 1.2
            workflow w {{
                hints {{
                    labels: {{{key}: "x"}}
                }}
            }}
            """
            with self.assertRaises(WDL.Error.ValidationError, msg=key):
                WDL.parse_document(doc).typecheck()

    def test_compound_map_key(self):
        # a compound map key is a literal, so the literal check passes it;
        # it's rejected by the map-key rule for values crossing the JSON
        # boundary (like input/output keys), naming the hint
        doc = """version 1.2
        workflow w {
            hints {
                labels: {[1]: "x"}
            }
        }
        """
        with self.assertRaises(WDL.Error.ValidationError) as ctx:
            WDL.parse_document(doc).typecheck()
        self.assertIn("workflow hint 'labels'", str(ctx.exception))

    def test_io_block_rejected(self):
        doc = """version 1.2
        workflow w {
            hints {
                inputs: input { x: hints { } }
            }
        }
        """
        with self.assertRaises(WDL.Error.ValidationError):
            WDL.parse_document(doc)
        # likewise nested within a hints object in the workflow hints section
        doc = """version 1.2
        workflow w {
            hints {
                group: hints { outputs: output { y: hints { } } }
            }
        }
        """
        with self.assertRaises(WDL.Error.ValidationError):
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
        # runs to completion
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

    def test_compound_map_key(self):
        # task hint values cross the JSON boundary (overrides, backends), so
        # their map keys are checked like input/output keys; a compound key
        # is rejected at typecheck, naming the hint
        doc = WDL.parse_document(
            """version 1.2
            task t {
                command <<< true >>>
                hints {
                    labels: {[1]: "x"}
                }
            }
            """
        )
        with self.assertRaises(WDL.Error.ValidationError) as ctx:
            doc.typecheck()
        self.assertIn("task hint 'labels'", str(ctx.exception))

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
            "disks: 42",  # not String|Map
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

    def test_reserved_key_block_value(self):
        # a reserved key may not take a hints block value, which would
        # otherwise bypass the accepted-types check (the check runs on
        # expression values only)
        for hint in ("max_cpu: hints { }", "short_task: hints { custom: 1 }"):
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
            with self.assertRaises(WDL.Error.ValidationError, msg=hint):
                doc.typecheck()
        # likewise nested within a compute-environment hints object
        doc = WDL.parse_document(
            """version 1.2
            task t {
                command <<< true >>>
                hints {
                    aws: hints {
                        max_cpu: hints { }
                    }
                }
            }
            """
        )
        with self.assertRaises(WDL.Error.ValidationError):
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

    def test_reserved_key_accepted_alternatives(self):
        # exercise accepted type alternatives and camelCase aliases the main
        # acceptances test doesn't reach (its Float case uses the
        # unconstrained key max_cpu_f, which tests nothing about max_cpu)
        doc = WDL.parse_document(
            """version 1.2
            task t {
                command <<< true >>>
                hints {
                    max_cpu: 2.5
                    maxMemory: "16 GB"
                    disks: "local-disk 100 SSD"
                    localizationOptional: true
                }
            }
            """
        )
        doc.typecheck()
        # the aliases carry the same type restrictions as their snake_case
        # names (if an alias fell out of the table, these would typecheck as
        # unconstrained keys)
        for hint in ("maxMemory: 1.5", "localizationOptional: 1"):
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

    def test_reserved_key_acceptances_lax(self):
        # the reserved-key check follows Lint's UnexpectedRuntimeValue:
        # accepted types are type classes checked with isinstance, so map
        # value types and optionality aren't distinguished
        doc = WDL.parse_document(
            """version 1.2
            task t {
                input {
                    Int? n
                }
                command <<< true >>>
                hints {
                    max_cpu: n
                    disks: {"/mnt": 3}
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
        # unknown input name: a specific error at the key's position
        doc = WDL.parse_document(
            header % "inputs: input { bogus: hints { localization_optional: true } }"
        )
        with self.assertRaises(WDL.Error.ValidationError) as ctx:
            doc.typecheck()
        self.assertEqual(str(ctx.exception), "task t has no input named 'bogus'")
        # unknown struct member in a dotted path
        doc = WDL.parse_document(
            header % "inputs: input { person.nme: hints { min_length: 3 } }"
        )
        with self.assertRaises(WDL.Error.NoSuchMember):
            doc.typecheck()
        # outputs block may not name an input
        doc = WDL.parse_document(header % "outputs: output { data: hints { } }")
        with self.assertRaises(WDL.Error.ValidationError) as ctx:
            doc.typecheck()
        self.assertEqual(str(ctx.exception), "task t has no output named 'data'")
        # inputs must be an input block (not an output block, nor an expression)
        for bad in ("inputs: output { data: hints { } }", "inputs: 42"):
            doc = WDL.parse_document(header % bad)
            with self.assertRaises(WDL.Error.ValidationError, msg=bad):
                doc.typecheck()
        # each entry value must be a hints block (spec: the members of an
        # input/output block are all of type hints), not an expression
        for bad in (
            "inputs: input { data: true }",
            "inputs: input { data: {} }",
            "outputs: output { report: 42 }",
        ):
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
        with self.assertRaises(WDL.Error.ValidationError) as ctx:
            doc.typecheck()
        self.assertEqual(str(ctx.exception), "task t has no output named 'summary'")

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
        with self.assertRaises(WDL.Error.ValidationError) as ctx:
            doc.typecheck()
        self.assertEqual(str(ctx.exception), "task t has no input named 'priv'")
        # the error points at the offending key (line 10), not the whole block
        self.assertEqual(ctx.exception.pos.line, 10)
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
        with self.assertRaises(WDL.Error.ValidationError) as ctx:
            doc.typecheck()
        self.assertEqual(
            str(ctx.exception),
            "task has both runtime and hints sections, which are mutually exclusive in WDL 1.2",
        )
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


if __name__ == "__main__":
    unittest.main()
