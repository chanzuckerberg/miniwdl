import unittest

from .context import WDL
from WDL import Env, StdLib, Tree, Value
from WDL.runtime.task import IOHintsValue, eval_task_hints

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


class TestWorkflowHintsFold(unittest.TestCase):
    def test_bare_key_objects(self):
        # bare identifiers in key position fold to their names, equivalently
        # to quoted keys, at any nesting depth
        doc = WDL.parse_document(
            """version 1.2
            workflow w {
                hints {
                    allow_nested_inputs: true
                    labels: {team: "genomics", "priority": 3}
                    config: {limits: {cpu: 4}}
                }
            }
            """
        )
        self.assertEqual(
            doc.workflow.hints,
            {
                "allow_nested_inputs": True,
                "labels": {"team": "genomics", "priority": 3},
                "config": {"limits": {"cpu": 4}},
            },
        )

    def test_nonliteral_key(self):
        # a dotted name or a compound value is not a plain key name/literal
        for key in ("a.b", "[1]"):
            doc = """version 1.2
            workflow w {
                hints {
                    labels: {%s: "x"}
                }
            }
            """ % (
                key,
            )
            with self.assertRaises(WDL.Error.SyntaxError):
                WDL.parse_document(doc)

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


class TestHintsEval(unittest.TestCase):
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
        ans = eval_task_hints(task, inputs, env, StdLib.Base(task.effective_wdl_version))
        self.assertEqual(ans["max_cpu"], Value.Int(12))  # override superseded 24
        self.assertEqual(ans["max_memory"], Value.String("36 GB"))
        self.assertEqual(ans["short_task"], Value.Boolean(True))
        self.assertEqual(ans["localization_optional"], Value.Boolean(False))
        inputs_val = ans["inputs"]
        self.assertIsInstance(inputs_val, IOHintsValue)
        self.assertEqual(inputs_val.kind, "input")
        self.assertEqual(
            inputs_val.entries, {"foo": {"localization_optional": Value.Boolean(True)}}
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
        ans = eval_task_hints(task, call_inputs, env, stdlib)
        self.assertEqual(ans["max_cpu"], Value.Int(16))
        self.assertEqual(ans["max_memory"], Value.String("32 GB"))
        self.assertEqual(
            ans["aws"],
            {"instance_type": Value.String("p5.48xlarge"), "max_cpu": Value.Int(96)},
        )
        inputs_val = ans["inputs"]
        self.assertIsInstance(inputs_val, IOHintsValue)
        self.assertEqual(inputs_val.kind, "input")
        self.assertEqual(
            inputs_val.entries["person.cv"], {"localization_optional": Value.Boolean(True)}
        )
        self.assertEqual(
            inputs_val.entries["data"]["streaming_min_size_mb"], Value.Int(10)
        )
        outputs_val = ans["outputs"]
        self.assertIsInstance(outputs_val, IOHintsValue)
        self.assertEqual(outputs_val.kind, "output")
        self.assertEqual(outputs_val.entries, {"report": {"max_length": Value.Int(5)}})

    def test_eval_overrides_replace(self):
        # JSON overrides replace the declared value of their top-level key
        # wholesale (they do not deep-merge into it)
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
                "wf.analyze.hints.aws.instance_type": "p5.24xlarge",
                "wf.analyze.hints.inputs.data.localization_optional": True,
            },
        )
        ans = eval_task_hints(task, call_inputs, env, stdlib)
        self.assertEqual(ans["max_cpu"], Value.Int(32))
        # the whole aws object was superseded: declared max_cpu is gone
        self.assertEqual(ans["aws"], {"instance_type": Value.String("p5.24xlarge")})
        # the whole inputs block was superseded: the person.cv entry is gone,
        # but the kind tag is retained
        inputs_val = ans["inputs"]
        self.assertIsInstance(inputs_val, IOHintsValue)
        self.assertEqual(inputs_val.kind, "input")
        self.assertEqual(
            inputs_val.entries, {"data": {"localization_optional": Value.Boolean(True)}}
        )
        # untouched keys still come from the WDL declarations
        self.assertEqual(ans["max_memory"], Value.String("32 GB"))


if __name__ == "__main__":
    unittest.main()
