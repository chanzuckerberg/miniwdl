import unittest, inspect
from typing import Optional
from .context import WDL

class TestTasks(unittest.TestCase):
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
        #
        task wc {
            # comment
            input {
                String in
            }
            String d = in + "_foo"
            command <<<
                echo "~{in}" | wc
                echo "$USER" > /dev/null
                echo "$(env)" >> /dev/null
                echo "${HOME}" >> /dev/null
            >>> #
            # comment
            output { #comment
                String ans = stdout()
            }
        }
        """]
        for task_str in variants:
            task = WDL.parse_tasks(task_str)[0]
            self.assertEqual(len(task.inputs), 1)
            self.assertEqual(str(task.inputs[0]), "String in")

            self.assertEqual(len(task.command.parts), 3)
            self.assertEqual(task.command.parts[1].expr.name, "in")

            self.assertEqual(len(task.outputs), 1)
            self.assertEqual(str(task.outputs[0].type), "String")
            self.assertEqual(str(task.outputs[0].name), "ans")
            self.assertIsInstance(task.outputs[0].expr, WDL.Expr.Apply)

            task.typecheck()

            self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('in', WDL.Value.String("hello"), [])).value, 'hello')

    def test_errors(self):
        with self.assertRaises(WDL.Error.UnknownIdentifier, msg="Unknown identifier bogus"):
            WDL.parse_tasks("""
            task wc {
                input {
                    String in
                }
                command {
                    echo "~{bogus}" | wc
                }
            }
            """)[0].typecheck()
        with self.assertRaises(WDL.Error.UnknownIdentifier, msg="Unknown identifier bogus"):
            WDL.parse_tasks("""
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
            """)[0].typecheck()

    def test_placeholders(self):
        task = WDL.parse_tasks("""
            task wc {
                input {
                    Boolean b
                }
                command <<<
                    echo "~{true='yes' false='no' b}"
                >>>
            }
            """)[0]
        task.typecheck()
        self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('b', WDL.Value.Boolean(True), [])).value, 'yes')
        self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('b', WDL.Value.Boolean(False), [])).value, 'no')
        self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('b', WDL.Value.Null(), [])).value, '')

        task = WDL.parse_tasks("""
            task wc {
                input {
                    Boolean b
                    File? f
                }
                command {
                    echo "${if b then 'yes' else 'no'}"
                }
            }
            """)[0]
        task.typecheck()
        self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('b', WDL.Value.Boolean(True), [])).value, 'yes')
        self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('b', WDL.Value.Boolean(False), [])).value, 'no')
        with self.assertRaises(WDL.Error.NullValue):
            self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('b', WDL.Value.Null(), [])).value, '')

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_tasks("""
                task wc {
                    input {
                        Int b
                    }
                    command {
                        echo "~{true='yes' false='no' b}"
                    }
                }
                """)[0].typecheck()

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_tasks("""
                task wc {
                    command {
                        echo "~{true='yes' false='no' 42}"
                    }
                }
                """)[0].typecheck()

        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_tasks("""
                task wc {
                    input {
                        Boolean b
                    }
                    command {
                        echo "~{false='no' b}"
                    }
                }
                """)[0].typecheck()

        task = WDL.parse_tasks("""
            task wc {
                input {
                    Array[String] s
                }
                command <<<
                    echo "~{sep=', ' s} baz"
                >>>
            }
            """)[0]
        task.typecheck()
        foobar = WDL.Value.Array(WDL.Type.Array(WDL.Type.String()), [WDL.Value.String("foo"), WDL.Value.String("bar")])
        self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('s', foobar, [])).value, 'foo, bar')
        foobar = WDL.Value.Array(WDL.Type.Array(WDL.Type.String()), [])
        self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('s', foobar, [])).value, '')
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            task = WDL.parse_tasks("""
            task wc {
                input {
                    Array[String] s
                }
                command <<<
                    echo "~{s} baz"
                >>>
            }
            """)[0].typecheck()
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_tasks("""
            task wc {
                input {
                    String s
                }
                command <<<
                    echo "~{sep=', ' s} baz"
                >>>
            }
            """)[0].typecheck()

        task = WDL.parse_tasks("""
            task wc {
                input {
                    Boolean? b
                }
                command {
                    echo "${default='foo' b}"
                }
            }
            """)[0]
        task.typecheck()
        self.assertTrue(task.inputs[0].type.optional)
        self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('b', WDL.Value.Boolean(True), [])).value, 'true')
        self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('b', WDL.Value.Boolean(False), [])).value, 'false')
        self.assertEqual(task.command.parts[1].eval(WDL.Env.bind('b', WDL.Value.Null(), [])).value, 'foo')

    def test_meta(self):
        task = WDL.parse_tasks("""
        task wc {
            input {
                Boolean? b
                Array[Int]+ n
            }
            String dollar = "$"
            String lbrace = "{"
            String rbrace = "}"
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
        """)[0]
        task.typecheck()
        self.assertIsInstance(task.parameter_meta['b']['help'], WDL.Expr.String)
        self.assertEqual(task.parameter_meta['b']['help'].parts, ['"', "it's a boolean", '"'])
        self.assertIsInstance(task.runtime['cpu'], WDL.Expr.Int)
        self.assertEqual(task.runtime['cpu'].value, 42)
        self.assertTrue(task.inputs[0].type.optional)
        self.assertFalse(task.inputs[1].type.optional)
        self.assertTrue(task.inputs[1].type.nonempty)

        task = WDL.parse_tasks(r"""
        task wc {
            input {
                Boolean? b
                Array[Int]+ n
            }
            String a_nonput_decl = "foo"
            meta {
                description: 'it\'s a task'
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
        """)[0]
        task.typecheck()
        self.assertIsInstance(task.meta['description'], WDL.Expr.String)
        self.assertEqual(task.meta['description'].parts, ["'", "it\\'s a task", "'"])


class TestDoc(unittest.TestCase):
    def test_count_foo(self):
        doc = r"""
        workflow count_lines_matching {
            call grep
            call count_lines {
                input:
                    in = grep.out
            }
        }
        task grep {
            File in
            String pattern

            command {
                grep ${pattern} ${in} > ans
            }

            output {
                File out = "ans"
            }
        }
        task count_lines {
            File in

            command {
                wc -l ${in}
            }

            output {
                Int out = read_int(stdout())
            }
        }
        """
        doc = WDL.parse_document(doc)
        self.assertIsInstance(doc.workflow, WDL.Document.Workflow)
        self.assertEqual(len(doc.workflow.elements), 2)
        self.assertEqual(len(doc.tasks), 2)
        doc.workflow.typecheck(doc)

    def test_bam_chrom_counter(self):
        doc = r"""
        workflow bam_chrom_counter {
            File bam

            call slice_bam {
                input : bam = bam
            }
            scatter (slice in slice_bam.slices) {
                call count_bam {
                    input: bam = slice
                }
            }
            output {
                File bai = slice_bam.bai
                Array[Int] count = count_bam.count
            }
        }

        task slice_bam {
            File bam
            Int num_chrom = 22
            command <<<
            set -ex
            samtools index ${bam}
            mkdir slices/
            for i in `seq ${num_chrom}`; do
                samtools view -b ${bam} -o slices/$i.bam $i
            done
            >>>
            runtime {
                docker: "quay.io/ucsc_cgl/samtools"
            }
            output {
                File bai = "${bam}.bai"
                Array[File] slices = glob("slices/*.bam")
            }
        }

        task count_bam {
            File bam
            command <<<
                samtools view -c ${bam}
            >>>
            runtime {
                docker: "quay.io/ucsc_cgl/samtools"
            }
            output {
                Int count = read_int(stdout())
            }
        }
        """
        doc = WDL.parse_document(doc)
        self.assertIsInstance(doc.workflow, WDL.Document.Workflow)
        self.assertEqual(len(doc.workflow.elements), 3)
        self.assertIsInstance(doc.workflow.elements[2], WDL.Document.Scatter)
        self.assertEqual(len(doc.workflow.elements[2].elements), 1)
        self.assertEqual(len(doc.tasks), 2)
        doc.workflow.typecheck(doc)

    def test_nested_scatter(self):
        doc = r"""
        import "x.wdl"
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = stdout()
            }
        }
        import "y.wdl" as z
        workflow contrived {
            Array[Int] xs = [1, 2, 3]
            Array[Int] ys = [4, 5, 6]
            scatter (x in xs) {
                scatter (y in ys) {
                    call sum { input:
                        x = x,
                        y = y
                    }
                }
            }
            output {
                Array[Array[Int]] z = sum.z
            }
        }
        """
        doc = WDL.parse_document(doc)
        self.assertIsInstance(doc.workflow, WDL.Document.Workflow)
        self.assertIsInstance(doc.workflow.elements[2], WDL.Document.Scatter)
        self.assertIsInstance(doc.workflow.elements[2].elements[0], WDL.Document.Scatter)
        self.assertEqual(len(doc.tasks), 1)
        doc.workflow.typecheck(doc)
        self.assertEqual(doc.imports, [("x.wdl","x",None), ("y.wdl","z",None)])

    def test_scatter_conditional(self):
        doc = r"""
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = stdout()
            }
            meta {
                foo: "bar"
            }
        }
        workflow contrived {
            Array[Int] xs = [1, 2, 3]
            Array[Int] ys = [4, 5, 6]
            scatter (x in xs) {
                scatter (y in ys) {
                    if (x + y < 5) {
                        call sum { input:
                            x = x,
                            y = y
                        }
                    }
                }
            }
            output {
                Array[Array[Int?]] z = sum.z
            }
        }
        """
        doc = WDL.parse_document(doc)
        doc.workflow.typecheck(doc)

    def test_errors(self):
        doc = r"""
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = stdout()
            }
        }
        workflow contrived {
            Int not_array
            scatter (x in not_array) {
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.NotAnArray):
            doc.workflow.typecheck(doc)

        doc = r"""
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = stdout()
            }
        }
        workflow contrived {
            call sum { input:
                x = 1,
                z = 0
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.NoSuchInput):
            doc.workflow.typecheck(doc)

        doc = r"""
        version 1.0
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = stdout()
            }
        }
        workflow contrived {
            call sum { input:
                x = 1,
                y = 2
            }
            output {
                Int z = sum.bogus
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.workflow.typecheck(doc)

        doc = r"""
        workflow contrived {
            call bogus { input:
                x = 1,
                y = 2
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.workflow.typecheck(doc)

        doc = r"""
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = stdout()
            }
        }
        workflow contrived {
            if ([1]) {
                call sum { input:
                    x = 1, y = 2
                }
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.workflow.typecheck(doc)
