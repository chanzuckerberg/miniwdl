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

            self.assertFalse(task.command.parts[0].strip().startswith("{"))
            self.assertFalse(task.command.parts[0].strip().startswith("<<<"))
            self.assertFalse(task.command.parts[-1].strip().endswith("}"))
            self.assertFalse(task.command.parts[-1].strip().endswith(">>>"))

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

    def test_compare_md5sums(self):
        txt = """
task compare_md5sum {
	Array[String] labels
	Array[File] files
	Array[File] ref_files

	command <<<
		python <<CODE	
		from collections import OrderedDict
		import os
		import json
		import hashlib

		def md5sum(filename, blocksize=65536):
		    hash = hashlib.md5()
		    with open(filename, 'rb') as f:
		        for block in iter(lambda: f.read(blocksize), b""):
		            hash.update(block)
		    return hash.hexdigest()

		with open('${write_lines(labels)}','r') as fp:
			labels = fp.read().splitlines()
		with open('${write_lines(files)}','r') as fp:
			files = fp.read().splitlines()
		with open('${write_lines(ref_files)}','r') as fp:
			ref_files = fp.read().splitlines()

		result = OrderedDict()
		match = OrderedDict()
		match_overall = True

		result['tasks'] = []
		result['failed_task_labels'] = []
		result['succeeded_task_labels'] = []
		for i, label in enumerate(labels):
			f = files[i]
			ref_f = ref_files[i]
			md5 = md5sum(f)
			ref_md5 = md5sum(ref_f)
			# if text file, read in contents
			if f.endswith('.qc') or f.endswith('.txt') or \
				f.endswith('.log') or f.endswith('.out'):
				with open(f,'r') as fp:
					contents = fp.read()
				with open(ref_f,'r') as fp:
					ref_contents = fp.read()
			else:
				contents = ''
				ref_contents = ''
			matched = md5==ref_md5
			result['tasks'].append(OrderedDict([
				('label', label),
				('match', matched),
				('md5sum', md5),
				('ref_md5sum', ref_md5),
				('basename', os.path.basename(f)),
				('ref_basename', os.path.basename(ref_f)),
				('contents', contents),
				('ref_contents', ref_contents),
				]))
			match[label] = matched
			match_overall &= matched
			if matched:
				result['succeeded_task_labels'].append(label)
			else:
				result['failed_task_labels'].append(label)		
		result['match_overall'] = match_overall

		with open('result.json','w') as fp:
			fp.write(json.dumps(result, indent=4))
		match_tmp = []
		for key in match:
			val = match[key]
			match_tmp.append('{}\t{}'.format(key, val))
		with open('match.tsv','w') as fp:
			fp.writelines('\n'.join(match_tmp))
		with open('match_overall.txt','w') as fp:
			fp.write(str(match_overall))
		CODE
	>>>
	output {
		Map[String,String] match = read_map('match.tsv') # key:label, val:match
		Boolean match_overall = read_boolean('match_overall.txt')
		File json = glob('result.json')[0] # details (json file)
		String json_str = read_string('result.json') # details (string)
	}
	runtime {
		cpu : 1
		memory : "4000 MB"
		time : 1
		disks : "local-disk 50 HDD"		
	}
}
"""
        task = WDL.parse_tasks(txt, version="draft-2")[0]
        task.typecheck()
        self.assertEqual(len(task.command.parts), 7)
        self.assertFalse(task.command.parts[0].strip().startswith("<<<"))
        self.assertFalse(task.command.parts[-1].strip().endswith(">>>"))


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
        self.assertIsInstance(doc.workflow, WDL.Tree.Workflow)
        self.assertEqual(len(doc.workflow.elements), 2)
        self.assertEqual(len(doc.tasks), 2)
        doc.typecheck()

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
            command {
                samtools view -c ${bam}
            }
            runtime {
                docker: "quay.io/ucsc_cgl/samtools"
            }
            output {
                Int count = read_int(stdout())
            }
        }
        """
        doc = WDL.parse_document(doc, version="draft-2")
        self.assertIsInstance(doc.workflow, WDL.Tree.Workflow)
        self.assertEqual(len(doc.workflow.elements), 3)
        self.assertIsInstance(doc.workflow.elements[2], WDL.Tree.Scatter)
        self.assertEqual(len(doc.workflow.elements[2].elements), 1)
        self.assertEqual(len(doc.tasks), 2)
        self.assertEqual(doc.tasks[0].name, "slice_bam")
        self.assertEqual(len(doc.tasks[0].command.parts), 7)
        self.assertFalse(doc.tasks[0].command.parts[0].strip().startswith("<<<"))
        self.assertFalse(doc.tasks[0].command.parts[-1].strip().endswith(">>>"))
        self.assertFalse(doc.tasks[1].command.parts[0].strip().startswith("{"))
        self.assertFalse(doc.tasks[1].command.parts[-1].strip().endswith("}"))
        doc.typecheck()

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
                Int z = read_int(stdout())
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
        self.assertIsInstance(doc.workflow, WDL.Tree.Workflow)
        self.assertIsInstance(doc.workflow.elements[2], WDL.Tree.Scatter)
        self.assertIsInstance(doc.workflow.elements[2].elements[0], WDL.Tree.Scatter)
        self.assertEqual(len(doc.tasks), 1)
        doc.typecheck()
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
                Int z = read_int(stdout())
            }
            meta {
                foo: "bar"
            }
        }
        workflow contrived {
            Array[Int] xs = [1, 2, 3]
            Array[Int] ys = [4, 5, 6]
            scatter (x in xs) {
                Int x2_ = x*x
                scatter (y in ys) {
                    if (x + y < 5) {
                        Int xy_ = x * y
                        call sum { input:
                            x = x,
                            y = y
                        }
                    }
                }
            }
            output {
                Array[Array[Int?]] z = sum.z
                Array[Array[Int?]] xy = xy_
                Array[Int] x2 = x2_
            }
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()

    def test_errors(self):
        doc = r"""
        version 1.0
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
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
            doc.typecheck()

        doc = r"""
        version 1.0
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
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
            doc.typecheck()

        doc = r"""
        version 1.0
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
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
            doc.typecheck()

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
            doc.typecheck()

        doc = r"""
        version 1.0
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
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
            doc.typecheck()

        doc = r"""
        version 1.0
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
            }
        }
        task sum {
            command {}
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        doc = r"""
        version 1.0
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
            }
        }
        workflow sum {
            call sum
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        doc = r"""
        version 1.0
        task sum {
            Int x
            Int y
            File y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        doc = r"""
        workflow contrived {
            Int x
            if (true) {
                Int x = 1
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        doc = r"""
        import "x.wdl"
        import "x.wdl"
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        doc = r"""
        import "x.wdl"
        import "y.wdl" as x
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

    def test_task_forward_reference(self):
        doc = r"""
        version 1.0
        task sum {
            input {
                Int x = y
            }
            Int y = 42
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
            }
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()
        self.assertEqual(len(doc.tasks[0].command.parts), 5)

        # TODO: test circular reference

    def test_draft2_workflow_outputs(self):
        doc = r"""
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
            }
            meta {
                foo: "bar"
            }
        }
        workflow contrived {
            Array[Int] xs = [1, 2, 3]
            Array[Int] ys = [4, 5, 6]
            scatter (x in xs) {
                Int x2_ = x*x
                scatter (y in ys) {
                    if (x + y < 5) {
                        Int xy_ = x * y
                        call sum { input:
                            x = x,
                            y = y
                        }
                    }
                }
            }
            output {
                sum.z
            }
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()
        self.assertEqual(str(doc.workflow.outputs[0].type), "Array[Array[Int?]]")

        doc = WDL.parse_document("""
            workflow bogus {
                output {
                    z
                }
            }
        """)
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()

        doc = WDL.parse_document("""
            workflow bogus {
                output {
                    add.z
                }
            }
        """)
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()

        doc = WDL.parse_document("""
            workflow bogus {
                output {
                    nonex.*
                }
            }
        """)
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()

        doc = WDL.parse_document("""
            task sum {
                Int x
                Int y
                command <<<
                    echo $(( ~{x} + ~{y} ))
                >>>
                output {
                    Int z = read_int(stdout())
                }
                meta {
                    foo: "bar"
                }
            }
            workflow bogus {
                call sum as adder
                output {
                    Int z = 4
                    adder.z
                }
            }
        """)
        doc.typecheck()

        doc = WDL.parse_document("""
            task sum {
                Int x
                Int y
                command <<<
                    echo $(( ~{x} + ~{y} ))
                >>>
                output {
                    Int z = read_int(stdout())
                }
                meta {
                    foo: "bar"
                }
            }
            workflow bogus {
                call sum
                call sum as adder
                output {
                    sum.z
                    adder.z
                }
            }
        """)
        doc.typecheck()

        doc = WDL.parse_document("""
            task sum {
                Int x
                Int y
                command <<<
                    echo $(( ~{x} + ~{y} ))
                >>>
                output {
                    Int z = read_int(stdout())
                }
                meta {
                    foo: "bar"
                }
            }
            workflow bogus {
                call sum as adder
                output {
                    adder.z
                    adder.z
                }
            }
        """)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        with self.assertRaises(WDL.Error.SyntaxError):
            doc = WDL.parse_document("""
                version 1.0
                task sum {
                    Int x
                    Int y
                    command <<<
                        echo $(( ~{x} + ~{y} ))
                    >>>
                    output {
                        Int z = read_int(stdout())
                    }
                    meta {
                        foo: "bar"
                    }
                }
                workflow bogus {
                    call sum as adder
                    output {
                        adder.z
                    }
                }
            """)

        doc = WDL.parse_document("""
            task sum {
                Int x
                Int y
                command <<<
                    echo $(( ~{x} + ~{y} ))
                >>>
                output {
                    Int z = read_int(stdout())
                    Int w = 4
                }
                meta {
                    foo: "bar"
                }
            }
            workflow bogus {
                call sum
                call sum as adder
                output {
                    sum.*
                    adder.*
                }
            }
        """)
        doc.typecheck()
        self.assertEqual(set(decl.name for decl in doc.workflow.outputs),
                         set(["adder.w", "adder.z", "sum.w", "sum.z"]))

        doc = WDL.parse_document("""
            task sum {
                Int x
                Int y
                command <<<
                    echo $(( ~{x} + ~{y} ))
                >>>
                output {
                    Int z = read_int(stdout())
                    Int w = 4
                }
                meta {
                    foo: "bar"
                }
            }
            workflow bogus {
                call sum
                call sum as adder
                Int k = 4
                Int j = 5
                output {
                    sum.*
                    adder.*
                    k
                    Int j = j
                }
            }
        """)
        doc.typecheck()

    def test_workflow_inputs(self):
        doc = r"""
        version 1.0
        task sum {
            Int x
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
            }
            meta {
                foo: "bar"
            }
        }
        workflow contrived {
            input {
                Int x
                Int y
            }
            call sum { input:
                x = x,
                y = y
            }
            output {
                Int z = sum.z
            }
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()
        self.assertEqual(set(decl.name for decl in doc.workflow.inputs), set(["x", "y"]))

        doc = r"""
        version 1.0
        task sum {
            input {
                Int x = y
            }
            Int y
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.StrayInputDeclaration):
            doc.typecheck()

        doc = r"""
        workflow wf {
            input {
                Int x = y
            }
            Int y
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.StrayInputDeclaration):
            doc.typecheck()

        doc = r"""
        version 1.0
        task sum {
            input {
                Int x
                Int y
            }
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
            }
        }
        workflow wf {
            input {
                Int x = y
                Int y
                Int z = sum.z
            }
            call sum { input:
                x = x,
                y = y
            }
            output {
                Int z = z
            }
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()

    def test_multi_errors(self):
        doc = r"""
        version 1.0
        task bogus1 {
            Int x = "42"
            command {
            }
        }
        task bogus2 {
            input {
                Int x
                Int y
            }
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
                Int w = 17 + "42"
            }
        }
        """
        doc = WDL.parse_document(doc)
        try:
            doc.typecheck()
            assert False
        except WDL.Error.MultipleValidationErrors as multi:
            self.assertEqual(len(multi.exceptions), 2)

        doc = r"""
        version 1.0
        workflow bogus {
            Int x = "42"
            call sum { input:
                not_there = 21
            }
            output {
                File wrong = sum.z
                Int bogus = sum.not_there
            }
        }
        task sum {
            input {
                Int x
                Int y
            }
            command <<<
                echo $(( ~{x} + ~{y} ))
            >>>
            output {
                Int z = read_int(stdout())
            }
        }
        """
        doc = WDL.parse_document(doc)
        try:
            doc.typecheck()
            assert False
        except WDL.Error.MultipleValidationErrors as multi:
            self.assertEqual(len(multi.exceptions), 4)

        doc = r"""
        version 1.0
        workflow bogus {
            call not_there_1
            call not_there_2
        }
        """
        doc = WDL.parse_document(doc)
        try:
            doc.typecheck()
            assert False
        except WDL.Error.MultipleValidationErrors as multi:
            self.assertEqual(len(multi.exceptions), 2)

        doc = r"""
        version 1.0
        workflow bogus {
            File f
            Array[String] a = [f+1,f+2]
        }
        """
        doc = WDL.parse_document(doc)
        try:
            doc.typecheck()
            assert False
        except WDL.Error.MultipleValidationErrors as multi:
            self.assertEqual(len(multi.exceptions), 2)
