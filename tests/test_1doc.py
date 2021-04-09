import unittest, inspect, tempfile, os, pickle
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
            self.assertEqual(task.command.parts[1].expr._ident, "in")

            self.assertEqual(len(task.outputs), 1)
            self.assertEqual(str(task.outputs[0].type), "String")
            self.assertEqual(str(task.outputs[0].name), "ans")
            self.assertIsInstance(task.outputs[0].expr, WDL.Expr.Apply)

            task.typecheck()

            self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings(WDL.Env.Binding('in', WDL.Value.String("hello"))), WDL.StdLib.Base("1.0")).value, 'hello')

            self.assertFalse(task.command.parts[0].strip().startswith("{"))
            self.assertFalse(task.command.parts[0].strip().startswith("<<<"))
            self.assertFalse(task.command.parts[-1].strip().endswith("}"))
            self.assertFalse(task.command.parts[-1].strip().endswith(">>>"))

    def test_errors(self):
        with self.assertRaises(WDL.Error.InvalidType):
            WDL.parse_tasks("""
            task wc {
                input {
                    Int[Int] wrong
                }
                command {
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
        with self.assertRaises(WDL.Error.MultipleValidationErrors):
            WDL.parse_tasks("""
            task wc {
                input {
                    String in
                    File out = stdout()
                    Array[File] files = glob("*.txt")
                }
                command {
                    echo "~{in}" | wc
                }
                output {
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
        stdlib = WDL.StdLib.Base("1.0")
        self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('b', WDL.Value.Boolean(True)), stdlib).value, 'yes')
        self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('b', WDL.Value.Boolean(False)), stdlib).value, 'no')
        self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('b', WDL.Value.Null()), stdlib).value, '')

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
        self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('b', WDL.Value.Boolean(True)), stdlib).value, 'yes')
        self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('b', WDL.Value.Boolean(False)), stdlib).value, 'no')
        with self.assertRaises(WDL.Error.NullValue):
            self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('b', WDL.Value.Null()), stdlib).value, '')

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
        foobar = WDL.Value.Array(WDL.Type.String(), [WDL.Value.String("foo"), WDL.Value.String("bar")])
        self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('s', foobar), stdlib).value, 'foo, bar')
        foobar = WDL.Value.Array(WDL.Type.String(), [])
        self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('s', foobar), stdlib).value, '')
        with self.assertRaises(WDL.Error.IncompatibleOperand):
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
        self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('b', WDL.Value.Boolean(True)), stdlib).value, 'true')
        self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('b', WDL.Value.Boolean(False)), stdlib).value, 'false')
        self.assertEqual(task.command.parts[1].eval(WDL.Env.Bindings().bind('b', WDL.Value.Null()), stdlib).value, 'foo')

        task = WDL.parse_tasks("""
            task wc {
                input {
                    Boolean? b
                }
                output {
                    String ans = stdout()
                }
                command {
                    echo "${default='foo' b}"
                }
            }
            """)[0]
        task.typecheck()

        with self.assertRaises(WDL.Error.MultipleDefinitions):
            WDL.parse_tasks("""
                task wc {
                    input {
                        Boolean? b
                    }
                    output {
                        String ans = stdout()
                    }
                    command {
                        echo "${default='foo' b}"
                    }
                    output {
                        String ans2 = stdout()
                    }
                }
                """)[0]

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
                IntKey: 999
            }
            command {
                echo "~{true='yes' false='no' b}"
            }
            runtime {
                memory: "1 GB"
                cpu: 42
                runtime_key: 123
                meta_key: 321
            }
        }
        """)[0]
        task.typecheck()
        self.assertIsInstance(task.parameter_meta['b']['help'], str)
        self.assertEqual(task.parameter_meta['b']['help'], "it's a boolean")
        self.assertIsInstance(task.parameter_meta['IntKey'], WDL.Expr.Int)
        self.assertIsInstance(task.runtime['cpu'], WDL.Expr.Int)
        self.assertEqual(task.runtime['cpu'].value, 42)
        self.assertEqual(task.runtime['runtime_key'].value, 123)
        self.assertEqual(task.runtime['meta_key'].value, 321)
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
        self.assertIsInstance(task.meta['description'], str)
        self.assertEqual(task.meta['description'], "it's a task")

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


class TestTypes(unittest.TestCase):
    def test_parser(self):
        def check(t,v):
            doc_txt = r"""
            workflow contrived {{
                {t} x
                {t} y
                output {{
                    {t} o_y = y
                }}
            }}
            """.format(t=t)
            doc = WDL.parse_document(doc_txt, v)
            self.assertEqual(str(doc.workflow.body[0].type), t)
            self.assertEqual(doc.workflow.body[0].type.optional, t.endswith("?"))
            self.assertEqual(str(list(doc.workflow.effective_outputs)[0].value), t)
        for t in ["Int", "Int?",
                  "Array[Int]", "Array[Int]?", "Array[Int]+?", "Array[Int?]+?",
                  "Map[String,Int]", "Map[String,Array[Float?]+]?",
                  "Pair[Int,Float?]"]:
            check(t, "draft-2")
            check(t, "1.0")

    def test_invalid(self):
        def check(t,v):
            doc_txt = r"""
            workflow contrived {{
                {t} x
            }}
            """.format(t=t)
            WDL.parse_document(doc_txt, v)
        for t in ["Int+", "Array?+", "Array[Int,Float]", "Map[String]", "Map[Int,Int,Int]",
                  "Pair[Int]", "Pair[Float,Float,Float]", "Array[Pair[Int]?]+",
                  "Bogus", "bogus?"]:
            with self.assertRaises((WDL.Error.InvalidType, WDL.Error.SyntaxError)):
                check(t,"draft-2")
                check(t,"1.0")

    def test_unify(self):
        doc = WDL.parse_document("""
        workflow unify {
            String s
            File? f2
            Array[Int] a1 = [1]
            Array[Int?]? a2 = []
            Array[Pair[String,String]] ap = [(0,1),(2,3)]
            Array[Map[String,String]] am = [{ "a": 0, "b": 1 }, { "a": "x", "b": "y" }, { 1: 2, 3: 4 }]

            output {
                Array[File?] a = [s, f2]
                Array[Array[Int?]?] a3 = [a1, a2]
                Map[String, File?] m = { "foo": s, "bar": f2 }
                Map[Float, File?] m2 = { 1: s, 2.0: f2 }
            }
        }
        """)
        doc.typecheck()
        for outp in doc.workflow.outputs:
            self.assertIsNone(outp.expr.literal)

        with self.assertRaises(WDL.Error.ValidationError):
            doc = WDL.parse_document("""
            workflow unify {
                Array[Pair[String,String]] bogus = [("a","b"), ("c",("d","e"))]
            }
            """)
            doc.typecheck()

    def test_map_io(self):
        with self.assertRaisesRegex(WDL.Error.ValidationError, "keys cannot"):
            WDL.parse_document("""
            workflow w {
                input {
                    Map[Pair[Int,Int],String] m
                }
            }
            """).typecheck()

        with self.assertRaisesRegex(WDL.Error.ValidationError, "keys cannot"):
            WDL.parse_document("""
            task t {
                command {}
                output {
                    Map[Pair[Int,Int],String] m = read_json("bogus")
                }
            }
            """).typecheck()

class TestDoc(unittest.TestCase):
    def test_count_foo(self):
        doc = r"""#foo
workflow count_lines_matching {
    call grep
    call count_lines {
        input:
            in = grep.out  	# bar
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
    #baz   
task count_lines {
    File in

    command {
        wc -l ${in}
    }

    output {
        Int out = read_int(stdout())
    }
}
   #bas 
"""
        doc = WDL.parse_document(doc)
        self.assertIsInstance(doc.workflow, WDL.Tree.Workflow)
        self.assertEqual(len(doc.workflow.body), 2)
        self.assertEqual(len(doc.tasks), 2)
        doc.typecheck()
        self.assertEqual(doc.source_comments[0].text, "#foo")
        self.assertEqual(doc.source_comments[5].text, "  	# bar")
        self.assertEqual(doc.source_comments[20].text, "    #baz   ")
        self.assertEqual(doc.source_comments[32].text, "   #bas ")

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
        self.assertEqual(len(doc.workflow.body), 3)
        self.assertIsInstance(doc.workflow.body[2], WDL.Tree.Scatter)
        self.assertEqual(len(doc.workflow.body[2].body), 1)
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
        self.assertIsInstance(doc.workflow.body[2], WDL.Tree.Scatter)
        self.assertIsInstance(doc.workflow.body[2].body[0], WDL.Tree.Scatter)
        self.assertEqual(len(doc.tasks), 1)
        doc.typecheck()
        self.assertEqual(len(doc.imports), 2)
        self.assertEqual(doc.imports[0].uri, "x.wdl")
        self.assertEqual(doc.imports[0].namespace, "x")
        self.assertEqual(doc.imports[1].uri, "y.wdl")
        self.assertEqual(doc.imports[1].namespace, "z")

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

        b = [False]
        def check_scatter_depth(node, d=0):
            self.assertEqual(node.scatter_depth, d)
            if d>0:
                b[0] = True
            for ch in node.children:
                if isinstance(ch, WDL.Tree.WorkflowNode):
                    check_scatter_depth(ch, d=(d+1 if isinstance(node, WDL.Scatter) and not isinstance(ch, WDL.Tree.Gather) else d))
        for node in doc.workflow.body:
            check_scatter_depth(node)
        assert b[0]

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
        with self.assertRaisesRegex(WDL.Error.UnknownIdentifier, " in namespace "):
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
        with self.assertRaises(WDL.Error.NoSuchTask):
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
        workflow contrived {
            Int x
            output {
                Int x = 1
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        doc = r"""
        import "x.wdl"
        import "x.y.wdl"
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

        doc = r"""
        import "https://www.google.com/task.wdl"
        """
        with self.assertRaises(WDL.Error.SyntaxError):
            WDL.parse_document(doc)

        doc = r"""
        import "0"
        """
        with self.assertRaises(WDL.Error.SyntaxError):
            WDL.parse_document(doc)

        doc = r"""
        workflow wf {
            scatter (x in []) {
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.IndeterminateType):
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
        with self.assertRaises(WDL.Error.NoSuchTask):
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

        try:
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
            assert False
        except WDL.Error.SyntaxError as err:
            self.assertEqual(err.pos.line, 19)
            self.assertEqual(err.pos.column, 30)

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
                    Int j2 = j
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
        version 1.0
        task sum {
            input {
                Int x = select_first([y,42])
            }
            Int? y
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
        self.assertIn("x", doc.tasks[0].available_inputs)
        self.assertNotIn("x", doc.tasks[0].required_inputs)
        self.assertNotIn("y", doc.tasks[0].available_inputs)

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
        workflow wf {
            input {
                Int x = select_first([y, 42])
            }
            Int? y
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()
        self.assertIn("x", doc.workflow.available_inputs)
        self.assertNotIn("y", doc.workflow.available_inputs)

        doc = r"""
        workflow wf {
            input {
                Int x = y[0]
            }
            scatter (i in [1,2,3]) {
                Int y
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.StrayInputDeclaration):
            doc.typecheck()

        doc = r"""
        workflow wf {
            scatter (i in [1,2,3]) {
                Int y
            }
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.StrayInputDeclaration):
            doc.typecheck()

        doc = r"""
        workflow wf {
            input {
                Int x = select_first([y, 42])
            }
            Int? y
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()
        self.assertIn("x", doc.workflow.available_inputs)
        self.assertNotIn("y", doc.workflow.available_inputs)

        doc = r"""
        workflow wf {
            Int x
            scatter (i in [1,2,3]) {
                Int? y
            }
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()
        self.assertIn("x", doc.workflow.available_inputs)
        self.assertNotIn("y", doc.workflow.available_inputs)

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
                Int z2 = z+1
                Int w = x+y
                Array[Int] outs = [z,w]
            }
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()

    def test_multi_errors(self):
        doc = r"""
        version 1.0
        task bogus1 {
            Int x = false
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
                Int w = 17 + false
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
            Int x = false
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

    def test_issue135_workflow_available_inputs(self):
        # Workflow.available_inputs should not include declarations in the
        # output section
        doc = r"""
        workflow a {
            File in
            output {
                File out = in
            }
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()
        self.assertEqual(len(doc.workflow.available_inputs), 1)
        self.assertEqual(list(doc.workflow.available_inputs)[0].name, "in")

    def test_issue173_workflow_section_order(self):
        doc = r"""
        version 1.0
        workflow a {
            output {
                String d = c
            }
            String c = b
            input {
                String b
            }
            parameter_meta {
                b: "Test"
            }
            meta {
                test: "This is a test"
            }
        }
        """
        WDL.parse_document(doc).typecheck()

    def test_issue233_version_development(self):
        # bug where "version development" resulted in draft-2 grammar
        doc = r"""
        version development

        # struct won't parse under draft-2
        struct Car {
            String make
            String model
        }
        """
        WDL.parse_document(doc).typecheck()

class TestCycleDetection(unittest.TestCase):
    def test_task(self):
        doc = r"""
        version 1.0
        task cyclic {
            input {
                Int i
            }
            Int x = i + y
            Int y = i - x

            command{}
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

        doc = r"""
        version 1.0
        task cyclic {
            input {
                Int i
            }
            Int x = i + y
            Int y = i - z
            Int z = i * x

            command{}
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

        doc = r"""
        version 1.0
        task cyclic {
            input {
                Int i = i
            }

            command{}
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

    def test_workflow(self):
        add = r"""
        task add {
            input {
                Int lhs
                Int rhs
            }

            command{}

            output {
                Int z = lhs + rhs
            }
        }
        """
        doc = r"""
        version 1.0
        workflow cyclic {
            input {
                Int n = add.z
            }
            call add { input: lhs = 0, rhs = n }
        }
        """ + add
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

        doc = r"""
        version 1.0
        workflow cyclic {
            input {
                Int n = add.z
            }
            call add { input: lhs = 1, rhs = add2.z }
            call add as add2 { input: lhs = n, rhs = 0 }
        }
        """ + add
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

        doc = r"""
        version 1.0
        workflow cyclic {
            input {
                Boolean b
            }
            scatter (i in [1, 2, 3]) {
                call add { input: lhs = i, rhs = select_first([add2.z,0]) }
            }
            if (b) {
                call add as add2 { input: lhs = add.z[0], rhs = 0 }
            }
        }
        """ + add
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

        doc = r"""
        version 1.0
        workflow cyclic {
            if (select_first([c])) {
                Boolean b = true
            }
            if (select_first([b])) {
                Boolean c = true
            }
        }
        """ + add
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

        doc = r"""
        version 1.0
        workflow cyclic {
            scatter (i in arr) {
                Int j = i
            }
            Array[Int] arr = j
        }
        """ + add
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

        doc = r"""
        version 1.0
        workflow cyclic {
            call add { input: lhs = 0, rhs = add.z }
        }
        """ + add
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

class TestStruct(unittest.TestCase):
    def test_parser(self):
        doc = r"""
        version 1.0

        struct Person {
            String name
            Int age
        }

        struct Name {
            Array[File]+ myFiles
            Boolean? myBoolean
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()
        self.assertEqual(str(doc.struct_typedefs.resolve("Person").members["age"]), "Int")
        self.assertEqual(str(doc.struct_typedefs.resolve("Name").members["myFiles"]), "Array[File]+")

        doc = r"""
        version 1.0

        struct Person {
            String a
            Int a
        }
        """
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc = WDL.parse_document(doc)

        doc = r"""
        version 1.0

        struct Person {
            String a
        }

        struct Person {
            Int b
        }
        """
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc = WDL.parse_document(doc)

    def test_decl(self):
        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.InvalidType):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
            Person p2 = p
        }

        struct Person {
            String name
            Int age
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
            Person p2 = 0
        }

        struct Person {
            String name
            Int age
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
            Int k = p
        }

        struct Person {
            String name
            Int age
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
            Car c
            Person p2 = c
        }

        struct Person {
            String name
            Int age
        }

        struct Car {
            String make
            String model
            Int year
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaisesRegex(WDL.Error.StaticTypeMismatch, "Expected Person instead of Car"):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
            Car c
        }

        struct Person {
            String name
            Int age
        }

        struct Car {
            String make
            String model
            Int year
            Person driver
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
            Car c
        }

        struct Person {
            String name
            Int age
            Car vehicle
        }

        struct Car {
            String make
            String model
            Int year
            Person driver
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

        doc = r"""
        version 1.0

        struct Self {
            Int k
            Self me
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Car c
            Int year = c.year
            Int age = c.driver.age
            Int month = c.driver.birthday.left
        }

        struct Person {
            String name
            Int age
            Pair[Int,Int] birthday
        }

        struct Car {
            String make
            String model
            Int year
            Person driver
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person? p
            Int age = p.age
        }

        struct Person {
            String name
            Int age
            Pair[Int,Int] birthday
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person? p
            Int? age = p.age
        }

        struct Person {
            String name
            Int age
            Pair[Int,Int] birthday
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person? p
            Int month = p.birthday.left
        }

        struct Person {
            String name
            Int age
            Pair[Int,Int] birthday
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
            Int month = p.bogus
        }

        struct Person {
            String name
            Int age
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.NoSuchMember):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
            Int month = p.left
        }

        struct Person {
            String name
            Int age
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.NoSuchMember):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Array[Person]+ ps

            output {
                Int p0age = ps[0].age
            }
        }

        struct Person {
            String name
            Int age
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Array[Person]+ ps

            output {
                Int q0age = q[0].age
            }
        }

        struct Person {
            String name
            Int age
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person[Int,Float] bogus
        }

        struct Person {
            String name
            Int age
        }
        """
        with self.assertRaises(WDL.Error.InvalidType):
            WDL.parse_document(doc)

        # Test struct members with compound types including other structs
        # (issue #127)
        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
        }

        struct Person {
            String name
            Array[Pair[Car,Int]] cars
        }

        struct Car {
            String make
            String model
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
        }

        struct Person {
            String name
            Array[Person] circular
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

        doc = r"""
        version 1.0

        workflow UsePerson {
            Person p
        }

        struct Person {
            String name
            Array[Car] cars
        }

        struct Car {
            String make
            String model
            Person owner
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

    def test_import(self):
        doc = r"""
        version 1.0
        import "../test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/structs/GermlineStructs.wdl"
        """
        docfn = tempfile.mktemp(".wdl")
        with open(docfn,"w") as outfile:
            outfile.write(doc)
        doc = WDL.load(docfn, path=[os.path.dirname(__file__)])

        doc = r"""
        version 1.0
        import "../test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/structs/GermlineStructs.wdl"

        struct SampleAndUnmappedBams {
            Int bogus
        }
        """
        with open(docfn,"w") as outfile:
            outfile.write(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc = WDL.load(docfn, path=[os.path.dirname(__file__)])

        doc = r"""
        version 1.0
        import "../test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/structs/GermlineStructs.wdl" alias SampleAndUnmappedBams as SAUB

        struct SampleAndUnmappedBams {
            Int bogus
        }
        """
        with open(docfn,"w") as outfile:
            outfile.write(doc)
        doc = WDL.load(docfn, path=[os.path.dirname(__file__)])

        doc = r"""
        version 1.0
        import "../test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/structs/GermlineStructs.wdl" alias PapiSettings as SampleAndUnmappedBams
        """
        with open(docfn,"w") as outfile:
            outfile.write(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc = WDL.load(docfn, path=[os.path.dirname(__file__)])

        doc = r"""
        version 1.0
        import "../test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/structs/GermlineStructs.wdl" alias PapiSettings as Person

        struct Person {
            String name
        }
        """
        with open(docfn,"w") as outfile:
            outfile.write(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc = WDL.load(docfn, path=[os.path.dirname(__file__)])

        doc = r"""
        version 1.0
        import "../test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/structs/GermlineStructs.wdl" alias PapiSettings as PapiSettings
        """
        with open(docfn,"w") as outfile:
            outfile.write(doc)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc = WDL.load(docfn, path=[os.path.dirname(__file__)])

        doc = r"""
        version 1.0
        import "../test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/structs/GermlineStructs.wdl" alias Bogus as AlsoBogus
        """
        with open(docfn,"w") as outfile:
            outfile.write(doc)
        with self.assertRaises(WDL.Error.NoSuchMember):
            doc = WDL.load(docfn, path=[os.path.dirname(__file__)])

        os.unlink(docfn)

    def test_object_literal(self):
        doc = r"""
        version 1.0

        workflow wf {
            Person alyssa = object { name: "alyssa", "age": 42, 'birthday': (4,20) }
            call t { input: p = alyssa }
        }

        task t {
            input {
                Person p
                Car c
            }

            command <<<
                echo "Hello, ~{p.name}!"
            >>>

            output {
                Person p2 = object { name: stdout(), age: 99, birthday: (1,23) }
            }
        }

        struct Person {
            String name
            Int age
            Pair[Int,Int] birthday
        }

        struct Car {
            String make
            String model
            Int year
            Person driver
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()
        self.assertIsInstance(doc.workflow.body[0].expr.literal, WDL.Value.Struct)
        self.assertIsNone(doc.tasks[0].outputs[0].expr.literal)

        # quickly check task pickle-ability with struct types
        pickled_task = pickle.dumps(doc.tasks[0])
        unpickled_task = pickle.loads(pickled_task)
        assert unpickled_task.inputs[0].type.members
        self.assertEqual(unpickled_task.inputs[0].pos.line, 11)
        pickled_doc = pickle.dumps(doc)
        #self.assertLess(float(len(pickled_task))/len(pickled_doc), 0.6)

        doc = r"""
        version 1.0

        workflow wf {
            Person alyssa = object { name: "alyssa", "name": "ben" }
        }

        struct Person {
            String name
        }
        """
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc = WDL.parse_document(doc)

        doc = r"""
        version 1.0

        workflow wf {
            Person alyssa = object { name: "alyssa" }
            Person ben = object { name: "ben", age: 42, bogus: 3.14}
            Person cy = object { name: "ben", age: false,}
            Int i = object {}
        }

        struct Person {
            String name
            Int age
        }
        """
        doc = WDL.parse_document(doc)
        with self.assertRaises(WDL.Error.MultipleValidationErrors) as ctx:
            doc.typecheck()
        self.assertEqual(len(ctx.exception.exceptions), 4)
        for i in range(4):
            self.assertTrue(isinstance(ctx.exception.exceptions[i], WDL.Error.StaticTypeMismatch))
        self.assertEqual(str(ctx.exception.exceptions[2]), "Expected Person instead of object(age : Boolean, name : String)")

        doc = r"""
        version 1.0

        workflow wf {
            Array[Person] ppl = [
                object { name: 'alyssa', friends: [2,4] },
                object { "name": "ben", 'friends': [8,16]},
                object { 'name': "cy", "friends": [32,64] }
            ]
        }

        struct Person {
            String name
            Array[Int] friends
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()

    def test_keywords(self):
        templ = r"""
        version 1.0

        struct {} {{
            String {}
        }}
        """
        WDL.parse_document(templ.format("foo","bar")).typecheck()
        for p in [
            ("task","bar"),
            ("foo","task"),
            ("struct","bar"),
            ("foo","struct"),
            ("Int","bar"),
            ("foo","Int")
        ]:
            try:
                WDL.parse_document(templ.format(*p))
                assert False
            except WDL.Error.SyntaxError as err:
                self.assertIsInstance(err.pos.line, int)
                self.assertIsInstance(err.pos.column, int)

class TestNoneLiteral(unittest.TestCase):
    def test_none_expr(self):
        doc = r"""
        version development
        struct Car {
            String make
            String model
        }
        workflow wf {
            input {
                Int? x = None
                Array[Float?] ax = [3, None]
                Array[Car?] ac = [None]
            }
            Array[Int?] a = [x, None]
            if (x == None) {
                Boolean y = true
            }
            output {
                Boolean b = defined(y)
            }
        }
        """
        doc = WDL.parse_document(doc)
        doc.typecheck()

        assert WDL.Value.Null() == WDL.Value.Null()
        assert str(WDL.Value.Null()) == "None"
        assert str(doc.workflow.inputs[0]) == "Int? x = None"

    def test_none_type_errors(self):
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_document(r"""version development
            workflow w {
                Int x = None
            }
            """).typecheck()
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_document(r"""version development
            workflow w {
                Array[Int] ax = [3, None]
            }
            """).typecheck()
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc = WDL.parse_document(r"""version development
            workflow w {
                Array[Float] ax = [3, None]
            }
            """)
            doc.typecheck()
            assert False, str(doc.workflow.body[0].expr.type)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            WDL.parse_document(r"""version development
            struct Car {
                String make
                String model
            }
            workflow w {
                Array[Car] ax = [None]
            }
            """).typecheck()
        with self.assertRaises(WDL.Error.NoSuchMember):
            WDL.parse_document(r"""version development
            workflow w {
                Int x = None.left
            }
            """).typecheck()
        with self.assertRaises(WDL.Error.SyntaxError):
            WDL.parse_document(r"""version development
            struct Car {
                String make
                String model
            }
            workflow w {
                Car c
                String s = c.None
            }
            """).typecheck()
