import unittest, inspect, os
from typing import Optional
from .context import WDL

tsk = r"""
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
"""

class TestCalls(unittest.TestCase):

    def test_missing_input(self):
        txt = tsk + r"""
        workflow contrived {
            call sum
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()
        self.assertFalse(doc.workflow.complete_calls)

        txt = tsk + r"""
        workflow contrived {
            Int x
            call sum { input:
                x = x
            }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()
        self.assertFalse(doc.workflow.complete_calls)

        txt = tsk + r"""
        workflow contrived {
            Int w
            Int z
            call sum { input:
                x = w,
                y = z
            }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()
        self.assertTrue(doc.workflow.complete_calls)

    def test_duplicate_input(self):
        txt = tsk + r"""
        workflow contrived {
            Int x
            call sum { input:
                x = x,
                x = x
            }
        }
        """
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc = WDL.parse_document(txt)

    def test_optional(self):
        txt = tsk + r"""
        workflow contrived {
            Int? x
            call sum { input: x = x }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = tsk + r"""
        workflow contrived {
            Int? x = 0
            call sum { input: x = x }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = tsk + r"""
        workflow contrived {
            Int? x = 0
            String? s = "foo"
            Pair[Int,String] p = (x,s)
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)
        self.assertIsInstance(doc.workflow.body[0].expr.literal, WDL.Value.Int)
        self.assertIsInstance(doc.workflow.body[1].expr.literal, WDL.Value.String)
        self.assertIsNone(doc.workflow.body[2].expr.literal)

        txt = tsk + r"""
        workflow contrived {
            Int? x = 0
            Array[Int] y = [x]
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)
        self.assertIsNone(doc.workflow.body[1].expr.literal)

        # TODO: test quant checking in Map & other composite types

    def test_nonempty(self):
        txt = r"""
        task p {
            Array[Int]+ x
            command <<<
                echo "~{sep=', ' x}"
            >>>
            output {
                String z = stdout()
            }
        }
        workflow contrived {
            Array[Int] x
            Array[Int]+ y = x
            call p { input: x=x }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = r"""
        task p {
            Array[Int]+ x
            command <<<
                echo "~{sep=', ' x}"
            >>>
            output {
                String z = stdout()
            }
        }
        workflow contrived {
            Array[Int] x
            Array[Int]+ y = x
            call p { input: x=y }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = r"""
        workflow contrived {
            Array[Int] x = []
            Array[Int]+ y = [1]
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = r"""
        workflow contrived {
            Array[Int]+ y = []
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

    def test_array_coercion(self):
        txt = tsk + r"""
        workflow contrived {
            Array[Int] x = 1
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

    def test_collision(self):
        tasks = tsk + r"""
        task p {
            Array[Int]+ x
            command <<<
                echo "~{sep=', ' x}"
            >>>
            output {
                String z = stdout()
            }
        }
        """
        txt = tasks + r"""
        workflow contrived {
            call sum
            call sum as sum2
        }
        """
        WDL.parse_document(txt).typecheck()
        txt = tasks + r"""
        workflow contrived {
            call sum
            call sum
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            call sum
            call p as sum
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            call sum as foo
            call p as foo
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            if (true) {
                call sum as foo
            }
            scatter (i in [1,2]) {
                call p as foo
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            Int i
            scatter (i in [1,2]) {
                call sum
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            scatter (i in [1,2]) {
                Int i = 42
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            if (true) {
                call sum as foo
            }
            scatter (foo in [1,2]) {
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()
        txt = tasks + r"""
        workflow contrived {
            scatter (foo in [1,2]) {
                call p as foo
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        task_no_outputs = r"""
        task p {
            Array[Int]+ x
            command <<<
                echo "~{sep=', ' x}"
            >>>
        }
        """
        txt = task_no_outputs + r"""
        workflow contrived {
            call p
            call p
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        txt = task_no_outputs + r"""
        workflow contrived {
            call p as q
            call p as q
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        txt = task_no_outputs + r"""
        workflow contrived {
            call p as contrived
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

    def test_if_defined(self):
        # test how we typecheck a construct like
        #   if defined(x) then EXPR_WITH_x else SOME_DEFAULT
        txt = r"""
        workflow contrived {
            Int? x
            Int y = x
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = r"""
        workflow contrived {
            Int? x
            Int y = if defined(x) then x+1 else 42
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.IncompatibleOperand):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = tsk + r"""
        workflow contrived {
            Boolean b
            if (b) {
                call sum
            }
            Int y = sum.z
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)

        txt = tsk + r"""
        workflow contrived {
            Boolean b
            if (b) {
                call sum
            }
            call sum as s2
            Int y = if defined(sum.z) then sum.z+1 else s2.z
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.IncompatibleOperand):
            doc.typecheck()
        doc = WDL.parse_document(txt)
        doc.typecheck(check_quant=False)
        self.assertEqual(len(list(doc.workflow.effective_outputs)), 2)

    def test_forward_reference(self):
        txt = tsk + r"""
        workflow contrived {
            Int y = x
            Int x
            Array[Int?] w_out = w
            scatter (z in [1,2,3]) {
                if (true) {
                    Int w = z
                }
            }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Int y = z
            scatter (z in [1,2,3]) {
                call sum
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()
        txt = tsk + r"""
        workflow contrived {
            scatter (z in [1,2,3]) {
                call sum
            }
            Int y = z
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.UnknownIdentifier):
            doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Array[Int] s = sum.z
            scatter (z in [1,2,3]) {
                call sum { input: x = s2.z }
            }
            call sum as s2
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Array[Int]+? s = sum.z
            if (2 == 2) {
                scatter (z in [1,2,3]) {
                    call sum { input: x = s2.z }
                }
            }
            call sum as s2
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()
        assert(doc.workflow.body[0].type.nonempty and doc.workflow.body[0].type.optional)

        txt = tsk + r"""
        workflow contrived {
            call sum
            Int sum = 1
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

        txt = tsk + r"""
        workflow contrived {
            Int s = 1
            call sum as s
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.MultipleDefinitions):
            doc.typecheck()

    def test_recursion(self):
        txt = r"""
        workflow self {
            call self as c
            output {
                String s = "t"
            }
        }
        """
        doc = WDL.parse_document(txt)
        with self.assertRaises(WDL.Error.CircularDependencies):
            doc.typecheck()

    def test_io_propagation(self):
        # should not be able to call a workflow containing an incomplete call
        with self.assertRaises(WDL.Error.UncallableWorkflow):
            WDL.load(os.path.join(os.path.dirname(__file__), "../test_corpi/contrived/incomplete_call.wdl"))

        doc = WDL.load("file://" + os.path.join(os.path.dirname(__file__), "../test_corpi/contrived/incomplete.wdl"))
        self.assertEqual(len(doc.workflow.available_inputs), 4)
        self.assertTrue(doc.workflow.available_inputs.has_namespace("add"))
        doc.workflow.available_inputs.resolve("add.x")
        doc.workflow.available_inputs.resolve("add.y")
        doc.workflow.available_inputs.resolve("add.msg")
        doc.workflow.available_inputs.resolve("add.who")
        self.assertEqual(len(doc.workflow.required_inputs), 2)
        doc.workflow.required_inputs.resolve("add.x")
        doc.workflow.required_inputs.resolve("add.y")
        self.assertEqual(len(doc.workflow.effective_outputs), 1)
        doc.workflow.effective_outputs.resolve("add.z")
        with self.assertRaises(KeyError):  # negative control
            doc.workflow.effective_outputs.resolve("bogus.z")
        with self.assertRaises(KeyError):
            doc.workflow.effective_outputs.resolve("z")

        doc = WDL.load("file://" + os.path.join(os.path.dirname(__file__), "../test_corpi/contrived/incomplete_import.wdl"))
        self.assertEqual(len(doc.workflow.available_inputs), 2)
        doc.workflow.available_inputs.resolve("sum.msg")
        doc.workflow.available_inputs.resolve("sum.who")
        self.assertFalse(doc.workflow.required_inputs)

        doc = WDL.load("file://" + os.path.join(os.path.dirname(__file__), "../test_corpi/contrived/contrived.wdl"))
        self.assertEqual(len(doc.workflow.available_inputs), 7)
        doc.workflow.available_inputs.resolve("popular")
        doc.workflow.available_inputs.resolve("fortytwo")
        doc.workflow.available_inputs.resolve("required")
        self.assertTrue(doc.workflow.available_inputs.has_namespace("c1"))
        doc.workflow.available_inputs.resolve("c1.opt")
        self.assertTrue(doc.workflow.available_inputs.has_namespace("c2"))
        doc.workflow.available_inputs.enter_namespace("c2").resolve("opt")
        doc.workflow.available_inputs.enter_namespace("c2").resolve("i")
        doc.workflow.available_inputs.enter_namespace("c2").resolve("y")
        self.assertEqual(len(doc.workflow.required_inputs), 1)
        doc.workflow.required_inputs.resolve("required")

        doc = WDL.load("file://" + os.path.join(os.path.dirname(__file__), "../test_corpi/contrived/tricky_outputs.wdl"))
        self.assertEqual(len(doc.workflow.effective_outputs), 3)
        self.assertEqual(str(doc.workflow.effective_outputs.resolve("hello.message")), "Array[String]")
        self.assertEqual(str(doc.workflow.effective_outputs.resolve("hello2.message")), "Array[String]?")
        self.assertEqual(str(doc.workflow.effective_outputs.resolve("hello3.message")), "String")

    def test_new_struct_literals(self):
        txt = r"""
        version development
        struct Person {
            String name
            Int age
        }
        struct Car {
            String make
            String model
            Int odometer
            Person owner
        }
        workflow garage {
            call drive {
                input:
                car = Car {
                    make: "Toyota",
                    model: "Camry",
                    odometer: 139000,
                    owner: Person {
                        name: "Mario",
                        age: 42
                    }
                },
                miles = 3000
            }
            output {
                Car car = Car {
                    make: drive.car_out.make,
                    model: drive.car_out.model,
                    odometer: drive.car_out.odometer,
                    owner: Person {
                        name: "Luigi",
                        age: 39
                    }
                }
            }
        }
        task drive {
            input {
                Car car
                Int miles
            }
            command {}
            output {
                Car car_out = Car {
                    make: car.make,
                    model: car.model,
                    odometer: car.odometer + miles,
                    owner: car.owner
                }
            }
        }
        """
        WDL.parse_document(txt).typecheck

        defs = R"""
        version development
        struct Person {
            String name
            Int age
        }
        struct Car {
            String make
            String model
            Int odometer
            Person owner
        }
        """
        
        doc = WDL.parse_document(defs + """
            workflow w {
                Person p = Car {
                    name: "Mario",
                    age: 42
                }
            }
        """)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()

        doc = WDL.parse_document(defs + """
            workflow w {
                Car c = Car {
                    make: "Toyota",
                    model: "Camry",
                    odometer: 139000,
                    owner: Person {
                        name: "Mario"
                    }
                }
            }
        """)
        with self.assertRaises(WDL.Error.StaticTypeMismatch):
            doc.typecheck()

        doc = WDL.parse_document(defs + """
            workflow w {
                Car c = Car {
                    make: "Toyota",
                    model: "Camry",
                    odometer: 139000,
                    owner: Bogus {
                        name: "Mario",
                        age: 42
                    }
                }
            }
        """)
        with self.assertRaises(WDL.Error.InvalidType):
            doc.typecheck()

    def test_agc_hello(self):
        txt = r"""
        version 1.0
        workflow hello_agc {
            call hello {}
        }
        task hello {
            command { echo "Hello Amazon Genomics CLI!" }
            runtime {
                docker: "ubuntu:latest"
            }
            output { String out = read_string( stdout() ) }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()

        txt = r"""
        version 1.0
        workflow hello_agc {
            call hello as foo {}
        }
        task hello {
            command { echo "Hello Amazon Genomics CLI!" }
            runtime {
                docker: "ubuntu:latest"
            }
            output { String out = read_string( stdout() ) }
        }
        """
        doc = WDL.parse_document(txt)
        doc.typecheck()
