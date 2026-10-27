# WDL assertion statements

This lab will walk through prototyping a significant extension to the WDL language: an `assert` statement to be checked at runtime. For example,

```wdl
version development
workflow div {
    input {
        Int numerator
        Int denominator
    }
    assert denominator != 0
    output {
        Int quotient = numerator / denominator
    }
}
```

We'll cover how to add the new construct to the language grammar and syntax tree, implementing the runtime assertion checking for tasks and workflows, and adding appropriate cases to the unit test suite. This builds on the basic miniwdl development process introduced in the previous codelab on [adding library functions](add_functions.md).

**Refer to the complete git diff for this example on the [codelab-assert](https://github.com/chanzuckerberg/miniwdl/pull/319/files) branch.**

## Syntax tree model

Among existing language constructs, the new assertion statement is most similar to a value declaration ({py:obj}`WDL.Decl <WDL.Tree.Decl>`), insofar as it entails evaluation of a single WDL expression after any dependencies it refers to. Imagine the statement `assert :expr:` translating to the declaration `Boolean some_unique_name = :expr:`, with the special behavior that the runner should raise an error if it evaluates to false.

We'll introduce the new statement to the syntax tree model by subclassing `WDL.Decl` in `WDL/Tree.py`,

```python3
class Assertion(Decl):
    message: str

    def __init__(self, pos: SourcePosition, expr: Expr.Base) -> None:
        super().__init__(pos, Type.Boolean(), f"_assert_L{pos.line}C{pos.column}", expr)
        self.message = f"assertion failed: {expr} ({pos.uri} Ln {pos.line} Col {pos.column})"
```

The new subclass initializes its {py:obj}`WDL.Decl <WDL.Tree.Decl>` base with the boolean type, an arbitrary unique name, and the WDL expression to be evaluated. Actual WDL declaration names can't start with an underscore, so we can be sure this name won't cause collisions.

A major advantage of this approach is that the workflow runner "sees" the new construct as a declaration, inheriting all the existing logic for analyzing the expression's dependencies and scheduling its evaluation. It also inherits the static type-checking logic to verify that the expression can only evaluate to a boolean value.

Lastly, our constructor pre-computes the assertion failure message for later use.

## Grammar and parser

Miniwdl uses [Lark](https://github.com/lark-parser/lark) for basic lexing/parsing, entailing (i) an [EBNF grammar](https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_form) hard-coded as a lengthy string in [`WDL/_grammar.py`](https://github.com/chanzuckerberg/miniwdl/blob/main/WDL/_grammar.py), and (ii) "transformers" to map Lark's low-level syntax tree onto the `WDL.Tree` object model, found in [`WDL/_parser.py`](https://github.com/chanzuckerberg/miniwdl/blob/main/WDL/_parser.py). The former source file furthermore includes several versions of the grammar for different WDL language versions, and the latter the mechanism for choosing the appropriate grammar based on the `version` declaration in the source code.

We'll introduce our new assertion statement to the "development" version of the grammar. In `WDL/_grammar.py` we add the production,

```python3
...
versions["development"] = r"""
...
assertion: "assert" expr
...
```

Reusing the existing `expr` production. Then we can let the statement appear in the body of a task, alongside any of its "non-input" declarations.

```text
task: "task" CNAME "{" task_section* command task_section* "}"
?task_section: task_input_decls
             | output_decls
             | meta_section
             | requirements_section
             | runtime_section
             | hints_section
             | task_env_decl -> noninput_decl
             | assertion -> noninput_decl
```

We also add `assert` to the development version's set of language keywords, which can't be used as names:

```python3
keywords["development"] = keywords["development"] | {"assert"}
```

(This creates a new set, rather than modifying the existing one in place, which may be shared with the current WDL language version.)

Next, in `WDL/_parser.py` we add to the `_DocTransformer` class,

```python3
...
class _DocTransformer:
    ...

    def assertion(self, meta, items):
        if self._version == "development":
            return Tree.Assertion(self._sp(meta), items[0])
        ...  # raise appropriate error
```

Lark calls this method when the new `assert` statement appears in the source code, upon which we create an instance of our new `Assertion` node based on the source position and expression.

Even though we haven't yet added the logic to actually check the asserted condition at runtime, we can begin writing our test cases to check that the new statement is recognized syntactically. In [`tests/test_7runner.py`](https://github.com/chanzuckerberg/miniwdl/blob/main/tests/test_7runner.py) we add a new test case class deriving from the `RunnerTestCase(unittest.TestCase)` helper,

```python3
...
class TestAssert(RunnerTestCase):
    task1 = R"""
    version development
    task div {
        input {
            Int numerator
            Int denominator
        }
        assert denominator != 0
        command {
            expr ~{numerator} / ~{denominator}
        }
        output {
            Int quotient = read_int(stdout())
        }
    }
    """

    def test_positive(self):
        outputs = self._run(self.task1, {"numerator": 7, "denominator": 2})
        self.assertEqual(outputs["quotient"], 3)
```

The `self._run()` helper method runs the given WDL on the inputs and returns the outputs. Run the new test with `python3 -m unittest tests.test_7runner.TestAssert`; the runner evaluates the assertion as if it were a declaration, but doesn't yet care about the outcome.

## Checking assertions in tasks

The task runtime logic is found in [`WDL/runtime/task.py`](https://github.com/chanzuckerberg/miniwdl/blob/main/WDL/runtime/task.py). We'll add the assertion checking logic to the `_eval_task_inputs` subroutine, which is responsible for evaluating all the task's input and "post-input" declarations.

```python3
def _eval_task_inputs(...):
    ...
    for decl in decls_to_eval:
        ...
        v = _eval_task_decl(logger, decl, container_env, stdlib, postprocess_decl)
        ...
        container_env = container_env.bind(decl.name, v)
        if isinstance(decl, Tree.Assertion) and not v.value:
            raise Error.RuntimeError(decl.message)
```

By this point the subroutine has already sorted the declarations in order of their dependencies, and the excerpted loop proceeds to evaluate each one and bind the value in the environment. We simply add a check to see if `decl` is actually an instance of our `Assertion` subclass, and if so, raise an error if the value is false.

Now we can add to our `TestAssert` class a new case checking that the assertion causes the task to fail:

```python3
class TestAssert(RunnerTestCase):
    ...
    def test_negative(self):
        self._run(self.task1, {"numerator": 7, "denominator": 0}, expected_exception=WDL.Error.RuntimeError)
```

## Assertions in workflows

Finally we can enable assertions in workflows as well as tasks. Add our `assertion` production to the grammar for workflow bodies (and scatter/if section bodies):

```text
workflow: "workflow" CNAME "{" workflow_element* "}"
?workflow_element: input_decls | any_decl | call | scatter | conditional | workflow_outputs | meta_section | hints_section | assertion

scatter: "scatter" "(" CNAME "in" expr ")" "{" inner_workflow_element* "}"
conditional: "if" "(" expr ")" "{" inner_workflow_element* "}"
?inner_workflow_element: any_decl | call | scatter | conditional | assertion
```

The workflow runner would now treat assertion statements as declarations and schedule their evaluation appropriately. We just need to add a check on the value in the corresponding logic, found in the workflow state machine in [`WDL/runtime/_workflow_state.py`](https://github.com/chanzuckerberg/miniwdl/blob/main/WDL/runtime/_workflow_state.py),

```python3
class StateMachine:
    ...
    def _do_job(self, cfg, stdlib, job):
        ...
        if isinstance(job.node, Tree.Decl):
            ...
            v = _eval_decl(...)
            if isinstance(job.node, Tree.Assertion) and not v.value:
                raise Error.RuntimeError(job.node.message)
            return Env.Bindings(Env.Binding(job.node.name, v))
```

And add tests,

```python3
class TestAssert(RunnerTestCase):
    ...
    wf1 = R"""
    version development
    workflow div {
        input {
            Int numerator
            Int denominator
        }
        assert denominator != 0
        output {
            Int quotient = numerator / denominator
        }
    }
    """

    def test_workflow(self):
        outputs = self._run(self.wf1, {"numerator": 7, "denominator": 2})
        self.assertEqual(outputs["quotient"], 3)
        self._run(self.wf1, {"numerator": 7, "denominator": 0}, expected_exception=WDL.Error.RuntimeError)
```

## Next steps

**Refer to the complete git diff for this example on the [codelab-assert](https://github.com/chanzuckerberg/miniwdl/pull/319/files) branch.**

To further polish our prototype, we might:

* Add a `WDL.runtime.AssertionFailed` exception for increased specificity compared to `RuntimeError`
* Test that the type-checker accepts only boolean expressions
* Test that assertions referring to intermediate values and/or call outputs work as expected
* Allow assertions referring to task and workflow outputs in the respective sections
* Exempt assertions from the `UnusedDeclaration` lint warning, which `miniwdl check` currently reports for them (since nothing references the synthetic declaration name)
* Accept an optional custom error message, e.g. `assert denominator != 0, "division by zero"`, and propagate it through the raised exception
