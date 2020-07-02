# Adding WDL functions

This lab will show how to add new functions to the WDL standard library, thus introducing development on miniwdl itself, emphasizing its expression type-checking and evaluation. To begin, check out the source repository and install Python packages needed for the development environment.

```bash
git clone --recursive https://github.com/chanzuckerberg/miniwdl.git  # or your own fork
cd miniwdl
pip3 install -r requirements.txt
pip3 install -r requirements.dev.txt
```

The standard library is implemented in `WDL/StdLib.py`, except for some details specific to the task runtime environment.

## factorial

We'll warm up by adding a `factorial(n)` function to WDL. This is an example of a "static" function, with fixed argument and return types (integer to integer, in this case), thus straightforward to define. In [`WDL.StdLib.Base.__init__`](https://github.com/chanzuckerberg/miniwdl/blob/main/WDL/StdLib.py), you'll find the definition of a `static()` helper followed by numerous uses to set up standard functions. We can define the new factorial function by adding to this section,

```python3
class Base:
    def __init__(self):
        ...
        @static([Type.Int()], Type.Int())
        def factorial(v: Value.Int) -> Value.Int:
            def f(n: int) -> int:
                return 1 if n <= 1 else n * f(n - 1)

            return Value.Int(f(v.value))
        ...
```

Given a list of the arguments' WDL data types (`WDL.Type.Base`) and the return type, the `static()` helper decorates a function which takes the argument values (`WDL.Value.Base`) and returns the result value. Miniwdl uses the declared data types to handle type-checking wherever the function is used. The function itself receives the integer argument in a [`WDL.Value`](https://miniwdl.readthedocs.io/en/latest/WDL.html#module-WDL.Value) "box," and similarly boxes its return value.

Notice that we include not only the WDL type signature, but also [PEP 484 type hints](https://www.python.org/dev/peps/pep-0484/) for the Python functions. The miniwdl codebase uses such type hints throughout, checked using [pyre](https://pyre-check.org/), which you can try with `make check` in the repository directory. When you send a pull request upstream, miniwdl's continuous integration will require pyre's approval.

**You can find the complete git diff for this and the following examples on the [codelab-stdlib-functions](https://github.com/chanzuckerberg/miniwdl/pull/318/files) branch.**

We can try an *ad hoc* test of our new factorial function,

```bash
$ python3 -m WDL run --dir /tmp <(echo "
workflow factorials {
    scatter (n in range(11)) {
        Pair[Int,Int] p = (n, factorial(n))
    }
    output {
        Array[Pair[Int,Int]] results = p
    }
}
")
```

An alternative to `python3 -m WDL run ...` is to `pip3 install .` the current code, making the familiar `miniwdl run ...` entry point use it (but also leaving it there!).

## word_count

Next we'll add `word_count(text_file)`, illustrating how to interact with the file system. 

```python3
        ...
        @static([Type.File()], Type.Int())
        def word_count(v: Value.File) -> Value.Int:
            with open(self._devirtualize_filename(v.value), "r") as infile:
                return Value.Int(len(infile.read().split(" ")))
        ...
```

Trying it out,

```bash
$ python3 -m WDL run --dir /tmp <(echo '
task wc {
    command {
        echo The quick brown fox jumps over the lazy dog > message.txt
    }
    output {
        Int n = word_count("message.txt")
    }
}
')
```

The interesting part is the call to `self._devirtualize_filename()`, an abstract method of the `WDL.StdLib.Base` object responsible for converting the WDL `File` value into a local filename for our Python function to open and process. It might, for example, translate a path generated inside a task's Docker container to an equivalent path on the host's file system. Or it might, for security reasons, raise an error given a filename that's unrelated to the workflow in progress.

## choose_random

Lastly we'll introduce a polymorphic function, which takes a variable number of arguments and whose return type depends on the argument types. The `static()` helper we've used so far isn't flexible enough to capture this, so we'll need to supply custom type-checking logic as well as the implementation.

Given `Array[T] x`, `choose_random(x)` returns a random element of `x`, with type `T`. Given `Array[T] x` and `Array[U] y`, `choose_random(x,y)` returns a `Pair[T,U]` with one random element from each `x` and `y`; equivalent to `choose_random(cross(x,y))`, but more efficient as it doesn't compute the full cross-product array.

We can implement this polymorphic function by writing a class derived from `WDL.StdLib.EagerFunction`, and in `WDL.StdLib.Base.__init__`, setting `self.choose_random` to an instance of this class.

```python3
        ...
        self.choose_random = _ChooseRandom()
        ...
...
class _ChooseRandom(EagerFunction):
    def infer_type(self, expr: "Expr.Apply") -> Type.Base:
        if len(expr.arguments) not in [1, 2]:
            raise Error.WrongArity(expr, 1)
        arg0ty = expr.arguments[0].type
        if not isinstance(arg0ty, Type.Array):
            raise Error.StaticTypeMismatch(expr.arguments[0], Type.Array(Type.Any()), arg0ty)
        if len(expr.arguments) == 1:
            return arg0ty.item_type
        arg1ty = expr.arguments[1].type
        if not isinstance(arg1ty, Type.Array):
            raise Error.StaticTypeMismatch(expr.arguments[1], Type.Array(Type.Any()), arg1ty)
        return Type.Pair(arg0ty.item_type, arg1ty.item_type)
```

`infer_type()` is invoked during static WDL validation and given the already-typechecked argument expressions. It either provides the function's return type, or raises a type mismatch error. Here it checks that each argument is an array, and determines the return type in terms of their array item type(s).

Continuing,

```python3
    def _call_eager(self, expr: "Expr.Apply", arguments: List[Value.Base]) -> Value.Base:
        import random

        if not arguments[0].value or (len(arguments) > 1 and not arguments[1].value):
            raise Error.RuntimeError("empty array passed to choose_random()")
        item0 = random.choice(arguments[0].value)
        if len(arguments) == 1:
            return item0
        item1 = random.choice(arguments[1].value)
        return Value.Pair(item0.type, item1.type, (item0, item1))
```

`_call_eager()` is invoked once the arguments have been evaluated to WDL values at runtime. It proceeds to construct the return value based on the one or two array arguments. (It's also possible to implement non-eager functions, where the implementation is given the environment and argument expressions, and decides when/whether to evaluate them.)

```bash
python3 -m WDL run --dir /tmp <(echo '
workflow test_choose_random {
    output {
        Int one = choose_random([16, 32, 64, 128])
        Pair[Int,String] two = choose_random([16, 32, 64, 128], ["dogs", "cats", "bears"])
    }
}
')
```

Try modifying the declaration types to confirm that `choose_random()`'s return type is properly inferred and constrained in each case. To propose a new function in the WDL specification, of course we should have more-thorough tests, e.g. to exercise the error code paths in both type-checking and evaluation.

**The [codelab-stdlib-functions](https://github.com/chanzuckerberg/miniwdl/pull/318/files) branch has the complete git diff for these examples.**
