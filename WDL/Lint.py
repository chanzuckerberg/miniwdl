"""
Linting: annotate WDL AST with hygiene warning
"""
from typing import Any, Optional
import WDL


class Linter(WDL.Walker.Base):
    """
    Linters are Walkers which annotate each Tree node with
        ``lint : List[Tuple[SourceNode,str,str]]``
    providing lint warnings with a node (possibly more-specific than the
    node it's attached to), short codename, and message.
    """

    def add(self, obj: WDL.SourceNode, message: str, subnode: Optional[WDL.SourceNode] = None):
        """
        Used by subclasses to attach lint to a node.

        Note, lint attaches to Tree nodes (Decl, Task, Workflow, Scatter,
        Conditional, Doucemnt). Warnings about individual expressinos should
        attach to their parent Tree node.
        """
        assert not isinstance(obj, WDL.Expr.Base)
        if not hasattr(obj, "lint"):
            obj.lint = []
        obj.lint.append((subnode or obj, self.__class__.__name__, message))


_all_linters = []


def a_linter(cls):
    """
    Decorator for subclasses of ``Linter`` to register them for use
    """
    _all_linters.append(cls)


def lint(doc):
    """
    Apply all linters to the document
    """

    # Add additional markups to the AST for use by the linters
    WDL.Walker.SetParents()(doc)
    WDL.Walker.MarkCalled()(doc)
    WDL.Walker.SetReferrers()(doc)

    for linter in _all_linters:
        linter()(doc)

    return doc


class _Collector(WDL.Walker.Base):
    def __init__(self):
        super().__init__()
        self.lint = []

    def _collect(self, obj):
        if hasattr(obj, "lint"):
            self.lint.extend(getattr(obj, "lint"))

    def document(self, obj) -> Any:
        self._collect(obj)
        super().document(obj)

    def workflow(self, obj) -> Any:
        self._collect(obj)
        super().workflow(obj)

    def call(self, obj) -> Any:
        self._collect(obj)
        super().call(obj)

    def scatter(self, obj) -> Any:
        self._collect(obj)
        super().scatter(obj)

    def conditional(self, obj) -> Any:
        self._collect(obj)
        super().conditional(obj)

    def decl(self, obj) -> Any:
        self._collect(obj)
        super().decl(obj)

    def task(self, obj) -> Any:
        self._collect(obj)
        super().task(obj)


def collect(doc):
    """
    Recursively traverse the already-linted document and collect a flat list of
    (tree node, linter_class, message)
    """
    collector = _Collector()
    collector(doc)
    return collector.lint


@a_linter
class StringCoercion(Linter):
    # String declaration with non-String rhs expression
    def decl(self, obj: WDL.Decl) -> Any:
        if isinstance(obj.type, WDL.Type.String) and obj.expr:
            if not isinstance(obj.expr.type, (WDL.Type.String, WDL.Type.File)):
                self.add(obj, "String {} = <{}>".format(obj.name, str(obj.expr.type)))
        super().decl(obj)

    def expr(self, obj: WDL.Expr.Base) -> Any:
        pt = getattr(obj, "parent")
        if isinstance(obj, WDL.Expr.Apply):
            # String function operands with non-String expression
            if obj.function_name == "_add":
                any_string = False
                all_string = True
                for arg in obj.arguments:
                    if isinstance(arg.type, WDL.Type.String):
                        any_string = True
                    elif not isinstance(arg.type, WDL.Type.File):
                        all_string = False
                if any_string and not all_string and not isinstance(pt, WDL.Task):
                    # exception when parent is Task (i.e. we're in the task
                    # command) because the coercion is probably intentional
                    self.add(pt, "string concatenation (+) has non-String argument", obj)
            else:
                F = WDL.Expr._stdlib[obj.function_name]
                if isinstance(F, WDL.StdLib._StaticFunction):
                    for i in range(min(len(F.argument_types), len(obj.arguments))):
                        F_i = F.argument_types[i]
                        arg_i = obj.arguments[i]
                        if isinstance(F_i, WDL.Type.String) and not isinstance(
                            arg_i.type, (WDL.Type.String, WDL.Type.File)
                        ):
                            msg = "{} coerced to String argument".format(arg_i.type)
                            self.add(pt, msg, arg_i)
        elif isinstance(obj, WDL.Expr.Array):
            # Array literal with mixed item types, one of which is String,
            # causing coercion of the others
            any_string = False
            all_string = True
            item_types = []
            for elt in obj.items:
                if isinstance(elt.type, WDL.Type.String):
                    any_string = True
                elif not isinstance(elt.type, WDL.Type.File):
                    all_string = False
                item_types.append(str(elt.type))
            if any_string and not all_string:
                msg = "literal {} has item types: [{}]".format(str(obj.type), ", ".join(item_types))
                self.add(pt, msg, obj)
        super().expr(obj)

    def call(self, obj: WDL.Tree.Call) -> Any:
        for name, inp_expr in obj.inputs.items():
            decl: Optional[WDL.Tree.Decl] = None
            if isinstance(obj.callee, WDL.Tree.Task):
                for d in obj.callee.inputs + obj.callee.postinputs:
                    if d.name == name:
                        decl = d
            else:
                assert isinstance(obj.callee, WDL.Tree.Workflow)
                for ele in obj.callee.elements:
                    if isinstance(ele, WDL.Tree.Decl) and ele.name == name:
                        decl = ele
            assert decl
            # note: in a workflow call, we want to flag File=>String coercions,
            # which are OK within tasks
            if isinstance(decl.type, WDL.Type.String) and not isinstance(
                inp_expr.type, WDL.Type.String
            ):
                msg = "{} input coerced to String {}".format(str(inp_expr.type), str(decl.name))
                self.add(obj, msg, inp_expr)


def _array_levels(ty: WDL.Type.Base, l=0):
    if isinstance(ty, WDL.Type.Array):
        return _array_levels(ty.item_type, l + 1)
    return l


def _is_array_coercion(value_type: WDL.Type.Base, expr_type: WDL.Type.Base):
    return isinstance(value_type, WDL.Type.Array) and _array_levels(value_type) > _array_levels(
        expr_type
    )


@a_linter
class ArrayCoercion(Linter):
    def decl(self, obj: WDL.Decl) -> Any:
        if obj.expr and _is_array_coercion(obj.type, obj.expr.type):
            msg = "{ty} coerced to Array[{ty}]".format(ty=str(obj.expr.type))
            self.add(obj, msg)
        super().decl(obj)

    def expr(self, obj: WDL.Expr.Base) -> Any:
        pt = getattr(obj, "parent")
        if isinstance(obj, WDL.Expr.Apply):
            F = WDL.Expr._stdlib[obj.function_name]
            if isinstance(F, WDL.StdLib._StaticFunction):
                for i in range(min(len(F.argument_types), len(obj.arguments))):
                    F_i = F.argument_types[i]
                    arg_i = obj.arguments[i]
                    if _is_array_coercion(F_i, arg_i.type):
                        msg = "{ty} coerced to Array[{ty}]".format(ty=str(arg_i.type))
                        self.add(pt, msg, arg_i)
        super().expr(obj)

    def call(self, obj: WDL.Tree.Call) -> Any:
        for name, inp_expr in obj.inputs.items():
            decl: Optional[WDL.Tree.Decl] = None
            if isinstance(obj.callee, WDL.Tree.Task):
                for d in obj.callee.inputs + obj.callee.postinputs:
                    if d.name == name:
                        decl = d
            else:
                assert isinstance(obj.callee, WDL.Tree.Workflow)
                for ele in obj.callee.elements:
                    if isinstance(ele, WDL.Tree.Decl) and ele.name == name:
                        decl = ele
            assert decl
            if _is_array_coercion(decl.type, inp_expr.type):
                msg = "{ty} coerced to Array[{ty}]".format(ty=str(inp_expr.type))
                self.add(obj, msg, inp_expr)


@a_linter
class OptionalCoercion(Linter):
    # Expressions that could blow up at runtime with empty optional values
    def expr(self, obj: WDL.Expr.Base) -> Any:
        if isinstance(obj, WDL.Expr.Apply):
            if obj.function_name == "_add":
                for arg in obj.arguments:
                    if arg.type.optional and not isinstance(getattr(obj, "parent"), WDL.Task):
                        # exception when parent is Task (i.e. we're in the
                        # task command) because the coercion is probably
                        # intentional, per "Prepending a String to an
                        # Optional Parameter"
                        self.add(getattr(obj, "parent"), "optional value passed to +", arg)
            else:
                F = WDL.Expr._stdlib[obj.function_name]
                if isinstance(F, WDL.StdLib._StaticFunction):
                    for i in range(min(len(F.argument_types), len(obj.arguments))):
                        if obj.arguments[i].type.optional and not F.argument_types[i].optional:
                            self.add(
                                getattr(obj, "parent"),
                                "optional value passed for mandatory function argument",
                                obj.arguments[i],
                            )
        super().expr(obj)


@a_linter
class IncompleteCall(Linter):
    # Call without all required inputs (allowed for top-level workflow)
    def call(self, obj: WDL.Call) -> Any:
        assert obj.callee is not None
        required_inputs = set(decl.name for decl in obj.callee.required_inputs)
        for name, _ in obj.inputs.items():
            if name in required_inputs:
                required_inputs.remove(name)
        if required_inputs:
            msg = "required input(s) {} omitted in call to {}; these become workflow inputs and prevent composition".format(
                ", ".join(required_inputs), obj.callee.name
            )
            self.add(obj, msg)
        super().call(obj)


@a_linter
class CallImportNameCollision(Linter):
    # A call name collides with the namespace of an imported document; allowed
    # but potentially confusing.
    def call(self, obj: WDL.Call) -> Any:
        doc = obj
        while not isinstance(doc, WDL.Document):
            doc = getattr(doc, "parent")
        for _, namespace, _ in doc.imports:
            if namespace == obj.name:
                msg = "call name {} collides with imported document namespace".format(obj.name)
                self.add(obj, msg)
        super().call(obj)


@a_linter
class UnusedImport(Linter):
    # Nothing used from an imported document
    def document(self, obj: WDL.Document) -> Any:
        for _, namespace, subdoc in obj.imports:
            assert subdoc is not None
            any_called = False
            for task in subdoc.tasks:
                if task.called:
                    any_called = True
            if subdoc.workflow and subdoc.workflow.called:
                any_called = True
            if not any_called:
                self.add(obj, "nothing used from the import " + namespace)
        super().document(obj)


@a_linter
class ForwardReference(Linter):
    def expr(self, obj: WDL.Expr.Base) -> Any:
        if (
            isinstance(obj, WDL.Expr.Ident)
            and isinstance(obj.ctx, (WDL.Decl, WDL.Call))
            and (
                obj.ctx.pos.line > obj.pos.line
                or (obj.ctx.pos.line == obj.pos.line and obj.ctx.pos.column > obj.pos.column)
            )
        ):
            if isinstance(obj.ctx, WDL.Decl):
                msg = "reference to {} precedes its declaration".format(obj.name)
            elif isinstance(obj.ctx, WDL.Call):
                msg = "reference to output of {} precedes the call".format(
                    ".".join(obj.namespace)
                )
            else:
                assert False
            self.add(getattr(obj, "parent"), msg, obj)
        super().expr(obj)


@a_linter
class UnusedDeclaration(Linter):
    def decl(self, obj: WDL.Tree.Decl) -> Any:
        pt = getattr(obj, "parent")
        is_output = (
            isinstance(pt, (WDL.Tree.Workflow, WDL.Tree.Task)) and pt.outputs and obj in pt.outputs
        )
        if not is_output and not getattr(obj, "referrers", []):
            # heuristic exception: File whose name suggests it's an hts index
            # file; as these commonly need to be localized, but not explicitly
            # used in task command
            index_suffixes = [
                "index",
                "indexes",
                "indices",
                "idx",
                "tbi",
                "bai",
                "crai",
                "csi",
                "fai",
                "dict",
            ]
            if not (
                (
                    isinstance(obj.type, WDL.Type.File)
                    and sum(1 for sfx in index_suffixes if obj.name.endswith(sfx))
                )
                or (
                    isinstance(obj.type, WDL.Type.Array)
                    and isinstance(obj.type.item_type, WDL.Type.File)
                    and sum(1 for sfx in index_suffixes if obj.name.endswith(sfx))
                )
            ):
                self.add(obj, "nothing refers to " + obj.name)


@a_linter
class UnusedCall(Linter):
    _workflow_with_outputs: bool = False

    def workflow(self, obj: WDL.Tree.Workflow) -> Any:
        self._workflow_with_outputs = getattr(obj, "called", False) and obj.outputs is not None
        super().workflow(obj)
        self._workflow_with_outputs = False

    def call(self, obj: WDL.Tree.Call) -> Any:
        if self._workflow_with_outputs and not getattr(obj, "referrers", []):
            self.add(
                obj,
                "the outputs of call "
                + obj.name
                + " aren't output from the workflow nor otherwise used",
            )
