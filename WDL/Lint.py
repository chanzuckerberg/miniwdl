"""
Linting: annotate WDL AST with hygiene warning
"""
import WDL
from typing import Any, Optional, Set


class Linter(WDL.Walker.Base):
    """
    Linters are Walkers which annotate each tree node with
        ``lint : List[Tuple[SourceNode,str,str]]``
    providing lint warnings with a node (possibly more-specific than the
    node it's attached to), short codename, and message.
    """

    def add(self, obj: WDL.SourceNode, message: str,
            subnode: Optional[WDL.SourceNode] = None):
        if not hasattr(obj, 'lint'):
            obj.lint = []
        obj.lint.append((subnode or obj, self.__class__.__name__, message))


class StringCoercion(Linter):
    # String declaration with non-String rhs expression
    def decl(self, obj: WDL.Decl) -> Any:
        if isinstance(obj.type, WDL.Type.String) \
                and obj.expr is not None \
                and not isinstance(obj.expr.type, WDL.Type.String) \
                and not isinstance(obj.expr.type, WDL.Type.File):
            self.add(obj, "String {} = <{}>".format(
                obj.name, str(obj.expr.type)))
        super().decl(obj)

    # String function operands with non-String expression
    def expr(self, obj: WDL.Expr.Base) -> Any:
        if isinstance(obj, WDL.Expr.Apply):
            if obj.function_name == "_add":
                any_string = False
                all_string = True
                for arg in obj.arguments:
                    if isinstance(arg.type, WDL.Type.String):
                        any_string = True
                    else:
                        all_string = False
                if any_string and not all_string and not isinstance(
                        getattr(obj, 'parent'), WDL.Task):
                    # exception when parent is Task (i.e. we're in the task
                    # command) because the coercion is probably intentional
                    self.add(
                        getattr(
                            obj,
                            'parent'),
                        "string concatenation (+) has non-String argument",
                        obj)
            else:
                F = WDL.Expr._stdlib[obj.function_name]
                if isinstance(F, WDL.StdLib._StaticFunction):
                    for i in range(min(len(F.argument_types),
                                       len(obj.arguments))):
                        if isinstance(
                            F.argument_types[i],
                            WDL.Type.String) and not isinstance(
                            obj.arguments[i].type,
                            WDL.Type.String) and not isinstance(
                            obj.arguments[i].type,
                                WDL.Type.File):
                            self.add(
                                getattr(
                                    obj,
                                    'parent'),
                                "non-String value for String function argument",
                                obj.arguments[i])
        super().expr(obj)


class OptionalCoercion(Linter):
    # Expressions that could blow up at runtime with empty optional values
    def expr(self, obj: WDL.Expr.Base) -> Any:
        if isinstance(obj, WDL.Expr.Apply):
            if obj.function_name == "_add":
                for arg in obj.arguments:
                    if arg.type.optional and not isinstance(
                            getattr(obj, 'parent'), WDL.Task):
                        # exception when parent is Task (i.e. we're in the
                        # task command) because the coercion is probably
                        # intentional, per "Prepending a String to an
                        # Optional Parameter"
                        self.add(getattr(obj, 'parent'),
                                 "optional value passed to +", arg)
            else:
                F = WDL.Expr._stdlib[obj.function_name]
                if isinstance(F, WDL.StdLib._StaticFunction):
                    for i in range(min(len(F.argument_types),
                                       len(obj.arguments))):
                        if obj.arguments[i].type.optional and not F.argument_types[i].optional:
                            self.add(
                                getattr(
                                    obj,
                                    'parent'),
                                "optional value passed for mandatory function argument",
                                obj.arguments[i])
        super().expr(obj)


class IncompleteCall(Linter):
    # Call without all required inputs (allowed for top-level workflow)
    def call(self, obj: WDL.Call) -> Any:
        assert obj.callee is not None
        required_inputs = set(decl.name for decl in obj.callee.required_inputs)
        for name, expr in obj.inputs.items():
            if name in required_inputs:
                required_inputs.remove(name)
        if len(required_inputs) > 0:
            self.add(
                obj,
                "required input(s) {} omitted in call to {}; these become workflow inputs and prevent composition".format(
                    ", ".join(required_inputs),
                    obj.callee.name))
        super().call(obj)


class CallImportNameCollision(Linter):
    # A call name collides with the namespace of an imported document; allowed
    # but potentially confusing.
    def call(self, obj: WDL.Call) -> Any:
        doc = obj
        while not isinstance(doc, WDL.Document):
            doc = getattr(doc, 'parent')
        for uri, namespace, subdoc in doc.imports:
            if namespace == obj.name:
                self.add(
                    obj,
                    "call name {} collides with imported document namespace".format(
                        obj.name))
        super().call(obj)


class UnusedImport(Linter):
    # Nothing used from an imported document
    def document(self, obj: WDL.Document) -> Any:
        for uri, namespace, subdoc in obj.imports:
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
