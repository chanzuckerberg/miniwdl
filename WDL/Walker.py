from typing import Any, List
import WDL


class Base:
    """
    Helper base class for traversing the WDL abstract syntax tree. When called
    on a node, invokes the appropriate method (document, workflow, call,
    scatter, conditional, decl, task). The base implementations of these
    methods recurse into the node's "children." Overriding subclasses can thus
    invoke their super at the appropriate point for preorder or postorder
    traversal (or omit super to prevent further descent).

    ``
    class PrintUnconditionalCallNames(Walker.Base):
        def conditional(self, obj):
            # skip everything inside conditionals by NOT calling
            #   super().conditional(obj)
            pass
        def call(self, obj):
            print(obj.name)
    walker = PrintUnconditionalCallNames()
    walker(wdl_document)
    ``
    """

    def __init__(self) -> None:
        pass

    def __call__(self, obj: WDL.Error.SourceNode) -> Any:
        if isinstance(obj, WDL.Tree.Document):
            return self.document(obj)
        if isinstance(obj, WDL.Tree.Workflow):
            return self.workflow(obj)
        if isinstance(obj, WDL.Tree.Call):
            return self.call(obj)
        if isinstance(obj, WDL.Tree.Scatter):
            return self.scatter(obj)
        if isinstance(obj, WDL.Tree.Conditional):
            return self.conditional(obj)
        if isinstance(obj, WDL.Tree.Decl):
            return self.decl(obj)
        if isinstance(obj, WDL.Tree.Task):
            return self.task(obj)
        if isinstance(obj, WDL.Expr.Base):
            return self.expr(obj)
        assert False

    def document(self, obj: WDL.Tree.Document) -> Any:
        for _, _, subdoc in obj.imports:
            assert isinstance(subdoc, WDL.Tree.Document)
            self(subdoc)
        for task in obj.tasks:
            self(task)
        if obj.workflow:
            self(obj.workflow)

    def workflow(self, obj: WDL.Tree.Workflow) -> Any:
        for elt in obj.elements:
            self(elt)

    def call(self, obj: WDL.Tree.Call) -> Any:
        for _, expr in obj.inputs.items():
            self(expr)

    def scatter(self, obj: WDL.Tree.Scatter) -> Any:
        self(obj.expr)
        for elt in obj.elements:
            self(elt)

    def conditional(self, obj: WDL.Tree.Conditional) -> Any:
        self(obj.expr)
        for elt in obj.elements:
            self(elt)

    def decl(self, obj: WDL.Tree.Decl) -> Any:
        if obj.expr:
            self(obj.expr)

    def task(self, obj: WDL.Tree.Task) -> Any:
        for elt in obj.inputs + obj.postinputs:
            self(elt)
        self(obj.command)
        for elt in obj.outputs:
            self(elt)
        # TODO: traverse runtime section

    def expr(self, obj: WDL.Expr.Base) -> Any:
        if isinstance(obj, WDL.Expr.Placeholder):
            self(obj.expr)
        elif isinstance(obj, WDL.Expr.String):
            for p in obj.parts:
                if isinstance(p, WDL.Expr.Base):
                    self(p)
        elif isinstance(obj, WDL.Expr.Array):
            for elt in obj.items:
                self(elt)
        elif isinstance(obj, WDL.Expr.IfThenElse):
            self(obj.condition)
            self(obj.consequent)
            self(obj.alternative)
        elif isinstance(obj, WDL.Expr.Apply):
            for elt in obj.arguments:
                self(elt)
        elif isinstance(obj, WDL.Expr.Pair):
            self(obj.left)
            self(obj.right)
        elif isinstance(obj, WDL.Expr.Map):
            for k, v in obj.items.items():
                self(k)
                self(v)
        else:
            pass


class SetParents(Base):
    """
    Add ``parent`` to each node.

    On Document, the document which imports this document (None at top level)

    On Workflow and Task, the containing document.

    On Call, Scatter, and Conditional, the containing Workflow, Scatter, or
    Conditional.

    On Decl, the contaning Task, Workflow, Scatter, or Conditional.

    On each Expr, the containing Decl, Call, Scatter, Conditional, or Task.
    """

    _parent_stack: List[WDL.Error.SourceNode] = []

    def document(self, obj: WDL.Tree.Document) -> None:
        super().document(obj)
        obj.parent = None
        for _, _, subdoc in obj.imports:
            subdoc.parent = obj
        for task in obj.tasks:
            task.parent = obj
        if obj.workflow:
            obj.workflow.parent = obj

    def workflow(self, obj: WDL.Tree.Workflow) -> None:
        super().workflow(obj)
        obj.parent = None
        for elt in obj.elements:
            elt.parent = obj

    def call(self, obj: WDL.Tree.Call) -> None:
        self._parent_stack.append(obj)
        super().call(obj)
        self._parent_stack.pop()

    def scatter(self, obj: WDL.Tree.Scatter) -> None:
        self._parent_stack.append(obj)
        super().scatter(obj)
        self._parent_stack.pop()
        obj.parent = None
        for elt in obj.elements:
            elt.parent = obj

    def conditional(self, obj: WDL.Tree.Conditional) -> None:
        self._parent_stack.append(obj)
        super().conditional(obj)
        self._parent_stack.pop()
        obj.parent = None
        for elt in obj.elements:
            elt.parent = obj

    def task(self, obj: WDL.Tree.Task) -> None:
        self._parent_stack.append(obj)
        super().task(obj)
        self._parent_stack.pop()
        obj.parent = None
        for elt in obj.inputs + obj.postinputs + obj.outputs:
            elt.parent = obj

    def decl(self, obj: WDL.Tree.Decl) -> None:
        self._parent_stack.append(obj)
        super().decl(obj)
        self._parent_stack.pop()

    def expr(self, obj: WDL.Expr.Base) -> None:
        super().expr(obj)
        obj.parent = self._parent_stack[-1]


class MarkCalled(Base):
    """
    Mark each Task and Workflow with ``called : bool`` according to whether
    there exists a Call to it in the top-level workflow (or a subworkflow it
    calls). Requires SetParents to have been applied previously.
    """

    marking: bool = False  # True while recursing from the top-level workflow

    def workflow(self, obj: WDL.Tree.Workflow) -> None:
        obj.called = False
        if obj.parent.parent is None:  # pyre-ignore
            assert not self.marking
            self.marking = True
            super().workflow(obj)
            self.marking = False
        elif self.marking:
            super().workflow(obj)

    def call(self, obj: WDL.Tree.Call) -> None:
        assert self.marking
        if isinstance(obj.callee, WDL.Tree.Workflow):
            self(obj.callee)
        obj.callee.called = True

    def task(self, obj: WDL.Tree.Task) -> None:
        obj.called = False


class SetReferrers(Base):
    """
    Add ``referrers`` to each Decl and Call in all tasks and workflows.

    It lists each Expr.Ident which uses the value (of a Decl) or any output of
    the Call. The Expr.Ident instances may be in declarations, call inputs,
    task commands, outputs, scatter arrays, if conditions.
    """

    def expr(self, obj: WDL.Expr.Base) -> None:
        if isinstance(obj, WDL.Expr.Ident) and isinstance(obj.ctx, (WDL.Tree.Decl, WDL.Tree.Call)):
            setattr(obj.ctx, "referrers", getattr(obj.ctx, "referrers", []) + [obj])
        return super().expr(obj)
