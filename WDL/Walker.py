# pylint: disable=assignment-from-no-return
from typing import Any, List, Optional
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

    If initialized with ``auto_descend=True``, then super invocations do
    do nothing (they can be omitted) and child nodes are recursed just after
     each method invocation (preorder traversal).
    """

    auto_descend: bool

    def __init__(self, auto_descend: bool = False) -> None:
        self.auto_descend = auto_descend

    def __call__(self, obj: WDL.Error.SourceNode, descend: Optional[bool] = None) -> Any:
        ans = None
        if isinstance(obj, WDL.Tree.Document):
            ans = self.document(obj)
        elif isinstance(obj, WDL.Tree.Workflow):
            ans = self.workflow(obj)
        elif isinstance(obj, WDL.Tree.Call):
            ans = self.call(obj)
        elif isinstance(obj, WDL.Tree.Scatter):
            ans = self.scatter(obj)
        elif isinstance(obj, WDL.Tree.Conditional):
            ans = self.conditional(obj)
        elif isinstance(obj, WDL.Tree.Decl):
            ans = self.decl(obj)
        elif isinstance(obj, WDL.Tree.Task):
            ans = self.task(obj)
        elif isinstance(obj, WDL.Tree.StructTypeDef):
            ans = self.struct_typedef(obj)
        elif isinstance(obj, WDL.Expr.Base):
            ans = self.expr(obj)
        else:
            assert False
        if descend is None:
            descend = self.auto_descend
        if descend:
            for ch in obj.children:
                self(ch)
        return ans

    def _descend(self, obj: WDL.Error.SourceNode) -> Any:
        if not self.auto_descend:
            for ch in obj.children:
                self(ch)

    def document(self, obj: WDL.Tree.Document) -> Any:
        self._descend(obj)

    def workflow(self, obj: WDL.Tree.Workflow) -> Any:
        self._descend(obj)

    def call(self, obj: WDL.Tree.Call) -> Any:
        self._descend(obj)

    def scatter(self, obj: WDL.Tree.Scatter) -> Any:
        self._descend(obj)

    def conditional(self, obj: WDL.Tree.Conditional) -> Any:
        self._descend(obj)

    def decl(self, obj: WDL.Tree.Decl) -> Any:
        self._descend(obj)

    def task(self, obj: WDL.Tree.Task) -> Any:
        self._descend(obj)

    def struct_typedef(self, obj: WDL.Tree.StructTypeDef) -> Any:
        self._descend(obj)

    def expr(self, obj: WDL.Expr.Base) -> Any:
        self._descend(obj)


class Multi(Base):
    """
    Multiplexes several walkers to run "concurrently" in one traversal of the
    AST, which will be more efficient than running them separately. This only
    works with ``auto_descend=True`` walkers.
    """

    _walkers: List[Base]

    def __init__(self, walkers: List[Base]) -> None:
        for w in walkers:
            assert w.auto_descend
        self._walkers = walkers
        super().__init__(auto_descend=True)

    def document(self, obj: WDL.Tree.Document) -> Any:
        for w in self._walkers:
            w.document(obj)

    def workflow(self, obj: WDL.Tree.Workflow) -> Any:
        for w in self._walkers:
            w.workflow(obj)

    def call(self, obj: WDL.Tree.Call) -> Any:
        for w in self._walkers:
            w.call(obj)

    def scatter(self, obj: WDL.Tree.Scatter) -> Any:
        for w in self._walkers:
            w.scatter(obj)

    def conditional(self, obj: WDL.Tree.Conditional) -> Any:
        for w in self._walkers:
            w.conditional(obj)

    def decl(self, obj: WDL.Tree.Decl) -> Any:
        for w in self._walkers:
            w.decl(obj)

    def task(self, obj: WDL.Tree.Task) -> Any:
        for w in self._walkers:
            w.task(obj)

    def struct_typedef(self, obj: WDL.Tree.StructTypeDef) -> Any:
        for w in self._walkers:
            w.struct_typedef(obj)

    def expr(self, obj: WDL.Expr.Base) -> Any:
        for w in self._walkers:
            w.expr(obj)


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
        for imp in obj.imports:
            imp.doc.parent = obj
        for stb in obj.struct_typedefs:
            stb.rhs.parent = obj
        for task in obj.tasks:
            task.parent = obj
        if obj.workflow:
            obj.workflow.parent = obj

    def workflow(self, obj: WDL.Tree.Workflow) -> None:
        super().workflow(obj)
        obj.parent = None
        for elt in (obj.inputs or []) + obj.elements + (obj.outputs or []):
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
        for elt in (obj.inputs or []) + obj.postinputs + obj.outputs:
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

    The top-level workflow is considered called.
    """

    marking: bool = False  # True while recursing from the top-level workflow

    def workflow(self, obj: WDL.Tree.Workflow) -> None:
        obj.called = False
        if obj.parent.parent is None:  # pyre-ignore
            assert not self.marking
            obj.called = True
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

    def __init__(self):
        super().__init__(auto_descend=True)

    def expr(self, obj: WDL.Expr.Base) -> None:
        if isinstance(obj, WDL.Expr.Ident) and isinstance(obj.ctx, (WDL.Tree.Decl, WDL.Tree.Call)):
            setattr(obj.ctx, "referrers", getattr(obj.ctx, "referrers", []) + [obj])
