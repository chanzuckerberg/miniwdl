from typing import Any
import WDL

class Base():
    """
    Helper base class for traversing the WDL abstract syntax tree. When called
    on a node, invokes the appropriate method (document, workflow, call,
    scatter, conditional, decl, task). The base implementations of these
    methods recurse into the node's "children." Overriding subclasses can thus
    invoke their super at the appropriate point for preorder or postorder
    traversal.

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

    def __call__(self, obj : WDL.Error.SourceNode) -> Any:
        if isinstance(obj, WDL.Document.Document):
            return self.document(obj)
        elif isinstance(obj, WDL.Document.Workflow):
            return self.workflow(obj)
        elif isinstance(obj, WDL.Document.Call):
            return self.call(obj)
        elif isinstance(obj, WDL.Document.Scatter):
            return self.scatter(obj)
        elif isinstance(obj, WDL.Document.Conditional):
            return self.conditional(obj)
        elif isinstance(obj, WDL.Document.Decl):
            return self.decl(obj)
        elif isinstance(obj, WDL.Document.Task):
            return self.task(obj)
        else:
            assert False

    def document(self, obj : WDL.Document.Document) -> Any:
        for namespace, uri, subdoc in obj.imports:
            assert isinstance(subdoc, WDL.Document.Document)
            self(subdoc)
        for task in obj.tasks:
            self(task)
        if obj.workflow:
            self(obj.workflow)

    def workflow(self, obj : WDL.Document.Workflow) -> Any:
        for elt in obj.elements:
            self(elt)

    def call(self, obj : WDL.Document.Call) -> Any:
        pass

    def scatter(self, obj : WDL.Document.Scatter) -> Any:
        for elt in obj.elements:
            self(elt)

    def conditional(self, obj : WDL.Document.Conditional) -> Any:
        for elt in obj.elements:
            self(elt)

    def decl(self, obj : WDL.Document.Decl) -> Any:
        pass

    def task(self, obj : WDL.Document.Task) -> Any:
        for elt in obj.inputs + obj.postinputs + obj.outputs:
            self(elt)

class SetParents(Base):
    """
    Add ``parent`` to each node.

    On Document, the document which imports this document (None at top level)

    On Workflow and Task, the containing document.

    On Call, Scatter, and Conditional, the containing Workflow, Scatter, or
    Conditional.

    On Decl, the contaning Task, Workflow, Scatter, or Conditional.
    """
    def document(self, obj : WDL.Document.Document) -> None:
        super().document(obj)
        obj.parent = None
        for namespace,uri,subdoc in obj.imports:
            subdoc.parent = obj
        for task in obj.tasks:
            task.parent = obj
        if obj.workflow:
            obj.workflow.parent = obj
    def workflow(self, obj : WDL.Document.Workflow) -> None:
        super().workflow(obj)
        obj.parent = None
        for elt in obj.elements:
            elt.parent = obj
    def scatter(self, obj : WDL.Document.Scatter) -> None:
        super().scatter(obj)
        obj.parent = None
        for elt in obj.elements:
            elt.parent = obj
    def conditional(self, obj : WDL.Document.Conditional) -> None:
        super().conditional(obj)
        obj.parent = None
        for elt in obj.elements:
            elt.parent = obj
    def task(self, obj : WDL.Document.Task) -> None:
        super().task(obj)
        obj.parent = None
        for elt in obj.inputs + obj.postinputs + obj.outputs:
            elt.parent = obj

class MarkCalled(Base):
    """
    Mark each Task and Workflow with ``called : bool`` according to whether
    there exists a Call to it in the top-level workflow (or a subworkflow it
    calls). Requires SetParents to have been applied previously.
    """
    marking : bool = False # True while recursing from the top-level workflow
    def workflow(self, obj : WDL.Document.Workflow) -> None:
        obj.called = False
        if obj.parent.parent is None: # pyre-ignore
            assert not self.marking
            self.marking = True
            super().workflow(obj)
            self.marking = False
        elif self.marking:
            super().workflow(obj)
    def call(self, obj : WDL.Document.Call) -> None:
        assert self.marking
        obj.callee.called = True
        if isinstance(obj.callee, WDL.Document.Workflow):
            self(obj.callee)
    def task(self, obj : WDL.Document.Task) -> None:
        obj.called = False
