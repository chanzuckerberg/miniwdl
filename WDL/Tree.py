# pyre-strict
"""
Abstract syntax tree (AST) for WDL documents, encompassing declarations, tasks, calls, and workflows. The AST is typically constructed and returned by :func:`~WDL.load` or :func:`~WDL.parse_document`.
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Callable, TypeVar, Tuple, Union
import WDL.Type as T
import WDL.Value as V
import WDL.Expr as E
import WDL.Env as Env
import WDL.Error as Err
from WDL.Error import SourcePosition, SourceNode
import copy, os, errno
import WDL._parser

class Decl(SourceNode):
    """A declaration such as an input/output variable"""
    type : T.Base
    """WDL type"""
    name : str
    """Identifier declared"""
    expr : Optional[E.Base]
    """Expression bound"""

    def __init__(self, pos : SourcePosition, type : T.Base, name: str, expr : Optional[E.Base] = None) -> None:
        super().__init__(pos)
        self.type = type
        self.name = name
        self.expr = expr

    def __str__(self) -> str:
        if self.expr is None:
            return "{} {}".format(str(self.type), self.name)
        return "{} {} = {}".format(str(self.type), self.name, str(self.expr))

    # TODO: when the declaration is evaluated,
    #  - the optional/nonempty type quantifiers should be checked
    #  - String to File coercion

class Task(SourceNode):
    """WDL Task"""
    name : str
    """Task name"""
    inputs : List[Decl]
    """Inputs declared within the ``input{}`` task section"""
    postinputs: List[Decl]
    """Declarations outside of the ``input{}`` task section"""
    command: E.String
    """Task command"""
    outputs: List[Decl]
    """Output declarations"""
    parameter_meta : Dict[str,Any]
    """``parameter_meta{}`` section as a JSON-like dict"""
    runtime : Dict[str,E.Base]
    """``runtime{}`` section, with keys and corresponding expressions to be evaluated"""
    meta : Dict[str,Any]
    """``meta{}`` section as a JSON-like dict"""

    def __init__(self, pos : SourcePosition, name : str, inputs : List[Decl], postinputs : List[Decl],
                 command : E.String, outputs : List[Decl], parameter_meta : Dict[str,Any],
                 runtime : Dict[str,E.Base], meta : Dict[str,Any]) -> None:
        super().__init__(pos)
        self.name = name
        self.inputs = inputs
        self.postinputs = postinputs
        self.command = command
        self.outputs = outputs
        self.parameter_meta = parameter_meta
        self.runtime = runtime
        self.meta = meta
        # TODO: enforce validity constraints on parameter_meta and runtime
        # TODO: complain of name collisions in inputs/postinputs

    def typecheck(self, type_env : Env.Types = []) -> None:
        for decl in (self.inputs+self.postinputs):
            type_env = _typecheck_decl(decl, type_env)
        self.command.infer_type(type_env).typecheck(T.String())
        for decl in self.outputs:
            type_env = _typecheck_decl(decl, type_env)
        # TODO: check runtime section

    @property
    def required_inputs(self) -> List[Decl]:
        return [decl for decl in (self.inputs+self.postinputs) if decl.expr is None]

# type-check a declaration within a type environment, and return the type
# environment with the new binding
def _typecheck_decl(decl : Decl, type_env : Env.Types) -> Env.Types:
    if decl.expr is not None:
        decl.expr.infer_type(type_env).typecheck(decl.type)
    ans : Env.Types = Env.bind(decl.name, decl.type, type_env)
    return ans

# forward-declaration of Document and Workflow types
TVDocument = TypeVar('TVDocument',bound='Document')
TVWorkflow = TypeVar('TVWorkflow',bound='Workflow')

class Call(SourceNode):
    """A call (within a workflow) to a task or sub-workflow"""
    callee_id : E.Ident
    """Identifier of the desired task/workflow"""
    name : str
    """Name of the call (defaults to task/workflow name)"""
    inputs: Dict[str,E.Base]
    """Call inputs provided"""

    callee : Optional[Union[Task,TVWorkflow]]
    """After the AST is typechecked, refers to the Task or Workflow object to call"""

    def __init__(self, pos : SourcePosition, callee_id : E.Ident, alias : Optional[str], inputs : Dict[str,E.Base]) -> None:
        super().__init__(pos)
        self.callee_id = callee_id
        self.name = alias if alias is not None else self.callee_id.name
        self.inputs = inputs
        self.callee = None

    def typecheck(self, type_env : Env.Types, doc : TVDocument) -> Env.Types:
        # resolve callee_id to a known task/workflow, either within the
        # current document or one of its imported sub-documents
        if len(self.callee_id.namespace) == 0:
            callee_doc = doc
        elif len(self.callee_id.namespace) == 1:
            for (uri,ns,subdoc) in doc.imports:
                if ns == self.callee_id.namespace[0]:
                    callee_doc = subdoc
        if callee_doc:
            assert isinstance(callee_doc,Document)
            if callee_doc.workflow and callee_doc.workflow.name == self.callee_id.name:
                self.callee = callee_doc.workflow
            else:
                for task in callee_doc.tasks:
                    if task.name == self.callee_id.name:
                        self.callee = task
        if self.callee is None:
            raise Err.UnknownIdentifier(self.callee_id)

        # typecheck call inputs against task/workflow input declarations
        for name, expr in self.inputs.items():
            decl = None
            if isinstance(self.callee, Task):
                for d in self.callee.postinputs:
                    if d.name == name:
                        decl = d
                for d in self.callee.inputs:
                    if d.name == name:
                        decl = d
            else:
                assert isinstance(self.callee, Workflow)
                for ele in self.callee.elements:
                    if isinstance(ele, Decl) and ele.name == name:
                        decl = ele
            if decl is None:
                raise Err.NoSuchInput(expr, name)
            else:
                expr.infer_type(type_env).typecheck(decl.type)

        # return a TypeEnv with ONLY the outputs (not including the input TypeEnv)
        outputs_env = []
        for outp in self.callee.outputs:
            outputs_env = Env.bind(outp.name, outp.type, outputs_env)
        return outputs_env

# Given a type environment, recursively promote each binding of type T to Array[T]
def _arrayize_types(type_env : Env.Types) -> Env.Types:
    ans = []
    for node in type_env:
        if isinstance(node, Env.Binding):
            ans.append(Env.Binding(node.name, T.Array(node.rhs)))
        elif isinstance(node, Env.Namespace):
            ans.append(Env.Namespace(node.namespace, _arrayize_types(node.bindings)))
        else:
            assert False
    return ans

# Given a type environment, recursively make each binding optional
def _optionalize_types(type_env : Env.Types) -> Env.Types:
    ans = []
    for node in type_env:
        if isinstance(node, Env.Binding):
            ty = copy.copy(node.rhs)
            ty.optional = True
            ans.append(Env.Binding(node.name, ty))
        elif isinstance(node, Env.Namespace):
            ans.append(Env.Namespace(node.namespace, _optionalize_types(node.bindings)))
        else:
            assert False
    return ans

TVScatter = TypeVar("TVScatter", bound="Scatter")
TVConditional = TypeVar("TVConditional", bound="Conditional")

# typecheck the workflow elements and return a type environment with the
# outputs of the calls within (only -- not including the input type env)
def _typecheck_workflow_body(elements : List[Union[Decl,Call,TVScatter,TVConditional]], type_env : Env.Types, doc : TVDocument) -> Env.Types:
    outputs_env = []

    for element in elements:
        if isinstance(element, Decl):
            # typecheck the declaration and add binding to type environment
            type_env = _typecheck_decl(element, type_env)
            outputs_env = Env.bind(element.name, element.type, outputs_env)
        elif isinstance(element, Call):
            call_outputs_env = element.typecheck(type_env, doc)
            # add call outputs to type environment, under the call namespace
            # TODO: complain of namespace collisions
            type_env = Env.namespace(element.name, call_outputs_env, type_env)
            outputs_env = Env.namespace(element.name, call_outputs_env, outputs_env)
        elif isinstance(element, Scatter) or isinstance(element, Conditional):
            # add outputs of calls within the subscatter to the type environment.
            sub_outputs_env = element.typecheck(type_env, doc)
            type_env = sub_outputs_env + type_env
            outputs_env = sub_outputs_env + outputs_env
        else:
            assert False

    return outputs_env

class Scatter(SourceNode):
    """A scatter stanza within a workflow"""
    variable : str
    """The scatter variable name"""
    expr : E.Base
    """Expression for the array over which to scatter"""
    elements : List[Union[Decl,Call,TVScatter,TVConditional]]
    """Scatter body"""

    def __init__(self, pos : SourcePosition, variable : str, expr : E.Base, elements : List[Union[Decl,Call,TVScatter,TVConditional]]) -> None:
        super().__init__(pos)
        self.variable = variable
        self.expr = expr
        self.elements = elements

    def typecheck(self, type_env : Env.Types, doc : TVDocument) -> Env.Types:
        # typecheck the array to determine the element type
        self.expr.infer_type(type_env)
        if not isinstance(self.expr.type, T.Array):
            raise Err.NotAnArray(self.expr)
        if self.expr.type.item_type is None:
            return type_env

        # add scatter variable to environment for typechecking body
        type_env = Env.bind(self.variable, self.expr.type.item_type, type_env)
        outputs_env = _typecheck_workflow_body(self.elements, type_env, doc)

        # promote each output type T to Array[T]
        return _arrayize_types(outputs_env)

class Conditional(SourceNode):
    """A conditional (if) stanza within a workflow"""
    expr : E.Base
    """Boolean expression"""
    elements : List[Union[Decl,Call,TVScatter,TVConditional]]
    """Conditional body"""

    def __init__(self, pos : SourcePosition, expr : E.Base, elements : List[Union[Decl,Call,TVScatter,TVConditional]]) -> None:
        super().__init__(pos)
        self.expr = expr
        self.elements = elements

    def typecheck(self, type_env : Env.Types, doc : TVDocument) -> Env.Types:
        # check expr : Boolean
        self.expr.infer_type(type_env)
        if not self.expr.type.coerces(T.Boolean()):
            raise Err.StaticTypeMismatch(self.expr, T.Boolean(), self.expr.type)

        outputs_env = _typecheck_workflow_body(self.elements, type_env, doc)

        # promote each output type T to T?
        return _optionalize_types(outputs_env)

class Workflow(SourceNode):
    name : str
    """Workflow name"""
    elements: List[Union[Decl,Call,Scatter,Conditional]]
    """Declarations, calls, and/or scatters"""
    outputs: Optional[List[Decl]]
    """Workflow outputs"""
    parameter_meta : Dict[str,Any]
    """``parameter_meta{}`` section as a JSON-like dict"""
    meta : Dict[str,Any]
    """``meta{}`` section as a JSON-like dict"""

    def __init__(self, pos : SourcePosition, name : str, elements : List[Union[Decl,Call,Scatter]], outputs : Optional[List[Decl]], parameter_meta : Dict[str,Any], meta : Dict[str,Any]) -> None:
        super().__init__(pos)
        self.name = name
        self.elements = elements
        self.outputs = outputs
        self.parameter_meta = parameter_meta
        self.meta = meta

    def typecheck(self, doc : TVDocument) -> None:
        type_env = _typecheck_workflow_body(self.elements, [], doc)

        # typecheck the output declarations
        if self.outputs is not None:
            for output in self.outputs:
                _typecheck_decl(output, type_env)

class Document(SourceNode):
    """Top-level document, with imports, tasks, and a workflow. Typically returned by :func:`~WDL.load` with imported sub-documents loaded, and everything typechecked. Alternatively, :func:`~WDL.parse_document` constructs the AST but doesn't process imports nor perform typechecking."""
    imports : List[Tuple[str,str,Optional[TVDocument]]]
    """Imports in the document (filename/URI, namespace, and later the sub-document)"""
    tasks : List[Task]
    """Tasks in the document"""
    workflow : Optional[Workflow]
    """Workflow in the document, if any"""

    def __init__(self, pos : SourcePosition, imports : List[Tuple[str,str]],
                 tasks : List[Task], workflow : Optional[Workflow]) -> None:
        super().__init__(pos)
        self.imports = []
        for (uri,namespace) in imports:
            # TODO: complain of namespace collisions

            # The sub-document is initially None. The WDL.load() function
            # populates it, after construction of this object but before
            # typechecking the contents.
            self.imports.append((uri,namespace,None))
        self.tasks = tasks
        self.workflow = workflow

        # TODO: complain about name collisions amongst tasks and/or the workflow

    def typecheck(self) -> None:
        """Typecheck each task in the document, then the workflow, if any. Documents returned by :func:`~WDL.load` have already been typechecked."""
        # typecheck each task
        for task in self.tasks:
            task.typecheck()
        # typecheck the workflow
        if self.workflow:
            self.workflow.typecheck(self)

def load(uri : str, path : List[str] = []) -> Document:
    for fn in ([uri] + [os.path.join(dn, uri) for dn in reversed(path)]):
        if os.path.exists(fn):
            with open(fn, 'r') as infile:
                # read and parse the document
                doc = WDL._parser.parse_document(infile.read(), uri)
                assert isinstance(doc, Document)
                # recursively descend into document's imports, and store the imported
                # documents into doc.imports
                # TODO: limit recursion; prevent mutual recursion
                for i in range(len(doc.imports)):
                    subdoc = load(doc.imports[i][0], [os.path.dirname(fn)]+path)
                    doc.imports[i] = (doc.imports[i][0], doc.imports[i][1], subdoc)
                doc.typecheck()
                return doc
    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), uri)
