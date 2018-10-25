# pyre-strict
"""
A WDL Document (source file) includes tasks and up to one workflow
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Callable, TypeVar, Tuple, Union
import WDL.Type as T
import WDL.Value as V
import WDL.Expr as E
import WDL.Env as Env
import WDL.Error as Err
from WDL.Error import SourcePosition, SourceNode

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
        """Infer and check types on all input/output declarations and the command, including any expression placeholders within the command"""
        for decl in (self.inputs+self.postinputs):
            type_env = _typecheck_decl(decl, type_env)
        self.command.infer_type(type_env).typecheck(T.String())
        for decl in self.outputs:
            type_env = _typecheck_decl(decl, type_env)
        # TODO: check runtime section

    @property
    def required_inputs(self) -> List[Decl]:
        return [decl for decl in (self.inputs+self.postinputs) if decl.expr is None]

def _typecheck_decl(decl : Decl, type_env : Env.Types) -> Env.Types:
    if decl.expr is not None:
        decl.expr.infer_type(type_env).typecheck(decl.type)
    ans : Env.Types = Env.bind(decl.name, decl.type, type_env)
    return ans

# forward-declaration of Document type
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
    """After typechecking, holds the task/workflow object to call"""

    def __init__(self, pos : SourcePosition, callee_id : E.Ident, alias : Optional[str], inputs : Dict[str,E.Base]) -> None:
        super().__init__(pos)
        self.callee_id = callee_id
        self.name = alias if alias is not None else self.callee_id.name
        self.inputs = inputs
        self.callee = None

    def typecheck(self, type_env : Env.Types, doc : TVDocument) -> Env.Types:
        """Resolve the callee_id within the type environment, and check the types of provided inputs against the callee_id inputs. Return a type environment describing the call outputs only."""

        # resolve callee_id to a known task/workflow, either within the
        # current document or one of its imported sub-documents
        if len(self.callee_id.namespace) == 0:
            callee_doc = doc
        elif len(self.callee_id.namespace) == 1:
            for (uri,ns,subdoc) in doc.imports:
                if ns == self.callee_id.namespace[0]:
                    callee_doc = subdoc
        if callee_doc:
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

TVScatter = TypeVar("TVScatter", bound="Scatter")
class Scatter(SourceNode):
    """A scatter stanza within a workflow"""
    variable : str
    """The scatter variable name"""
    expr : E.Base
    """Expression for the array over which to scatter"""
    elements: List[Union[Decl,Call,TVScatter]]
    """Calls and/or bound declarations"""

    def __init__(self, pos : SourcePosition, variable : str, expr : E.Base, elements : List[Union[Decl,Call]]) -> None:
        super().__init__(pos)
        self.variable = variable
        self.expr = expr
        self.elements = elements

    def typecheck(self, type_env : Env.Types, doc : TVDocument) -> Env.Types:
        """Typecheck the scatter array and each element of the body; return a type environment describing the scatter outputs only (namespaced with their respective call names)."""

        # typecheck the array to determine the element type
        self.expr.infer_type(type_env)
        if not isinstance(self.expr.type, T.Array):
            raise Err.NotAnArray(self.expr)
        if self.expr.type.item_type is None:
            return type_env

        # type environment within the scatter body, including the scatter
        # variable and call outputs as they appear
        type_env = Env.bind(self.variable, self.expr.type.item_type, type_env)
        # type environment with the call outputs only
        outputs_env = []

        for element in self.elements:
            if isinstance(element, Decl):
                type_env = _typecheck_decl(element, type_env)
                # are declarations within scatters visible as arrays after the scatter?
            elif isinstance(element, Call):
                call_outputs_env = element.typecheck(type_env, doc)
                type_env = Env.namespace(element.name, call_outputs_env, type_env)
                outputs_env = Env.namespace(element.name, call_outputs_env, outputs_env)
            elif isinstance(element, Scatter):
                subscatter_outputs_env = element.typecheck(type_env, doc)
                type_env = subscatter_outputs_env + type_env
                outputs_env = subscatter_outputs_env + outputs_env
            else:
                assert False

        # promote each output type t to Array[t]
        return Env.arrayize(outputs_env)

class Workflow(SourceNode):
    name : str
    """Workflow name"""
    elements: List[Union[Decl,Call,Scatter]]
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
        """Typecheck each workflow element and the outputs, given all the tasks/subworkflows available to be called."""

        type_env = []
        for element in self.elements:
            if isinstance(element, Decl):
                type_env = _typecheck_decl(element, type_env)
            elif isinstance(element, Call):
                outputs_env = element.typecheck(type_env, doc)
                type_env = Env.namespace(element.name, outputs_env, type_env)
            elif isinstance(element, Scatter):
                outputs_env = element.typecheck(type_env, doc)
                type_env = outputs_env + type_env
            else:
                assert False

        # typecheck the output declarations
        if self.outputs is not None:
            for output in self.outputs:
                _typecheck_decl(output, type_env)

class Document(SourceNode):
    """Top-level document"""
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
