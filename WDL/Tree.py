# pyre-strict
"""
Abstract syntax tree (AST) for WDL documents, containing tasks and workflows,
which contain declarations, calls, and scatter & if sections. The AST is
typically constructed and returned by :func:`~WDL.load` or
:func:`~WDL.parse_document`.

The ``WDL.Tree.*`` classes are also exported by the base ``WDL`` module, i.e.
``WDL.Tree.Document`` can be abbreviated ``WDL.Document``.
"""

import os
import errno
from typing import Any, List, Optional, Dict, TypeVar, Tuple, Union
import WDL.Type as T
import WDL.Expr as E
import WDL.Env as Env
import WDL.Error as Err
from WDL.Error import SourcePosition, SourceNode
import WDL._parser


class Decl(SourceNode):
    """A variable declaration within a task or workflow"""
    type: T.Base
    ":type: WDL.Type.Base"
    name: str
    """Declared variable name

    :type: str"""
    expr: Optional[E.Base]
    """Bound expression, if any

    :type: Optional[WDL.Expr.Base]"""

    def __init__(self, pos: SourcePosition, type: T.Base, name: str,
                 expr: Optional[E.Base] = None) -> None:
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
    name: str
    """:type: str"""
    inputs: List[Decl]
    """:type: List[WDL.Tree.Decl]

    Inputs declared within the ``input{}`` task section
    """
    postinputs: List[Decl]
    """:type: List[WDL.Tree.Decl]

    Declarations outside of the ``input{}`` task section"""
    command: E.String
    ":type: WDL.Expr.String"
    outputs: List[Decl]
    """:type: List[WDL.Tree.Decl]

    Output declarations"""
    parameter_meta: Dict[str, Any]
    """:type: Dict[str,Any]

    ``parameter_meta{}`` section as a JSON-like dict"""
    runtime: Dict[str, E.Base]
    """:type: Dict[str,WDL.Expr.Base]

    ``runtime{}`` section, with keys and corresponding expressions to be evaluated"""
    meta: Dict[str, Any]
    """:type: Dict[str,Any]

    ``meta{}`` section as a JSON-like dict"""

    def __init__(self,
                 pos: SourcePosition,
                 name: str,
                 inputs: List[Decl],
                 postinputs: List[Decl],
                 command: E.String,
                 outputs: List[Decl],
                 parameter_meta: Dict[str, Any],
                 runtime: Dict[str, E.Base],
                 meta: Dict[str, Any]) -> None:
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

    def typecheck(self) -> None:
        # First collect a type environment for all the input & postinput
        # declarations, so that we're prepared for possible forward-references
        # in their right-hand side expressions.
        type_env = []
        for decl in self.inputs + self.postinputs:
            type_env = _add_decl_to_type_env(decl, type_env)
        # Pass through input & postinput declarations again, typecheck their
        # right-hand side expressions against the type environment.
        for decl in self.inputs + self.postinputs:
            _typecheck_decl_expr(decl, type_env) # pyre-ignore
        # TODO: detect circular dependencies among input & postinput decls
        # Typecheck the command (string)
        self.command.infer_type(type_env).typecheck(T.String())
        # Add output declarations to type environment
        for decl in self.outputs:
            type_env = _add_decl_to_type_env(decl, type_env)
        # Typecheck the output expressions
        for decl in self.outputs:
            _typecheck_decl_expr(decl, type_env) # pyre-ignore
        # TODO: detect circularities in output declarations
        # TODO: check runtime section

    @property
    def required_inputs(self) -> List[Decl]:
        return [decl for decl in (self.inputs + self.postinputs)
                if decl.expr is None and decl.type.optional is False]

def _add_decl_to_type_env(decl: Decl, type_env: Env.Types) -> Env.Types:
    try:
        Env.resolve(type_env, [], decl.name)
        raise Err.MultipleDefinitions(decl, "Multiple declarations of " + decl.name)
    except KeyError:
        pass
    ans: Env.Types = Env.bind(decl.name, decl.type, type_env)
    return ans


def _typecheck_decl_expr(decl: Decl, type_env: Env.Types) -> None:
    # 2. A declaration of Array[T]+ = <expr> is accepted even if we can't
    #    prove <expr> is nonempty statically. Its nonemptiness should be
    #    checked at runtime. Exception when <expr> is an empty array literal
    if decl.expr:
        check_type = decl.type
        if isinstance(check_type, T.Array):
            if check_type.nonempty and isinstance(decl.expr, E.Array) and not decl.expr.items:
                raise Err.EmptyArray(decl.expr)
            check_type = check_type.copy(nonempty=False)
        decl.expr.infer_type(type_env).typecheck(check_type)


# type-check a declaration within a type environment, and return the type
# environment with the new binding


def _typecheck_decl(decl: Decl, type_env: Env.Types) -> Env.Types:
    try:
        Env.resolve(type_env, [], decl.name)
        raise Err.MultipleDefinitions(decl, "Multiple declarations of " + decl.name)
    except KeyError:
        pass
    # Subtleties:
    # 1. In a declaration like: String? x = "who", we record x in the type
    #    environment as String instead of String? since it won't actually
    #    be null at runtime
    # 2. A declaration of Array[T]+ = <expr> is accepted even if we can't
    #    prove <expr> is nonempty statically. Its nonemptiness should be
    #    checked at runtime. Exception when <expr> is an empty array literal
    nonnull = False
    if decl.expr is not None:
        check_type = decl.type
        if isinstance(check_type, T.Array):
            if check_type.nonempty and isinstance(decl.expr, E.Array) and not decl.expr.items:
                raise Err.EmptyArray(decl.expr)
            check_type = check_type.copy(nonempty=False)
        decl.expr.infer_type(type_env).typecheck(check_type)
        if decl.expr.type.optional is False:
            nonnull = True
    ty = decl.type.copy(optional=False) if nonnull else decl.type
    ans: Env.Types = Env.bind(decl.name, ty, type_env)
    return ans


# forward-declaration of Document and Workflow types
TVDocument = TypeVar('TVDocument', bound='Document')
TVWorkflow = TypeVar('TVWorkflow', bound='Workflow')


class Call(SourceNode):
    """A call (within a workflow) to a task or sub-workflow"""
    callee_id: E.Ident
    """
    :type: WDL.Expr.Ident

    Identifier of the desired task/workflow"""
    name: str
    """:type: string

    defaults to task/workflow name"""
    inputs: Dict[str, E.Base]
    """
    :type: Dict[str,WDL.Expr.Base]

    Call inputs provided"""

    callee: Optional[Union[Task, TVWorkflow]]
    """
    :type: Union[WDL.Tree.Task, WDL.Tree.Workflow]

    After the AST is typechecked, refers to the Task or Workflow object to call"""

    def __init__(self, pos: SourcePosition, callee_id: E.Ident,
                 alias: Optional[str], inputs: Dict[str, E.Base]) -> None:
        super().__init__(pos)
        self.callee_id = callee_id
        self.name = alias if alias is not None else self.callee_id.name
        self.inputs = inputs
        self.callee = None

    def typecheck(self, type_env: Env.Types, doc: TVDocument) -> Env.Types:
        # resolve callee_id to a known task/workflow, either within the
        # current document or one of its imported sub-documents
        if not self.callee_id.namespace:
            callee_doc = doc
        elif len(self.callee_id.namespace) == 1:
            for _, ns, subdoc in doc.imports:
                if ns == self.callee_id.namespace[0]:
                    callee_doc = subdoc
        if callee_doc:
            assert isinstance(callee_doc, Document)
            if callee_doc.workflow and callee_doc.workflow.name == self.callee_id.name:
                self.callee = callee_doc.workflow
            else:
                for task in callee_doc.tasks:
                    if task.name == self.callee_id.name:
                        self.callee = task
        if self.callee is None:
            raise Err.UnknownIdentifier(self.callee_id)
        assert isinstance(self.callee, (Task, Workflow))

        # Make a set of the input names which are required for this call to
        # typecheck. In the top-level workflow, nothing is actually required
        # as missing call inputs become workflow inputs required at runtime.
        required_inputs = set(
            decl.name for decl in self.callee.required_inputs) if doc.imported else set()

        # typecheck call inputs against task/workflow input declarations
        for name, expr in self.inputs.items():
            decl = None
            if isinstance(self.callee, Task):
                for d in self.callee.inputs + self.callee.postinputs:
                    if d.name == name:
                        decl = d
            else:
                assert isinstance(self.callee, Workflow)
                for ele in self.callee.elements:
                    if isinstance(ele, Decl) and ele.name == name:
                        decl = ele
            if decl is None:
                raise Err.NoSuchInput(expr, name)
            expr.infer_type(type_env).typecheck(decl.type)
            if name in required_inputs:
                required_inputs.remove(name)

        if required_inputs:
            raise Err.MissingInput(self, self.name, required_inputs)

        # return a TypeEnv with ONLY the outputs (not including the input
        # TypeEnv)
        outputs_env = []
        for outp in self.callee.outputs:
            outputs_env = Env.bind(outp.name, outp.type, outputs_env)
        return outputs_env

# Given a type environment, recursively promote each binding of type T to
# Array[T]


def _arrayize_types(type_env: Env.Types) -> Env.Types:
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


def _optionalize_types(type_env: Env.Types) -> Env.Types:
    ans = []
    for node in type_env:
        if isinstance(node, Env.Binding):
            ty = node.rhs.copy(optional=True)
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


def _typecheck_workflow_body(elements: List[Union[Decl, Call, TVScatter, TVConditional]],
                             type_env: Env.Types,
                             doc: TVDocument) -> Env.Types:
    outputs_env = []

    for element in elements:
        if isinstance(element, Decl):
            # typecheck the declaration and add binding to type environment
            type_env = _typecheck_decl(element, type_env)
            outputs_env = Env.bind(element.name, element.type, outputs_env)
        elif isinstance(element, Call):
            call_outputs_env = element.typecheck(type_env, doc)
            # add call outputs to type environment, under the call namespace
            try:
                Env.resolve_namespace(type_env, [element.name])
                raise Err.MultipleDefinitions(
                    element,
                    "Workflow has multiple calls named {}; give calls distinct names using `call {} as NAME ...`".format(
                        element.name,
                        element.callee.name))
            except KeyError:
                pass
            type_env = Env.namespace(element.name, call_outputs_env, type_env)
            outputs_env = Env.namespace(element.name, call_outputs_env, outputs_env)
        elif isinstance(element, (Scatter, Conditional)):
            # add outputs of calls within the subscatter to the type
            # environment.
            sub_outputs_env = element.typecheck(type_env, doc)
            type_env = sub_outputs_env + type_env
            outputs_env = sub_outputs_env + outputs_env
        else:
            assert False

    return outputs_env


class Scatter(SourceNode):
    """A scatter stanza within a workflow"""
    variable: str
    """
    :type: string

    Scatter variable name"""
    expr: E.Base
    """
    :type: WDL.Expr.Base

    Expression for the array over which to scatter"""
    elements: List[Union[Decl, Call, TVScatter, TVConditional]]
    """
    :type: List[Union[WDL.Tree.Decl,WDL.Tree.Call,WDL.Tree.Scatter,WDL.Tree.Conditional]]

    Scatter body"""

    def __init__(self, pos: SourcePosition, variable: str, expr: E.Base,
                 elements: List[Union[Decl, Call, TVScatter, TVConditional]]) -> None:
        super().__init__(pos)
        self.variable = variable
        self.expr = expr
        self.elements = elements

    def typecheck(self, type_env: Env.Types, doc: TVDocument) -> Env.Types:
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
    expr: E.Base
    """
    :tree: WDL.Expr.Base

    Boolean expression"""
    elements: List[Union[Decl, Call, TVScatter, TVConditional]]
    """
    :type: List[Union[WDL.Tree.Decl,WDL.Tree.Call,WDL.Tree.Scatter,WDL.Tree.Conditional]]

    Conditional body"""

    def __init__(self,
                 pos: SourcePosition,
                 expr: E.Base,
                 elements: List[Union[Decl, Call, TVScatter, TVConditional]]) -> None:
        super().__init__(pos)
        self.expr = expr
        self.elements = elements

    def typecheck(self, type_env: Env.Types, doc: TVDocument) -> Env.Types:
        # check expr : Boolean
        self.expr.infer_type(type_env)
        if not self.expr.type.coerces(T.Boolean()):
            raise Err.StaticTypeMismatch(self.expr, T.Boolean(), self.expr.type)

        outputs_env = _typecheck_workflow_body(self.elements, type_env, doc)

        # promote each output type T to T?
        return _optionalize_types(outputs_env)


class Workflow(SourceNode):
    name: str
    ":type: str"
    elements: List[Union[Decl, Call, Scatter, Conditional]]
    ":type: List[Union[WDL.Tree.Decl,WDL.Tree.Call,WDL.Tree.Scatter,WDL.Tree.Conditional]]"
    outputs: Optional[List[Decl]]
    """:type: Optional[List[Decl]]

    Workflow outputs, if the ``output{}`` stanza is present"""
    parameter_meta: Dict[str, Any]
    """
    :type: Dict[str,Any]

    ``parameter_meta{}`` section as a JSON-like dict"""
    meta: Dict[str, Any]
    """
    :type: Dict[str,Any]

    ``meta{}`` section as a JSON-like dict"""

    def __init__(self,
                 pos: SourcePosition,
                 name: str,
                 elements: List[Union[Decl, Call, Scatter]],
                 outputs: Optional[List[Decl]],
                 parameter_meta: Dict[str, Any],
                 meta: Dict[str,
                            Any]) -> None:
        super().__init__(pos)
        self.name = name
        self.elements = elements
        self.outputs = outputs
        self.parameter_meta = parameter_meta
        self.meta = meta

    def typecheck(self, doc: TVDocument) -> None:
        type_env = _typecheck_workflow_body(self.elements, [], doc)

        # typecheck the output declarations
        if self.outputs is not None:
            for output in self.outputs:
                _typecheck_decl(output, type_env)

    @property
    def required_inputs(self) -> List[Decl]:
        return [decl for decl in self.elements if isinstance(
            decl, Decl) and decl.expr is None and decl.type.optional is False]


class Document(SourceNode):
    """
    Top-level document, with imports, tasks, and a workflow. Typically returned
    by :func:`~WDL.load` with imported sub-documents loaded, and everything
    typechecked. Alternatively, :func:`~WDL.parse_document` constructs the AST
    but doesn't process imports nor perform typechecking.
    """
    imports: List[Tuple[str, str, Optional[TVDocument]]]
    """
    :type: List[Tuple[str,str,Optional[WDL.Tree.Document]]]

    Imports in the document (filename/URI, namespace, and later the sub-document)"""
    tasks: List[Task]
    """:type: List[WDL.Tree.Task]"""
    workflow: Optional[Workflow]
    """:type: Optional[WDL.Tree.Workflow]"""

    imported: bool
    """
    :type: bool

    True iff this document has been loaded as an import from another document"""

    def __init__(self,
                 pos: SourcePosition,
                 imports: List[Tuple[str, str]],
                 tasks: List[Task],
                 workflow: Optional[Workflow],
                 imported: bool) -> None:
        super().__init__(pos)
        self.imports = []
        for (uri, namespace) in imports:
            # TODO: complain of namespace collisions

            # The sub-document is initially None. The WDL.load() function
            # populates it, after construction of this object but before
            # typechecking the contents.
            self.imports.append((uri, namespace, None))
        self.tasks = tasks
        self.workflow = workflow
        self.imported = imported

        # TODO: complain about name collisions amongst tasks and/or the
        # workflow

    def typecheck(self) -> None:
        """Typecheck each task in the document, then the workflow, if any. Documents returned by :func:`~WDL.load` have already been typechecked."""
        names = set()
        for _, namespace, _ in self.imports:
            if namespace in names:
                raise Err.MultipleDefinitions(self, "Multiple imports with namespace " + namespace)
            names.add(namespace)
        names = set()
        # typecheck each task
        for task in self.tasks:
            if task.name in names:
                raise Err.MultipleDefinitions(task, "Multiple tasks named " + task.name)
            names.add(task.name)
            task.typecheck()
        # typecheck the workflow
        if self.workflow:
            if self.workflow.name in names:
                raise Err.MultipleDefinitions(
                    self.workflow,
                    "Workflow name collides with a task also named " +
                    self.workflow.name)
            self.workflow.typecheck(self)


def load(uri: str, path: List[str] = [], imported: Optional[bool] = False) -> Document:
    for fn in ([uri] + [os.path.join(dn, uri) for dn in reversed(path)]):
        if os.path.exists(fn):
            with open(fn, 'r') as infile:
                # read and parse the document
                doc = WDL._parser.parse_document(infile.read(), uri, imported)
                assert isinstance(doc, Document)
                # recursively descend into document's imports, and store the imported
                # documents into doc.imports
                # TODO: limit recursion; prevent mutual recursion
                for i in range(len(doc.imports)):
                    try:
                        subpath = [os.path.dirname(fn)] + path
                        subdoc = load(doc.imports[i][0], subpath, True)
                    except Exception as exn:
                        raise Err.ImportError(uri, doc.imports[i][0]) from exn
                    doc.imports[i] = (doc.imports[i][0],
                                      doc.imports[i][1], subdoc)
                doc.typecheck()
                return doc
    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), uri)
