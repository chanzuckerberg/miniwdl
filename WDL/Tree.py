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
from typing import Any, List, Optional, Dict, TypeVar, Tuple, Union, Iterable
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

    def __init__(
        self, pos: SourcePosition, type: T.Base, name: str, expr: Optional[E.Base] = None
    ) -> None:
        super().__init__(pos)
        self.type = type
        self.name = name
        self.expr = expr

    def __str__(self) -> str:
        if self.expr is None:
            return "{} {}".format(str(self.type), self.name)
        return "{} {} = {}".format(str(self.type), self.name, str(self.expr))

    @property
    def children(self) -> Iterable[SourceNode]:
        if self.expr:
            yield self.expr

    def add_to_type_env(self, type_env: Env.Types) -> Env.Types:
        # Add an appropriate binding in the type env, after checking for name
        # collision.
        try:
            Env.resolve(type_env, [], self.name)
            raise Err.MultipleDefinitions(self, "Multiple declarations of " + self.name)
        except KeyError:
            pass
        ans: Env.Types = Env.bind(self.name, self.type, type_env, ctx=self)
        return ans

    def typecheck(self, type_env: Env.Types, check_quant: bool) -> None:
        # Infer the expression's type and ensure it checks against the declared
        # type. One time use!
        #
        # Subtlety: accept Array[T]+ = <expr> is accepted even if we can't
        # statically prove <expr> is nonempty. Its nonemptiness should be
        # checked at runtime. We do reject an empty array literal for <expr>.
        if self.expr:
            check_type = self.type
            if isinstance(check_type, T.Array):
                if check_type.nonempty and isinstance(self.expr, E.Array) and not self.expr.items:
                    raise Err.EmptyArray(self.expr)
                check_type = check_type.copy(nonempty=False)
            self.expr.infer_type(type_env, check_quant).typecheck(check_type)

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

    def __init__(
        self,
        pos: SourcePosition,
        name: str,
        inputs: List[Decl],
        postinputs: List[Decl],
        command: E.String,
        outputs: List[Decl],
        parameter_meta: Dict[str, Any],
        runtime: Dict[str, E.Base],
        meta: Dict[str, Any],
    ) -> None:
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

    @property
    def children(self) -> Iterable[SourceNode]:
        for d in self.inputs:
            yield d
        for d in self.postinputs:
            yield d
        yield self.command
        for d in self.outputs:
            yield d
        for _, ex in self.runtime.items():
            yield ex

    def typecheck(self, check_quant: bool = True) -> None:
        # First collect a type environment for all the input & postinput
        # declarations, so that we're prepared for possible forward-references
        # in their right-hand side expressions.
        type_env = []
        for decl in self.inputs + self.postinputs:
            type_env = decl.add_to_type_env(type_env)
        # Pass through input & postinput declarations again, typecheck their
        # right-hand side expressions against the type environment.
        for decl in self.inputs + self.postinputs:
            decl.typecheck(type_env, check_quant)
        # TODO: detect circular dependencies among input & postinput decls
        # Typecheck the command (string)
        self.command.infer_type(type_env, check_quant).typecheck(T.String())
        # Typecheck runtime expressions
        for _, runtime_expr in self.runtime.items():
            runtime_expr.infer_type(type_env, check_quant).typecheck(T.String())
        # Add output declarations to type environment
        for decl in self.outputs:
            type_env = decl.add_to_type_env(type_env)
        # Typecheck the output expressions
        for decl in self.outputs:
            decl.typecheck(type_env, check_quant)
        # TODO: detect circularities in output declarations

    @property
    def required_inputs(self) -> List[Decl]:
        return [
            decl
            for decl in (self.inputs + self.postinputs)
            if decl.expr is None and decl.type.optional is False
        ]


# forward-declarations
TVScatter = TypeVar("TVScatter", bound="Scatter")
TVConditional = TypeVar("TVConditional", bound="Conditional")
TVDocument = TypeVar("TVDocument", bound="Document")
TVWorkflow = TypeVar("TVWorkflow", bound="Workflow")


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

    def __init__(
        self,
        pos: SourcePosition,
        callee_id: E.Ident,
        alias: Optional[str],
        inputs: Dict[str, E.Base],
    ) -> None:
        super().__init__(pos)
        self.callee_id = callee_id
        self.name = alias if alias is not None else self.callee_id.name
        self.inputs = inputs
        self.callee = None

    @property
    def children(self) -> Iterable[SourceNode]:
        yield self.callee_id
        for _, ex in self.inputs.items():
            yield ex

    def resolve(self, doc: TVDocument) -> None:
        # Set self.callee to the Task/Workflow being called. Use exactly once
        # prior to add_to_type_env() or typecheck_input()
        if self.callee:
            return
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

    def add_to_type_env(self, type_env: Env.Types) -> Env.Types:
        # Add the call's outputs to the type environment under the appropriate
        # namespace, after checking for namespace collisions.
        assert self.callee
        try:
            Env.resolve_namespace(type_env, [self.name])
            raise Err.MultipleDefinitions(
                self,
                "Workflow has multiple calls named {}; give calls distinct names using `call {} as NAME ...`".format(
                    self.name, self.callee.name
                ),
            )
        except KeyError:
            pass
        outputs_env = []
        for outp in self.callee.outputs:
            outputs_env = Env.bind(outp.name, outp.type, outputs_env, ctx=self)
        return Env.namespace(self.name, outputs_env, type_env)

    def typecheck_input(self, type_env: Env.Types, doc: TVDocument, check_quant: bool) -> None:
        # Check the input expressions against the callee's inputs. One-time use
        assert self.callee

        # Make a set of the input names which are required for this call to
        # typecheck. In the top-level workflow, nothing is actually required
        # as missing call inputs become workflow inputs required at runtime.
        required_inputs = (
            set(decl.name for decl in self.callee.required_inputs) if doc.imported else set()
        )

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
            expr.infer_type(type_env, check_quant).typecheck(decl.type)
            if name in required_inputs:
                required_inputs.remove(name)

        # Check whether any required inputs were missed
        if required_inputs:
            raise Err.MissingInput(self, self.name, required_inputs)


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

    _type_env: Optional[Env.Types] = None
    """
    After typechecking: the type environment, inside the scatter, consisting of
    - all available declarations outside of the scatter
    - the scatter variable (singleton)
    - declarations in the scatter (singleton)
    - call outputs in the scatter (singleton)
    - declarations & outputs in sub-scatter sections (as arrays)
    - declarations & outputs in sub-conditional sections (as optionals)
    """

    def __init__(
        self,
        pos: SourcePosition,
        variable: str,
        expr: E.Base,
        elements: List[Union[Decl, Call, TVScatter, TVConditional]],
    ) -> None:
        super().__init__(pos)
        self.variable = variable
        self.expr = expr
        self.elements = elements

    @property
    def children(self) -> Iterable[SourceNode]:
        yield self.expr
        for elt in self.elements:
            yield elt

    def add_to_type_env(self, type_env: Env.Types) -> Env.Types:
        # Add declarations and call outputs in this section as they'll be
        # available outside of the section (i.e. a declaration of type T is
        # seen as Array[T] outside)
        inner_type_env = []
        for elt in self.elements:
            inner_type_env = elt.add_to_type_env(inner_type_env)
        nonempty = False
        if isinstance(self.expr._type, T.Array):
            # Subtlety: if the scatter array is statically nonempty, then so
            # too are the arrayized values
            nonempty = self.expr.type.nonempty
        return _arrayize_types(inner_type_env, nonempty) + type_env


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

    _type_env: Optional[Env.Types] = None
    """
    After typechecking: the type environment, inside the conditional:
    - all available declarations outside of the conditional
    - declarations in the conditional
    - call outputs in the conditional
    - declarations & outputs in sub-scatter sections (as arrays)
    - declarations & outputs in sub-conditional sections (as optionals)
    """

    def __init__(
        self,
        pos: SourcePosition,
        expr: E.Base,
        elements: List[Union[Decl, Call, TVScatter, TVConditional]],
    ) -> None:
        super().__init__(pos)
        self.expr = expr
        self.elements = elements

    @property
    def children(self) -> Iterable[SourceNode]:
        yield self.expr
        for elt in self.elements:
            yield elt

    def add_to_type_env(self, type_env: Env.Types) -> Env.Types:
        # Add declarations and call outputs in this section as they'll be
        # available outside of the section (i.e. a declaration of type T is
        # seen as T? outside)
        inner_type_env = []
        for elt in self.elements:
            inner_type_env = elt.add_to_type_env(inner_type_env)
        return _optionalize_types(inner_type_env) + type_env


class Workflow(SourceNode):
    name: str
    ":type: str"
    elements: List[Union[Decl, Call, Scatter, Conditional]]
    ":type: List[Union[WDL.Tree.Decl,WDL.Tree.Call,WDL.Tree.Scatter,WDL.Tree.Conditional]]"
    outputs: Optional[List[Decl]]
    """:type: Optional[List[Decl]]

    Workflow outputs, if the ``output{}`` section is present"""
    _output_idents: Optional[List[E.Ident]]
    parameter_meta: Dict[str, Any]
    """
    :type: Dict[str,Any]

    ``parameter_meta{}`` section as a JSON-like dict"""
    meta: Dict[str, Any]
    """
    :type: Dict[str,Any]

    ``meta{}`` section as a JSON-like dict"""

    _type_env: Optional[Env.Types] = None
    """
    After typechecking: the type environment in the main workflow body,
    - declarations at the top level of the workflow body
    - outputs of calls at the top level the workflow body
    - declarations & outputs inside scatter sections (as arrays)
    - declarations & outputs inside conditional sections (as optionals)
    """

    def __init__(
        self,
        pos: SourcePosition,
        name: str,
        elements: List[Union[Decl, Call, Scatter]],
        outputs: Optional[List[Decl]],
        parameter_meta: Dict[str, Any],
        meta: Dict[str, Any],
        output_idents: Optional[List[E.Ident]] = None,
    ) -> None:
        super().__init__(pos)
        self.name = name
        self.elements = elements
        self.outputs = outputs
        self._output_idents = output_idents
        self.parameter_meta = parameter_meta
        self.meta = meta

    @property
    def children(self) -> Iterable[SourceNode]:
        for elt in self.elements:
            yield elt
        if self.outputs:
            for d in self.outputs:
                yield d

    @property
    def required_inputs(self) -> List[Decl]:
        return [
            decl
            for decl in self.elements
            if isinstance(decl, Decl) and decl.expr is None and decl.type.optional is False
        ]

    def typecheck(self, doc: TVDocument, check_quant: bool) -> None:
        assert doc.workflow is self
        assert self._type_env is None
        # 1. resolve all calls
        _resolve_calls(doc)
        # 2. build type environments in the workflow and each scatter &
        #    conditional section therein
        _build_workflow_type_env(doc, check_quant)
        # 3. typecheck the right-hand side expressions of each declaration
        #    and the inputs to each call (descending into scatter & conditional
        #    sections)
        _typecheck_workflow_elements(doc, check_quant)
        # 4. convert deprecated output_idents, if any, to output declarations
        self._rewrite_output_idents()
        # 5. typecheck the output expressions
        if self.outputs:
            output_names = set()
            for output in self.outputs:
                assert output.expr
                if output.name in output_names:
                    raise Err.MultipleDefinitions(
                        output, "multiple workflow outputs named " + output.name
                    )
                output.typecheck(self._type_env, check_quant)
                output_names.add(output.name)

    def _rewrite_output_idents(self) -> None:
        if self._output_idents:
            assert self.outputs is not None

            # for each listed identifier, formulate a synthetic declaration
            output_ident_decls = []
            for output_idents in self._output_idents:
                output_idents = [output_idents]

                if output_idents[0].name == "*":
                    # wildcard: expand to each call output
                    wildcard = output_idents[0]
                    output_idents = []
                    try:
                        for binding in Env.resolve_namespace(self._type_env, wildcard.namespace):
                            binding_name = binding.name
                            assert isinstance(binding_name, str)
                            output_idents.append(
                                E.Ident(wildcard.pos, wildcard.namespace + [binding_name])
                            )
                    except KeyError:
                        raise Err.UnknownIdentifier(wildcard)

                for output_ident in output_idents:
                    try:
                        ty = Env.resolve(self._type_env, output_ident.namespace, output_ident.name)
                    except KeyError:
                        raise Err.UnknownIdentifier(output_ident)
                    assert isinstance(ty, T.Base)
                    # the output name is supposed to be 'fully qualified'
                    # including the call namespace. we're going to stick it
                    # into the decl name with a ., which is a weird corner
                    # case!
                    synthetic_output_name = ".".join(output_ident.namespace + [output_ident.name])
                    output_ident_decls.append(
                        Decl(output_ident.pos, ty, synthetic_output_name, output_ident)
                    )

            # put the synthetic declarations into self.outputs
            self.outputs = output_ident_decls + self.outputs  # pyre-fixme
            self._output_idents = None


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

    def __init__(
        self,
        pos: SourcePosition,
        imports: List[Tuple[str, str]],
        tasks: List[Task],
        workflow: Optional[Workflow],
        imported: bool,
    ) -> None:
        super().__init__(pos)
        self.imports = []
        for (uri, namespace) in imports:
            # The sub-document is initially None. The WDL.load() function
            # populates it, after construction of this object but before
            # typechecking the contents.
            self.imports.append((uri, namespace, None))
        self.tasks = tasks
        self.workflow = workflow
        self.imported = imported

    @property
    def children(self) -> Iterable[SourceNode]:
        for _, _, doc in self.imports:
            if doc:
                yield doc
        for task in self.tasks:
            yield task
        if self.workflow:
            yield self.workflow

    def typecheck(self, check_quant: bool = True) -> None:
        """Typecheck each task in the document, then the workflow, if any.

        Documents returned by :func:`~WDL.load` have already been typechecked."""
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
            task.typecheck(check_quant=check_quant)
        # typecheck the workflow
        if self.workflow:
            if self.workflow.name in names:
                raise Err.MultipleDefinitions(
                    self.workflow,
                    "Workflow name collides with a task also named " + self.workflow.name,
                )
            self.workflow.typecheck(self, check_quant=check_quant)


def load(
    uri: str, path: List[str] = [], check_quant: bool = True, imported: Optional[bool] = False
) -> Document:
    for fn in [uri] + [os.path.join(dn, uri) for dn in reversed(path)]:
        if os.path.exists(fn):
            with open(fn, "r") as infile:
                # read and parse the document
                doc = WDL._parser.parse_document(infile.read(), uri=uri, imported=imported)
                assert isinstance(doc, Document)
                # recursively descend into document's imports, and store the imported
                # documents into doc.imports
                # TODO: limit recursion; prevent mutual recursion
                for i in range(len(doc.imports)):
                    try:
                        subpath = [os.path.dirname(fn)] + path
                        subdoc = load(
                            doc.imports[i][0], subpath, check_quant=check_quant, imported=True
                        )
                    except Exception as exn:
                        raise Err.ImportError(uri, doc.imports[i][0]) from exn
                    doc.imports[i] = (doc.imports[i][0], doc.imports[i][1], subdoc)
                doc.typecheck(check_quant=check_quant)
                return doc
    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), uri)


#
# Typechecking helper functions
#


def _resolve_calls(
    doc: Document, element: Optional[Union[Workflow, Scatter, Conditional]] = None
) -> None:
    # Resolve all calls in the workflow (descending into scatter & conditional
    # sections)
    element = element or doc.workflow
    if element:
        for child in element.elements:
            if isinstance(child, Call):
                child.resolve(doc)
            elif isinstance(child, (Scatter, Conditional)):
                _resolve_calls(doc, child)  # pyre-ignore


def _build_workflow_type_env(
    doc: TVDocument,
    check_quant: bool,
    self: Optional[Union[Workflow, Scatter, Conditional]] = None,
    outer_type_env: Env.Types = [],
) -> None:
    # Populate each Workflow, Scatter, and Conditional object with its
    # _type_env attribute containing the type environment available in the body
    # of the respective section. This is tricky because:
    # - forward-references to any declaration or call output in the workflow
    #   are valid, except
    #   - circular dependencies, direct or indirect
    #   - (corollary) scatter and conditional expressions can't refer to
    #     anything within the respective section
    # - a scatter variable is visible only inside the scatter
    # - declarations & call outputs of type T within a scatter have type
    #   Array[T] outside of the scatter
    # - declarations & call outputs of type T within a conditional have type T?
    #   outside of the conditional
    #
    # preconditions:
    # - _resolve_calls()
    #
    # postconditions:
    # - typechecks scatter and conditional expressions (recursively)
    # - sets _type_env attributes on each Workflow/Scatter/Conditional
    self = self or doc.workflow
    if not self:
        return
    assert isinstance(self, (Scatter, Conditional)) or self is doc.workflow
    assert self._type_env is None

    # When we've been called recursively on a scatter or conditional section,
    # the 'outer' type environment has everything available in the workflow
    # -except- the body of self.
    type_env = outer_type_env
    if isinstance(self, Scatter):
        # typecheck scatter array
        self.expr.infer_type(type_env, check_quant)
        if not isinstance(self.expr.type, T.Array):
            raise Err.NotAnArray(self.expr)
        if self.expr.type.item_type is None:
            raise Err.EmptyArray(self.expr)
        # bind the scatter variable to the array item type within the body
        try:
            Env.resolve(type_env, [], self.variable)
            raise Err.MultipleDefinitions(
                self, "Name collision for scatter variable " + self.variable
            )
        except KeyError:
            pass
        type_env = Env.bind(self.variable, self.expr.type.item_type, type_env, ctx=self)
    elif isinstance(self, Conditional):
        # typecheck the condition
        self.expr.infer_type(type_env, check_quant)
        if not self.expr.type.coerces(T.Boolean()):
            raise Err.StaticTypeMismatch(self.expr, T.Boolean(), self.expr.type)

    # descend into child scatter & conditional elements, if any.
    for child in self.elements:
        if isinstance(child, (Scatter, Conditional)):
            # prepare the 'outer' type environment for the child element, by
            # adding all its sibling declarations and call outputs
            child_outer_type_env = type_env
            for sibling in self.elements:
                if sibling is not child:
                    child_outer_type_env = sibling.add_to_type_env(child_outer_type_env)
            _build_workflow_type_env(doc, check_quant, child, child_outer_type_env)

    # finally, populate self._type_env with all our children
    for child in self.elements:
        type_env = child.add_to_type_env(type_env)
    self._type_env = type_env


def _arrayize_types(type_env: Env.Types, nonempty: bool = False) -> Env.Types:
    # Given a type environment, recursively promote each binding of type T to
    # Array[T] -- used in Scatter.add_to_type_env
    ans = []
    for node in type_env:
        if isinstance(node, Env.Binding):
            ans.append(Env.Binding(node.name, T.Array(node.rhs, nonempty=nonempty), node.ctx))
        elif isinstance(node, Env.Namespace):
            ans.append(Env.Namespace(node.namespace, _arrayize_types(node.bindings, nonempty)))
        else:
            assert False
    return ans


def _optionalize_types(type_env: Env.Types) -> Env.Types:
    # Given a type environment, recursively make each binding optional -- used
    # in Conditional.add_to_type_env
    ans = []
    for node in type_env:
        if isinstance(node, Env.Binding):
            ty = node.rhs.copy(optional=True)
            ans.append(Env.Binding(node.name, ty, node.ctx))
        elif isinstance(node, Env.Namespace):
            ans.append(Env.Namespace(node.namespace, _optionalize_types(node.bindings)))
        else:
            assert False
    return ans


def _typecheck_workflow_elements(
    doc: Document, check_quant: bool, self: Optional[Union[Workflow, Scatter, Conditional]] = None
) -> None:
    # following _resolve_calls() and _build_workflow_type_env(), typecheck all
    # the declaration expressions and call inputs
    self = self or doc.workflow
    assert self and (self._type_env is not None)
    for child in self.elements:
        if isinstance(child, Decl):
            child.typecheck(self._type_env, check_quant)
        elif isinstance(child, Call):
            child.typecheck_input(self._type_env, doc, check_quant)
        elif isinstance(child, (Scatter, Conditional)):
            _typecheck_workflow_elements(doc, check_quant, child)  # pyre-ignore
        else:
            assert False
