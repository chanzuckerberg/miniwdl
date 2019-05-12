"""
Abstract syntax tree (AST) for WDL documents, containing tasks and workflows,
which contain declarations, calls, and scatter & if sections. The AST is
typically constructed and returned by :func:`~WDL.load` or
:func:`~WDL.parse_document`.

The ``WDL.Tree.*`` classes are also exported by the base ``WDL`` module, i.e.
``WDL.Tree.Document`` can be abbreviated ``WDL.Document``.

.. inheritance-diagram:: WDL.Tree
"""

import os
import errno
from typing import (
    Any,
    List,
    Optional,
    Dict,
    TypeVar,
    Tuple,
    Union,
    Iterable,
    Callable,
    Generator,
    Set,
    NamedTuple,
)
import WDL.Type as T
import WDL.Expr as E
import WDL.Env as Env
import WDL.Error as Err
import WDL.StdLib
from WDL.Error import SourcePosition, SourceNode
import WDL._parser


class StructTypeDef(SourceNode):
    """WDL struct type definition"""

    name: str
    """
    :type: str

    Name of the struct type (in the current document)
    """

    members: Dict[str, T.Base]
    """
    :type: Dict[str, WDL.Type.Base]

    Member names and types
    """

    imported: bool
    """
    :type: bool

    True if this struct type was imported from another document
    """

    def __init__(
        self, pos: SourcePosition, name: str, members: Dict[str, T.Base], imported: bool = False
    ) -> None:
        super().__init__(pos)
        self.name = name
        self.members = members
        self.imported = imported

    @property
    def type_id(self) -> str:
        """
        :type: str

        A string uniquely describing the member names and types, excluding the struct type name; useful to identify aliased struct types.
        """
        return T._struct_type_id(self.members)


class Decl(SourceNode):
    """A value declaration within a task or workflow"""

    type: T.Base
    ":type: WDL.Type.Base"
    name: str
    """Declared value name

    :type: str"""
    expr: Optional[E.Base]
    """:type: Optional[WDL.Expr.Base]

    Bound expression, if any"""

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

    __repr__ = __str__

    @property
    def children(self) -> Iterable[SourceNode]:
        if self.expr:
            yield self.expr

    def add_to_type_env(
        self, struct_typedefs: Env.StructTypeDefs, type_env: Env.Types
    ) -> Env.Types:
        # Add an appropriate binding in the type env, after checking for name
        # collision.
        try:
            Env.resolve(type_env, [], self.name)
            raise Err.MultipleDefinitions(self, "Multiple declarations of " + self.name)
        except KeyError:
            pass
        try:
            Env.resolve_namespace(type_env, [self.name])
            raise Err.MultipleDefinitions(self, "Value/call name collision on " + self.name)
        except KeyError:
            pass
        _resolve_struct_typedefs(self.pos, self.type, struct_typedefs)
        if isinstance(self.type, T.StructInstance):
            return _add_struct_instance_to_type_env([self.name], self.type, type_env, ctx=self)
        return Env.bind(type_env, [], self.name, self.type, ctx=self)

    def typecheck(
        self,
        type_env: Env.Types,
        stdlib: Optional[WDL.StdLib.Base] = None,
        check_quant: bool = True,
    ) -> None:
        # Infer the expression's type and ensure it checks against the declared
        # type. One time use!
        if self.expr:
            self.expr.infer_type(type_env, stdlib=stdlib, check_quant=check_quant).typecheck(
                self.type
            )

    # TODO: when the declaration is evaluated,
    #  - the optional/nonempty type quantifiers should be checked
    #  - String to File coercion


class Task(SourceNode):
    """WDL Task"""

    name: str
    """:type: str"""
    inputs: Optional[List[Decl]]
    """:type: Optional[List[WDL.Tree.Decl]]

    Declarations in the ``input{}`` task section, if it's present"""
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
        inputs: Optional[List[Decl]],
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
        # TODO: if the input section exists, then all postinputs decls must be
        #       bound

    @property
    def available_inputs(self) -> Env.Decls:
        """:type: WDL.Env.Decls

        Yields the task's input declarations. This is all declarations in the
        task's ``input{}`` section, if it's present. Otherwise, it's all
        declarations in the task, excluding outputs. (This dichotomy bridges
        pre-1.0 and 1.0+ WDL versions.)

        Each input is at the top level of the Env, with no namespace.
        """
        ans = []
        for decl in self.inputs if self.inputs is not None else self.postinputs:
            ans = Env.bind(ans, [], decl.name, decl)
        return ans

    @property
    def required_inputs(self) -> Env.Decls:
        """:type: WDL.Env.Decls

        Yields the input declarations which are required to call the task
        (available inputs that are unbound and non-optional).

        Each input is at the top level of the Env, with no namespace.
        """
        ans: Env.Decls = []
        for b in self.available_inputs:
            assert isinstance(b, Env.Binding)
            d: Decl = b.rhs
            if d.expr is None and d.type.optional is False:
                ans.append(b)
        return ans

    @property
    def effective_outputs(self) -> Env.Types:
        """:type: WDL.Env.Types

        Yields each task output with its type, at the top level of the Env with
        no namespace. (Present for isomorphism with
        ``Workflow.effective_outputs``)
        """
        ans = []
        for decl in self.outputs:
            ans = Env.bind(ans, [], decl.name, decl.type, ctx=decl)
        return ans

    @property
    def children(self) -> Iterable[SourceNode]:
        for d in self.inputs or []:
            yield d
        for d in self.postinputs:
            yield d
        yield self.command
        for d in self.outputs:
            yield d
        for _, ex in self.runtime.items():
            yield ex

    def typecheck(
        self, struct_typedefs: Optional[Env.StructTypeDefs] = None, check_quant: bool = True
    ) -> None:
        struct_typedefs = struct_typedefs or []
        # warm-up check: if input{} section exists then all postinput decls
        # must be bound
        if self.inputs is not None:
            for decl in self.postinputs:
                if not decl.expr:
                    raise Err.StrayInputDeclaration(
                        self,
                        "unbound declaration {} {} outside task input{} section".format(
                            str(decl.type), decl.name, "{}"
                        ),
                    )
        # First collect a type environment for all the input & postinput
        # declarations, so that we're prepared for possible forward-references
        # in their right-hand side expressions.
        type_env = []
        for decl in (self.inputs or []) + self.postinputs:
            type_env = decl.add_to_type_env(struct_typedefs, type_env)

        with Err.multi_context() as errors:
            # Pass through input & postinput declarations again, typecheck their
            # right-hand side expressions against the type environment.
            for decl in (self.inputs or []) + self.postinputs:
                errors.try1(lambda: decl.typecheck(type_env, check_quant=check_quant))
            # Typecheck the command (string)
            errors.try1(
                lambda: self.command.infer_type(type_env, check_quant=check_quant).typecheck(
                    T.String()
                )
            )
            # Typecheck runtime expressions
            for _, runtime_expr in self.runtime.items():
                errors.try1(
                    lambda runtime_expr=runtime_expr: runtime_expr.infer_type(
                        type_env, check_quant=check_quant
                    ).typecheck(T.String())
                )
            # Add output declarations to type environment
            for decl in self.outputs:
                type_env2 = errors.try1(
                    lambda decl=decl: decl.add_to_type_env(struct_typedefs, type_env)
                )
                if type_env2:
                    type_env = type_env2
            errors.maybe_raise()
            # Typecheck the output expressions
            for decl in self.outputs:
                errors.try1(lambda: decl.typecheck(type_env, check_quant=check_quant))

        # check for cyclic dependencies among decls
        _detect_cycles(
            # pyre-ignore
            _dependency_matrix(ch for ch in self.children if isinstance(ch, Decl))
        )


# forward-declarations
TVScatter = TypeVar("TVScatter", bound="Scatter")
TVConditional = TypeVar("TVConditional", bound="Conditional")
TVDocument = TypeVar("TVDocument", bound="Document")


class Call(SourceNode):
    """A call (within a workflow) to a task or sub-workflow"""

    callee_id: List[str]
    """
    :type: List[str]

    Namespaced identifier of the desired task/workflow"""
    name: str
    """:type: string

    defaults to task/workflow name"""
    inputs: Dict[str, E.Base]
    """
    :type: Dict[str,WDL.Expr.Base]

    Call inputs provided"""

    callee: Optional[Union[Task, "Workflow"]]
    """
    :type: Union[WDL.Tree.Task, WDL.Tree.Workflow]

    After the AST is typechecked, refers to the Task or Workflow object to call"""

    def __init__(
        self,
        pos: SourcePosition,
        callee_id: List[str],
        alias: Optional[str],
        inputs: Dict[str, E.Base],
    ) -> None:
        super().__init__(pos)
        assert callee_id
        self.callee_id = callee_id
        self.name = alias if alias is not None else self.callee_id[-1]
        self.inputs = inputs
        self.callee = None

    @property
    def children(self) -> Iterable[SourceNode]:
        for _, ex in self.inputs.items():
            yield ex

    def resolve(self, doc: TVDocument) -> None:
        # Set self.callee to the Task/Workflow being called. Use exactly once
        # prior to add_to_type_env() or typecheck_input()
        if self.callee:
            return
        callee_doc = None
        if len(self.callee_id) == 1:
            callee_doc = doc
        elif len(self.callee_id) == 2:
            for imp in doc.imports:
                if imp.namespace == self.callee_id[0]:
                    callee_doc = imp.doc
        if callee_doc:
            assert isinstance(callee_doc, Document)
            if callee_doc.workflow and callee_doc.workflow.name == self.callee_id[-1]:
                if not callee_doc.workflow.complete_calls or (
                    callee_doc.workflow.outputs is None and callee_doc.workflow.effective_outputs
                ):
                    raise Err.UncallableWorkflow(self, ".".join(self.callee_id))
                self.callee = callee_doc.workflow
            else:
                for task in callee_doc.tasks:
                    if task.name == self.callee_id[-1]:
                        self.callee = task
        if self.callee is None:
            raise Err.NoSuchTask(self, ".".join(self.callee_id))
        assert isinstance(self.callee, (Task, Workflow))

    def add_to_type_env(
        self, struct_typedefs: Env.StructTypeDefs, type_env: Env.Types
    ) -> Env.Types:
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
        try:
            Env.resolve(type_env, [], self.name)
            raise Err.MultipleDefinitions(self, "Value/call name collision on " + self.name)
        except KeyError:
            pass
        return self.effective_outputs + type_env

    def typecheck_input(self, type_env: Env.Types, check_quant: bool) -> bool:
        # Check the input expressions against the callee's inputs. One-time use.
        # Returns True if the call supplies all required inputs, False otherwise.
        assert self.callee

        # Make a set of the input names which are required for this call
        # pyre-fixme
        required_inputs = set(decl.name for decl in self.callee.required_inputs)

        # typecheck call inputs against task/workflow input declarations
        with Err.multi_context() as errors:
            for name, expr in self.inputs.items():
                try:
                    decl = Env.resolve(self.callee.available_inputs, [], name)
                    errors.try1(
                        lambda expr=expr, decl=decl: expr.infer_type(
                            type_env, check_quant=check_quant
                        ).typecheck(decl.type)
                    )
                except KeyError:
                    errors.append(Err.NoSuchInput(expr, name))
                if name in required_inputs:
                    required_inputs.remove(name)
        assert (not required_inputs) == (not list(self.required_inputs))
        return not required_inputs

    @property
    def available_inputs(self) -> Env.Decls:
        """:type: WDL.Env.Decls

        Yields the task/workflow inputs which are *not* supplied in the call
        ``inputs:``, and thus may be supplied at workflow launch; in namespaces
        according to the call names.
        """
        assert self.callee

        supplied_inputs = set(self.inputs.keys())
        ans: Env.Decls = []
        for b in self.callee.available_inputs:
            if (isinstance(b, Env.Binding) and b.name not in supplied_inputs) or isinstance(
                b, Env.Namespace
            ):
                ans.append(b)
        if ans:
            ans = [Env.Namespace(self.name, ans)]
        return ans

    @property
    def required_inputs(self) -> Env.Decls:
        """:type: WDL.Env.Decls

        Yields the required task/workflow inputs which are *not* supplied in
        the call ``inputs:`` (incomplete calls), and thus must be supplied at
        workflow launch; in namespaces according to the call name.
        """
        assert self.callee

        supplied_inputs = set(self.inputs.keys())
        ans: Env.Decls = []
        for b in self.callee.required_inputs:
            if (isinstance(b, Env.Binding) and b.name not in supplied_inputs) or isinstance(
                b, Env.Namespace
            ):
                ans.append(b)
        if ans:
            ans = [Env.Namespace(self.name, ans)]
        return ans

    @property
    def effective_outputs(self) -> Env.Types:
        """:type: WDL.Env.Decls

        Yields the effective outputs of the callee Task or Workflow, in a
        namespace according to the call name.
        """
        ans: Env.Types = []
        assert self.callee
        for outp in self.callee.effective_outputs:
            if isinstance(outp, Env.Binding):
                ans = Env.bind(ans, [self.name], outp.name, outp.rhs, ctx=self)
        return ans


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
        self.elements = elements  # pyre-ignore

    @property
    def children(self) -> Iterable[SourceNode]:
        yield self.expr
        for elt in self.elements:
            yield elt

    def add_to_type_env(
        self, struct_typedefs: Env.StructTypeDefs, type_env: Env.Types
    ) -> Env.Types:
        # Add declarations and call outputs in this section as they'll be
        # available outside of the section (i.e. a declaration of type T is
        # seen as Array[T] outside)
        inner_type_env: Env.Types = []
        for elt in self.elements:
            inner_type_env = elt.add_to_type_env(struct_typedefs, inner_type_env)
        # Subtlety: if the scatter array is statically nonempty, then so too
        # are the arrayized values.
        nonempty = isinstance(self.expr._type, T.Array) and self.expr._type.nonempty
        inner_type_env = Env.map(inner_type_env, lambda ns, b: T.Array(b.rhs, nonempty=nonempty))
        return inner_type_env + type_env

    @property
    def effective_outputs(self) -> Env.Types:
        # Yield the outputs of calls in this section and subsections, typed
        # and namespaced appropriately, as they'll be propagated if the
        # workflow lacks an explicit output{} section
        nonempty = isinstance(self.expr._type, T.Array) and self.expr._type.nonempty
        ans: Env.Types = []
        for elt in self.elements:
            if not isinstance(elt, Decl):
                ans = elt.effective_outputs + ans
        ans = Env.map(ans, lambda ns, b: T.Array(b.rhs, nonempty=nonempty))
        return ans


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
        self.elements = elements  # pyre-ignore

    @property
    def children(self) -> Iterable[SourceNode]:
        yield self.expr
        for elt in self.elements:
            yield elt

    def add_to_type_env(
        self, struct_typedefs: Env.StructTypeDefs, type_env: Env.Types
    ) -> Env.Types:
        # Add declarations and call outputs in this section as they'll be
        # available outside of the section (i.e. a declaration of type T is
        # seen as T? outside)
        inner_type_env = []
        for elt in self.elements:
            inner_type_env = elt.add_to_type_env(struct_typedefs, inner_type_env)
        return Env.map(inner_type_env, lambda ns, b: b.rhs.copy(optional=True)) + type_env

    @property
    def effective_outputs(self) -> Env.Types:
        # Yield the outputs of calls in this section and subsections, typed
        # and namespaced appropriately, as they'll be propagated if the
        # workflow lacks an explicit output{} section
        ans = []
        for elt in self.elements:
            if not isinstance(elt, Decl):
                ans = elt.effective_outputs + ans
        return Env.map(ans, lambda ns, b: b.rhs.copy(optional=True))


class Workflow(SourceNode):
    name: str
    ":type: str"
    inputs: Optional[List[Decl]]
    """:type: List[WDL.Tree.Decl]

    Declarations in the ``input{}`` workflow section, if it's present"""
    elements: List[Union[Decl, Call, Scatter, Conditional]]
    """:type: List[Union[WDL.Tree.Decl,WDL.Tree.Call,WDL.Tree.Scatter,WDL.Tree.Conditional]]

    Workflow body in between ``input{}`` and ``output{}`` sections, if any
    """
    outputs: Optional[List[Decl]]
    """:type: Optional[List[WDL.Tree.Decl]]

    Workflow output declarations, if the ``output{}`` section is present"""
    # following two fields temporarily hold old-style (pre 1.0) outputs with
    # bare identifiers or namespace wildcards. We postprocess them into
    # full declarations as expected in WDL 1.0+.
    _output_idents: List[List[str]]
    _output_idents_pos: Optional[Err.SourcePosition]
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

    complete_calls: bool
    """
    After typechecking, False if the workflow has a call which does not supply
    all required inputs (and thus cannot be called from another workflow).
    """

    def __init__(
        self,
        pos: SourcePosition,
        name: str,
        inputs: Optional[List[Decl]],
        elements: List[Union[Decl, Call, Scatter, Conditional]],
        outputs: Optional[List[Decl]],
        parameter_meta: Dict[str, Any],
        meta: Dict[str, Any],
        output_idents: Optional[List[List[str]]] = None,
        output_idents_pos: Optional[SourcePosition] = None,
    ) -> None:
        super().__init__(pos)
        self.name = name
        self.inputs = inputs
        self.elements = elements
        self.outputs = outputs
        self._output_idents = output_idents or []
        self._output_idents_pos = output_idents_pos
        self.parameter_meta = parameter_meta
        self.meta = meta
        self.complete_calls = True

    @property
    def available_inputs(self) -> Env.Decls:
        """:type: WDL.Env.Decls

        Yields the workflow's input declarations. This includes:

        1. If the ``input{}`` workflow section is present, all declarations
        within that section. Otherwise, all declarations in the workflow body,
        excluding outputs. (This dichotomy bridges pre-1.0 and 1.0+ WDL
        versions.) These appear at the top level of the Env, with no namepsace.

        2. Available inputs of all calls in the workflow, namespaced by the
        call names.
        """
        ans = []

        if self.inputs is not None:
            for decl in self.inputs:
                ans = Env.bind(ans, [], decl.name, decl)

        for elt in _decls_and_calls(self):
            if isinstance(elt, Decl):
                if self.inputs is None:
                    ans = Env.bind(ans, [], elt.name, elt)
            elif isinstance(elt, Call):
                ans = elt.available_inputs + ans
            else:
                assert False

        return ans

    @property
    def required_inputs(self) -> Env.Decls:
        """:type: WDL.Env.Decls

        Yields the subset of available inputs which are required to start the
        workflow."""
        ans = []

        if self.inputs is not None:
            for decl in self.inputs:
                if decl.expr is None and decl.type.optional is False:
                    ans = Env.bind(ans, [], decl.name, decl)

        for elt in _decls_and_calls(self):
            if isinstance(elt, Decl):
                if self.inputs is None and elt.expr is None and elt.type.optional is False:
                    ans = Env.bind(ans, [], elt.name, elt)
            elif isinstance(elt, Call):
                ans = elt.required_inputs + ans
            else:
                assert False

        return ans

    @property
    def effective_outputs(self) -> Env.Types:
        """:type: WDL.Env.Decls

        If the ``output{}`` workflow section is present, yields the names and
        types therein, at the top level of the Env. Otherwise, yield all the
        call outputs, namespaced and typed appropriately.
        """
        ans = []

        if self.outputs is not None:
            for decl in self.outputs:
                ans = Env.bind(ans, [], decl.name, decl.type, ctx=decl)
        else:
            for elt in self.elements:
                if not isinstance(elt, Decl):
                    ans = elt.effective_outputs + ans

        return ans

    @property
    def children(self) -> Iterable[SourceNode]:
        for d in self.inputs or []:
            yield d
        for elt in self.elements:
            yield elt
        for d in self.outputs or []:
            yield d

    def typecheck(self, doc: TVDocument, check_quant: bool) -> None:
        assert doc.workflow is self
        assert self._type_env is None
        # 1. resolve all calls
        _resolve_calls(doc)
        # 2. build type environments in the workflow and each scatter &
        #    conditional section therein
        _build_workflow_type_env(doc, check_quant)
        with Err.multi_context() as errors:
            # 3. typecheck the right-hand side expressions of each declaration
            #    and the inputs to each call (descending into scatter & conditional
            #    sections)
            for decl in self.inputs or []:
                errors.try1(
                    lambda decl=decl: decl.typecheck(self._type_env, check_quant=check_quant)
                )
            if errors.try1(lambda: _typecheck_workflow_elements(doc, check_quant)) == False:
                self.complete_calls = False
            # 4. convert deprecated output_idents, if any, to output declarations
            if self._output_idents:
                self._rewrite_output_idents()
            # 5. typecheck the output expressions
            if self.outputs:
                output_names = set()
                for output in self.outputs:
                    assert output.expr
                    if output.name in output_names:
                        errors.append(
                            Err.MultipleDefinitions(
                                output, "multiple workflow outputs named " + output.name
                            )
                        )
                    errors.try1(
                        lambda output=output: output.typecheck(
                            self._type_env, check_quant=check_quant
                        )
                    )
                    output_names.add(output.name)
        # 6. check for cyclic dependencies
        _detect_cycles(_dependency_matrix(_decls_and_calls(self)))  # pyre-fixme

    def _rewrite_output_idents(self) -> None:
        # for pre-1.0 workflow output sections with a list of namespaced
        # identifiers (instead of bound decls)
        assert self._type_env is not None

        # for each listed identifier, formulate a synthetic declaration
        output_ident_decls = []
        for output_idents in self._output_idents:
            assert self._output_idents_pos
            output_idents = [output_idents]

            if output_idents[0][-1] == "*":
                # wildcard: expand to each call output
                wildcard_namespace = output_idents[0][:-1]
                output_idents = []
                try:
                    for binding in Env.resolve_namespace(self._type_env, wildcard_namespace):
                        assert isinstance(binding, Env.Binding)
                        binding_name = binding.name
                        assert isinstance(binding_name, str)
                        output_idents.append(wildcard_namespace + [binding_name])
                except KeyError:
                    raise Err.NoSuchTask(
                        self._output_idents_pos, ".".join(wildcard_namespace)
                    ) from None

            for output_ident in output_idents:
                try:
                    ty = Env.resolve(self._type_env, output_ident[:-1], output_ident[-1])
                except KeyError:
                    raise Err.UnknownIdentifier(
                        E.Ident(self._output_idents_pos, output_ident)
                    ) from None
                assert isinstance(ty, T.Base)
                # the output name is supposed to be 'fully qualified'
                # including the call namespace. we're going to stick it
                # into the decl name with a ., which is a weird corner
                # case!
                synthetic_output_name = ".".join(output_ident)
                output_ident_decls.append(
                    Decl(
                        self.pos,
                        ty,
                        synthetic_output_name,
                        E.Ident(self._output_idents_pos, output_ident),
                    )
                )

        # put the synthetic declarations into self.outputs
        self.outputs = output_ident_decls + self.outputs  # pyre-fixme
        self._output_idents = []


DocImport = NamedTuple(
    "DocImport",
    [
        ("pos", Err.SourcePosition),
        ("uri", str),
        ("namespace", str),
        ("aliases", List[Tuple[str, str]]),
        ("doc", "Optional[Document]"),
    ],
)
"""
Represents one imported document, with position of the import statement, import URI, namespace, struct type aliases, and (after typechecking) the ``Document`` object.
"""


class Document(SourceNode):
    """
    Top-level document, with imports, tasks, and a workflow. Typically returned
    by :func:`~WDL.load` with imported sub-documents loaded, and everything
    typechecked. Alternatively, :func:`~WDL.parse_document` constructs the AST
    but doesn't process imports nor perform typechecking.
    """

    imports: List[DocImport]
    """
    :type: List[DocImport]

    Imported documents"""
    struct_typedefs: Env.StructTypeDefs
    """:type: Dict[str, Dict[str, WDL.Type.Base]]"""
    tasks: List[Task]
    """:type: List[WDL.Tree.Task]"""
    workflow: Optional[Workflow]
    """:type: Optional[WDL.Tree.Workflow]"""

    def __init__(
        self,
        pos: SourcePosition,
        imports: List[DocImport],
        struct_typedefs: Dict[str, StructTypeDef],
        tasks: List[Task],
        workflow: Optional[Workflow],
    ) -> None:
        super().__init__(pos)
        self.imports = imports
        self.struct_typedefs = []
        for name, struct_typedef in struct_typedefs.items():
            self.struct_typedefs = Env.bind(self.struct_typedefs, [], name, struct_typedef)
        self.tasks = tasks
        self.workflow = workflow

    @property
    def children(self) -> Iterable[SourceNode]:
        for imp in self.imports:
            if imp.doc:
                yield imp.doc
        for stb in self.struct_typedefs:
            # pylint: disable=no-member
            assert isinstance(stb, Env.Binding) and isinstance(stb.rhs, StructTypeDef)
            yield stb.rhs
        for task in self.tasks:
            yield task
        if self.workflow:
            yield self.workflow

    def typecheck(self, check_quant: bool = True) -> None:
        """Typecheck each task in the document, then the workflow, if any.

        Documents returned by :func:`~WDL.load` have already been typechecked."""
        names = set()
        for imp in self.imports:
            if imp.namespace in names:
                raise Err.MultipleDefinitions(
                    self, "Multiple imports with namespace " + imp.namespace
                )
            names.add(imp.namespace)
        _import_structs(self)
        _initialize_struct_typedefs(self.struct_typedefs)
        names = set()
        # typecheck each task
        with Err.multi_context() as errors:
            for task in self.tasks:
                if task.name in names:
                    errors.append(
                        Err.MultipleDefinitions(task, "Multiple tasks named " + task.name)
                    )
                names.add(task.name)
                errors.try1(
                    lambda task=task: task.typecheck(self.struct_typedefs, check_quant=check_quant)
                )
        # typecheck the workflow
        if self.workflow:
            if self.workflow.name in names:
                raise Err.MultipleDefinitions(
                    self.workflow,
                    "Workflow name collides with a task also named " + self.workflow.name,
                )
            self.workflow.typecheck(self, check_quant=check_quant)


def load(
    uri: str,
    path: List[str] = [],
    check_quant: bool = True,
    import_uri: Optional[Callable[[str], str]] = None,
    import_max_depth=10,
) -> Document:
    if uri.startswith("file://"):
        uri = uri[7:]
    elif uri.find("://") > 0 and import_uri:
        uri = import_uri(uri)
    for fn in [uri] + [os.path.join(dn, uri) for dn in reversed(path)]:
        if os.path.exists(fn):
            with open(fn, "r") as infile:
                # read and parse the document
                source_text = infile.read()
                doc = WDL._parser.parse_document(source_text, uri=uri)
                assert isinstance(doc, Document)
                # recursively descend into document's imports, and store the imported
                # documents into doc.imports
                # TODO: are we supposed to do something smart for relative imports
                #       within a document loaded by URI?
                for i in range(len(doc.imports)):
                    imp = doc.imports[i]
                    if import_max_depth <= 1:
                        raise Err.ImportError(
                            uri, imp.uri, "exceeded import_max_depth; circular imports?"
                        )
                    try:
                        subpath = [os.path.dirname(fn)] + path
                        subdoc = load(
                            imp.uri,
                            subpath,
                            check_quant=check_quant,
                            import_uri=import_uri,
                            import_max_depth=(import_max_depth - 1),
                        )
                    except Exception as exn:
                        raise Err.ImportError(uri, imp.uri) from exn
                    doc.imports[i] = DocImport(
                        pos=imp.pos,
                        uri=imp.uri,
                        namespace=imp.namespace,
                        aliases=imp.aliases,
                        doc=subdoc,
                    )
                try:
                    doc.typecheck(check_quant=check_quant)
                except Err.ValidationError as exn:
                    exn.source_text = source_text
                    raise exn
                except Err.MultipleValidationErrors as multi:
                    for exn in multi.exceptions:
                        if not exn.source_text:
                            exn.source_text = source_text
                    raise multi
                return doc
    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), uri)


#
# Typechecking helper functions
#


def _decls_and_calls(
    element: Union[Workflow, Scatter, Conditional]
) -> Generator[Union[Decl, Call], None, None]:
    # Yield each Decl and Call in the workflow, including those nested within
    # scatter/conditional sections
    for ch in element.children:
        if isinstance(ch, (Decl, Call)):
            yield ch
        elif isinstance(ch, (Scatter, Conditional)):
            for gch in _decls_and_calls(ch):
                yield gch


def _resolve_calls(doc: Document) -> None:
    # Resolve all calls in the workflow (descending into scatter & conditional
    # sections)
    if doc.workflow:
        with Err.multi_context() as errors:
            for c in _decls_and_calls(doc.workflow):
                if isinstance(c, Call):
                    errors.try1(lambda c=c: c.resolve(doc))


def _build_workflow_type_env(
    doc: Document,
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
    assert doc.workflow
    self = self or doc.workflow
    if not self:
        return
    assert isinstance(self, (Scatter, Conditional)) or self is doc.workflow
    assert self._type_env is None

    # When we've been called recursively on a scatter or conditional section,
    # the 'outer' type environment has everything available in the workflow
    # -except- the body of self.
    type_env = outer_type_env

    if isinstance(self, Workflow):
        # start with workflow inputs
        for decl in self.inputs or []:
            type_env = decl.add_to_type_env(doc.struct_typedefs, type_env)
    elif isinstance(self, Scatter):
        # typecheck scatter array
        self.expr.infer_type(type_env, check_quant=check_quant)
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
        try:
            Env.resolve_namespace(type_env, [self.variable])
            raise Err.MultipleDefinitions(
                self, "Call name collision for scatter variable " + self.variable
            )
        except KeyError:
            pass
        type_env = Env.bind(type_env, [], self.variable, self.expr.type.item_type, ctx=self)
    elif isinstance(self, Conditional):
        # typecheck the condition
        self.expr.infer_type(type_env, check_quant=check_quant)
        if not self.expr.type.coerces(T.Boolean()):
            raise Err.StaticTypeMismatch(self.expr, T.Boolean(), self.expr.type)
    else:
        assert False

    # descend into child scatter & conditional elements, if any.
    for child in self.elements:
        if isinstance(child, (Scatter, Conditional)):
            # prepare the 'outer' type environment for the child element, by
            # adding all its sibling declarations and call outputs
            child_outer_type_env = type_env
            for sibling in self.elements:
                if sibling is not child:
                    child_outer_type_env = sibling.add_to_type_env(
                        doc.struct_typedefs, child_outer_type_env
                    )
            _build_workflow_type_env(doc, check_quant, child, child_outer_type_env)
        elif doc.workflow.inputs is not None and isinstance(child, Decl) and not child.expr:
            raise Err.StrayInputDeclaration(
                self,
                "unbound declaration {} {} outside workflow input{} section".format(
                    str(child.type), child.name, "{}"
                ),
            )

    # finally, populate self._type_env with all our children
    for child in self.elements:
        type_env = child.add_to_type_env(doc.struct_typedefs, type_env)
    self._type_env = type_env


def _typecheck_workflow_elements(
    doc: Document, check_quant: bool, self: Optional[Union[Workflow, Scatter, Conditional]] = None
) -> bool:
    # following _resolve_calls() and _build_workflow_type_env(), typecheck all
    # the declaration expressions and call inputs
    self = self or doc.workflow
    assert self and (self._type_env is not None)
    complete_calls = True
    with Err.multi_context() as errors:
        for child in self.elements:
            if isinstance(child, Decl):
                errors.try1(
                    _translate_struct_mismatch(
                        doc,
                        lambda child=child: child.typecheck(
                            self._type_env, check_quant=check_quant
                        ),
                    )
                )
            elif isinstance(child, Call):
                if (
                    errors.try1(
                        _translate_struct_mismatch(
                            doc,
                            lambda child=child: child.typecheck_input(
                                self._type_env, check_quant=check_quant
                            ),
                        )
                    )
                    == False
                ):
                    complete_calls = False
            elif isinstance(child, (Scatter, Conditional)):
                if (
                    errors.try1(
                        _translate_struct_mismatch(
                            doc,
                            lambda child=child: _typecheck_workflow_elements(
                                doc, check_quant, child
                            ),
                        )
                    )
                    == False
                ):
                    complete_calls = False
            else:
                assert False
    return complete_calls


def _translate_struct_mismatch(doc: Document, stmt: Callable[[], Any]) -> Callable[[], Any]:
    # When we get a StaticTypeMismatch error during workflow typechecking,
    # which involves a struct type imported from another document, the error
    # message may require translation from the struct type's original name
    # within in the imported document to its aliased name in the current
    # document.
    def f(doc=doc, stmt=stmt):
        try:
            return stmt()
        except Err.StaticTypeMismatch as exc:
            expected = exc.expected
            if isinstance(expected, T.StructInstance):
                for stb in doc.struct_typedefs:
                    assert isinstance(stb, Env.Binding) and isinstance(stb.rhs, StructTypeDef)
                    if id(stb.rhs.members) == id(expected.members):
                        expected = T.StructInstance(stb.name, optional=expected.optional)
                        expected.members = stb.rhs.members
            actual = exc.actual
            if isinstance(actual, T.StructInstance):
                for stb in doc.struct_typedefs:
                    assert isinstance(stb, Env.Binding) and isinstance(stb.rhs, StructTypeDef)
                    if id(stb.rhs.members) == id(actual.members):
                        actual = T.StructInstance(stb.name, optional=actual.optional)
                        actual.members = stb.rhs.members
            raise Err.StaticTypeMismatch(exc.node or exc.pos, expected, actual, exc.args[0])

    return f


class _AdjM:
    # A sparse adjacency matrix for topological sorting
    # which we should not have implemented ourselves
    _forward: Dict[int, Set[int]]
    _reverse: Dict[int, Set[int]]
    _unconstrained: Set[int]

    def __init__(self):
        self._forward = dict()
        self._reverse = dict()
        self._unconstrained = set()

    def sinks(self, source: int) -> Iterable[int]:
        for sink in self._forward.get(source, []):
            yield sink

    def sources(self, sink: int) -> Iterable[int]:
        for source in self._reverse.get(sink, []):
            yield source

    @property
    def nodes(self) -> Iterable[int]:
        for node in self._forward.keys():
            yield node

    @property
    def unconstrained(self) -> Iterable[int]:
        for n in self._unconstrained:
            assert not self._reverse[n]
            yield n

    def add_node(self, node: int):
        if node not in self._forward:
            assert node not in self._reverse
            self._forward[node] = set()
            self._reverse[node] = set()
            self._unconstrained.add(node)
        else:
            assert node in self._reverse

    def add_edge(self, source: int, sink: int):
        self.add_node(source)
        self.add_node(sink)
        if sink not in self._forward[source]:
            self._forward[source].add(sink)
            self._reverse[sink].add(source)
            if sink in self._unconstrained:
                self._unconstrained.remove(sink)
        else:
            assert source in self._reverse[sink]
            assert sink not in self._unconstrained

    def remove_edge(self, source: int, sink: int):
        if source in self._forward and sink in self._forward[source]:
            self._forward[source].remove(sink)
            self._reverse[sink].remove(source)
            if not self._reverse[sink]:
                self._unconstrained.add(sink)
        else:
            assert not (sink in self._reverse and source in self._reverse[sink])

    def remove_node(self, node: int):
        for source in list(self.sources(node)):
            self.remove_edge(source, node)
        for sink in list(self.sinks(node)):
            self.remove_edge(node, sink)
        del self._forward[node]
        del self._reverse[node]
        self._unconstrained.remove(node)


def _dependencies(obj: Union[Decl, Call, E.Base]) -> Iterable[Union[Decl, Call]]:
    # Yield each Decl/Call referenced by any Expr.Ident within the given
    # Decl/Call/Expr
    if isinstance(obj, Decl):
        if obj.expr:
            for dep in _dependencies(obj.expr):
                yield dep
    elif isinstance(obj, Call):
        for v in obj.inputs.values():
            for dep in _dependencies(v):
                yield dep
    elif isinstance(obj, E.Ident):
        if isinstance(obj.ctx, (Decl, Call)):
            yield obj.ctx
        else:
            assert isinstance(obj.ctx, Scatter)
    else:
        assert isinstance(obj, E.Base)
        for subexpr in obj.children:
            assert isinstance(subexpr, (Decl, Call, E.Base))
            for dep in _dependencies(subexpr):
                yield dep


def _dependency_matrix(
    objs: Iterable[Union[Decl, Call]], obj_id: Optional[Callable[[Err.SourceNode], int]] = None
) -> Tuple[Dict[int, Union[Decl, Call]], _AdjM]:
    # Given collection of Decl & Call, produce mapping of object ids
    # to the objects and the adjacency matrix for their dependencies
    # obj_id: get unique int id for object, defaults to id()
    obj_id = obj_id or id
    obj_ids = dict()
    adj = _AdjM()
    for obj in objs:
        oid = obj_id(obj)
        assert oid not in obj_ids or id(obj) == id(obj_ids[oid])
        obj_ids[oid] = obj
        for dep in _dependencies(obj):
            did = obj_id(dep)
            assert did not in obj_ids or id(dep) == id(obj_ids[did])
            obj_ids[did] = dep
            adj.add_edge(did, oid)
    return (obj_ids, adj)


def _detect_cycles(p: Tuple[Dict[int, Err.SourceNode], _AdjM]):
    # attempt topsort, destroys adj
    nodes, adj = p
    node = next(adj.unconstrained, None)
    while node:
        adj.remove_node(node)
        node = next(adj.unconstrained, None)
    node = next(adj.nodes, None)
    if node:
        raise Err.CircularDependencies(nodes[node])


def _import_structs(doc: Document):
    # Add imported structs to doc.struct_typedefs, with collision checks
    for imp in [
        imp for imp in doc.imports if imp.doc
    ]:  # imp.doc should be None only for certain legacy unit tests
        imported_structs = {}
        for stb in imp.doc.struct_typedefs:
            assert isinstance(stb, Env.Binding) and isinstance(stb.rhs, StructTypeDef)
            imported_structs[stb.name] = stb.rhs
        for (name, alias) in imp.aliases:
            if name not in imported_structs:
                raise Err.NoSuchMember(imp.pos, name)
            if alias in imported_structs:
                raise Err.MultipleDefinitions(
                    imp.pos,
                    "struct type alias {} collides with another struct type in the imported document".format(
                        alias
                    ),
                )
            try:
                existing = Env.resolve(doc.struct_typedefs, [], alias)
                raise Err.MultipleDefinitions(
                    imp.pos,
                    "struct type alias {} collides with a struct {} document".format(
                        alias,
                        (
                            "type/alias from another imported"
                            if existing.imported
                            else "type in this"
                        ),
                    ),
                )
            except KeyError:
                pass
            if alias != name:
                imported_structs[alias] = imported_structs[name]
                del imported_structs[name]
        for (name, st) in imported_structs.items():
            existing = None
            try:
                existing = Env.resolve(doc.struct_typedefs, [], name)
                if existing.type_id != st.type_id:
                    raise Err.MultipleDefinitions(
                        imp.pos,
                        "imported struct {} must be aliased because it collides with a struct {} document".format(
                            name,
                            (
                                "type/alias from another imported"
                                if existing.imported
                                else "type in this"
                            ),
                        ),
                    )
            except KeyError:
                pass
            if not existing:
                st2 = StructTypeDef(imp.pos, name, st.members, imported=True)
                doc.struct_typedefs = Env.bind(doc.struct_typedefs, [], name, st2)


def _resolve_struct_typedef(
    pos: Err.SourcePosition, ty: T.StructInstance, struct_typedefs: Env.StructTypeDefs
):
    # On construction, WDL.Type.StructInstance is not yet resolved to the
    # struct type definition. Here, given the Env.StructTypeDefs computed
    # on document construction, we populate 'members' with the dict of member
    # types and names.
    try:
        struct_typedef = Env.resolve(struct_typedefs, [], ty.type_name)
    except KeyError:
        raise Err.InvalidType(pos, "Unknown type " + ty.type_name)
    ty.members = struct_typedef.members


def _resolve_struct_typedefs(
    pos: Err.SourcePosition,
    ty: T.Base,
    struct_typedefs: Env.StructTypeDefs,
    members_dict_ids: Optional[List[int]] = None,
):
    members_dict_ids = members_dict_ids or []
    # resolve all StructInstance within a potentially compound type
    if isinstance(ty, T.StructInstance):
        _resolve_struct_typedef(pos, ty, struct_typedefs)
        if id(ty.members) in members_dict_ids:
            # circular struct types!
            raise StopIteration
        members_dict_ids = [id(ty.members)] + (members_dict_ids or [])
    for p in ty.parameters:
        _resolve_struct_typedefs(pos, p, struct_typedefs, members_dict_ids=members_dict_ids)


def _initialize_struct_typedefs(struct_typedefs: Env.StructTypeDefs):
    # bootstrap struct typechecking: resolve all StructInstance members of the
    # struct types; also detect & error circular struct definitions
    for b in struct_typedefs:
        assert isinstance(b, Env.Binding)
        for member_ty in b.rhs.members.values():
            try:
                _resolve_struct_typedefs(b.rhs.pos, member_ty, struct_typedefs)
            except StopIteration:
                raise Err.CircularDependencies(b.rhs)


def _add_struct_instance_to_type_env(
    namespace: List[str], ty: T.StructInstance, type_env: Env.Types, ctx: Any
):
    # populate the type env with a binding for the struct instance and a
    # namespace containing its members (recursing if any members are themselves
    # struct instances)
    assert isinstance(ty.members, dict)
    ans = Env.bind(type_env, namespace[:-1], namespace[-1], ty, ctx)
    for member_name, member_type in ty.members.items():
        if isinstance(member_type, T.StructInstance):
            ans = _add_struct_instance_to_type_env(namespace + [member_name], member_type, ans, ctx)
        else:
            ans = Env.bind(ans, namespace, member_name, member_type, ctx=ctx)
    return ans
