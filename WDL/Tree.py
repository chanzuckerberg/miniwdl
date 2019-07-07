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
from .Error import SourcePosition, SourceNode
from . import Type, Expr, Env, Error, StdLib, _parser, _util


class StructTypeDef(SourceNode):
    """WDL struct type definition"""

    name: str
    """
    :type: str

    Name of the struct type (in the current document)
    """

    members: Dict[str, Type.Base]
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
        self, pos: SourcePosition, name: str, members: Dict[str, Type.Base], imported: bool = False
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
        return Type._struct_type_id(self.members)


class Decl(SourceNode):
    """A value declaration within a task or workflow"""

    type: Type.Base
    ":type: WDL.Type.Base"
    name: str
    """Declared value name

    :type: str"""
    expr: Optional[Expr.Base]
    """:type: Optional[WDL.Expr.Base]

    Bound expression, if any"""

    def __init__(
        self, pos: SourcePosition, type: Type.Base, name: str, expr: Optional[Expr.Base] = None
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
        self, struct_typedefs: Env.StructTypeDefs, type_env: Env.Types, collision_ok: bool = False
    ) -> Env.Types:
        # Add an appropriate binding in the type env, after checking for name
        # collision.
        if not collision_ok:
            try:
                Env.resolve(type_env, [], self.name)
                raise Error.MultipleDefinitions(self, "Multiple declarations of " + self.name)
            except KeyError:
                pass
            try:
                Env.resolve_namespace(type_env, [self.name])
                raise Error.MultipleDefinitions(self, "Value/call name collision on " + self.name)
            except KeyError:
                pass
        _resolve_struct_typedefs(self.pos, self.type, struct_typedefs)
        if isinstance(self.type, Type.StructInstance):
            return _add_struct_instance_to_type_env([self.name], self.type, type_env, ctx=self)
        return Env.bind(type_env, [], self.name, self.type, ctx=self)

    def typecheck(
        self, type_env: Env.Types, stdlib: Optional[StdLib.Base] = None, check_quant: bool = True
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
    command: Expr.String
    ":type: WDL.Expr.String"
    outputs: List[Decl]
    """:type: List[WDL.Tree.Decl]

    Output declarations"""
    parameter_meta: Dict[str, Any]
    """:type: Dict[str,Any]

    ``parameter_meta{}`` section as a JSON-like dict"""
    runtime: Dict[str, Expr.Base]
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
        command: Expr.String,
        outputs: List[Decl],
        parameter_meta: Dict[str, Any],
        runtime: Dict[str, Expr.Base],
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
                    raise Error.StrayInputDeclaration(
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

        with Error.multi_context() as errors:
            # Pass through input & postinput declarations again, typecheck their
            # right-hand side expressions against the type environment.
            for decl in (self.inputs or []) + self.postinputs:
                errors.try1(lambda: decl.typecheck(type_env, check_quant=check_quant))
            # Typecheck the command (string)
            errors.try1(
                lambda: self.command.infer_type(type_env, check_quant=check_quant).typecheck(
                    Type.String()
                )
            )
            # Typecheck runtime expressions
            for _, runtime_expr in self.runtime.items():
                errors.try1(
                    lambda runtime_expr=runtime_expr: runtime_expr.infer_type(
                        type_env, check_quant=check_quant
                    ).typecheck(Type.String())
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
    inputs: Dict[str, Expr.Base]
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
        inputs: Dict[str, Expr.Base],
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

    def resolve(self, doc: TVDocument, call_names: Optional[Set[str]] = None) -> None:
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
            wf = callee_doc.workflow
            if isinstance(wf, Workflow) and wf.name == self.callee_id[-1]:
                if not wf.complete_calls or (wf.outputs is None and wf.effective_outputs):
                    raise Error.UncallableWorkflow(self, ".".join(self.callee_id))
                self.callee = wf
            else:
                for task in callee_doc.tasks:
                    if task.name == self.callee_id[-1]:
                        self.callee = task
        if self.callee is None:
            raise Error.NoSuchTask(self, ".".join(self.callee_id))
        assert isinstance(self.callee, (Task, Workflow))
        # If given call_names, check for name collisions
        if call_names is not None:
            if self.name in call_names:
                raise Error.MultipleDefinitions(
                    self,
                    "Workflow has multiple calls named {}; give calls distinct names using `call {} as NAME ...`".format(
                        self.name, self.callee.name
                    ),
                )
            call_names.add(self.name)

    def add_to_type_env(
        self, struct_typedefs: Env.StructTypeDefs, type_env: Env.Types
    ) -> Env.Types:
        # Add the call's outputs to the type environment under the appropriate
        # namespace, after checking for namespace collisions.
        assert self.callee
        try:
            Env.resolve(type_env, [], self.name)
            raise Error.MultipleDefinitions(self, "Value/call name collision on " + self.name)
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
        with Error.multi_context() as errors:
            for name, expr in self.inputs.items():
                try:
                    decl = Env.resolve(self.callee.available_inputs, [], name)
                    errors.try1(
                        lambda expr=expr, decl=decl: expr.infer_type(
                            type_env, check_quant=check_quant
                        ).typecheck(decl.type)
                    )
                except KeyError:
                    errors.append(Error.NoSuchInput(expr, name))
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
    expr: Expr.Base
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
        expr: Expr.Base,
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
            inner_type_env = elt.add_to_type_env(struct_typedefs, inner_type_env)  # pyre-ignore
        # Subtlety: if the scatter array is statically nonempty, then so too
        # are the arrayized values.
        nonempty = isinstance(self.expr._type, Type.Array) and self.expr._type.nonempty
        inner_type_env = Env.map(inner_type_env, lambda ns, b: Type.Array(b.rhs, nonempty=nonempty))
        return inner_type_env + type_env

    @property
    def effective_outputs(self) -> Env.Types:
        # Yield the outputs of calls in this section and subsections, typed
        # and namespaced appropriately, as they'll be propagated if the
        # workflow lacks an explicit output{} section
        nonempty = isinstance(self.expr._type, Type.Array) and self.expr._type.nonempty
        ans: Env.Types = []
        for elt in self.elements:
            if not isinstance(elt, Decl):
                ans = elt.effective_outputs + ans
        ans = Env.map(ans, lambda ns, b: Type.Array(b.rhs, nonempty=nonempty))
        return ans


class Conditional(SourceNode):
    """A conditional (if) stanza within a workflow"""

    expr: Expr.Base
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
        expr: Expr.Base,
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
    _output_idents_pos: Optional[Error.SourcePosition]
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

        for elt in _decls_and_calls(self, exclude_outputs=True):
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

        for elt in _decls_and_calls(self, exclude_outputs=True):
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
        # 1. resolve all calls and check for call name collisions
        _resolve_calls(doc)
        # 2. build type environments in the workflow and each scatter &
        #    conditional section therein
        _build_workflow_type_env(doc, check_quant)
        with Error.multi_context() as errors:
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
                output_type_env = self._type_env
                assert output_type_env
                for output in self.outputs:
                    assert output.expr
                    if output.name in output_names:
                        errors.append(
                            Error.MultipleDefinitions(
                                output, "multiple workflow outputs named " + output.name
                            )
                        )
                    output_names.add(output.name)
                    # tricky sequence here: we need to call Decl.add_to_type_env to resolve
                    # potential struct type, but:
                    # 1. we don't want it to check for name collision in the usual way in order to
                    #    handle a quirk of draft-2 workflow output style, where an output may take
                    #    the name of another decl in the workflow. Instead we've tracked and
                    #    rejected any duplicate names among the workflow outputs.
                    # 2. we still want to typecheck the output expression againsnt the 'old' type
                    #    environment
                    output_type_env2 = output.add_to_type_env(
                        doc.struct_typedefs, output_type_env, collision_ok=True
                    )
                    errors.try1(
                        lambda output=output: output.typecheck(
                            output_type_env, check_quant=check_quant
                        )
                    )
                    output_type_env = output_type_env2
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
                    raise Error.NoSuchTask(
                        self._output_idents_pos, ".".join(wildcard_namespace)
                    ) from None

            for output_ident in output_idents:
                try:
                    ty = Env.resolve(self._type_env, output_ident[:-1], output_ident[-1])
                except KeyError:
                    raise Error.UnknownIdentifier(
                        Expr.Ident(self._output_idents_pos, output_ident)
                    ) from None
                assert isinstance(ty, Type.Base)
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
                        Expr.Ident(self._output_idents_pos, output_ident),
                    )
                )

        # put the synthetic declarations into self.outputs
        self.outputs = output_ident_decls + self.outputs  # pyre-fixme
        self._output_idents = []


DocImport = NamedTuple(
    "DocImport",
    [
        ("pos", Error.SourcePosition),
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
                raise Error.MultipleDefinitions(
                    self, "Multiple imports with namespace " + imp.namespace
                )
            names.add(imp.namespace)
        _import_structs(self)
        _initialize_struct_typedefs(self.struct_typedefs)
        names = set()
        # typecheck each task
        with Error.multi_context() as errors:
            for task in self.tasks:
                if task.name in names:
                    errors.append(
                        Error.MultipleDefinitions(task, "Multiple tasks named " + task.name)
                    )
                names.add(task.name)
                errors.try1(
                    lambda task=task: task.typecheck(self.struct_typedefs, check_quant=check_quant)
                )
        # typecheck the workflow
        if self.workflow:
            if self.workflow.name in names:
                raise Error.MultipleDefinitions(
                    self.workflow,
                    "Workflow name collides with a task also named " + self.workflow.name,
                )
            self.workflow.typecheck(self, check_quant=check_quant)


def load(
    uri: str,
    path: Optional[List[str]] = None,
    check_quant: bool = True,
    import_uri: Optional[Callable[[str], str]] = None,
    import_max_depth: int = 10,
    source_text: Optional[str] = None,
) -> Document:
    path = path or []
    if source_text is None:
        if uri.startswith("file://"):
            uri = uri[7:]
        elif uri.find("://") > 0 and import_uri:
            uri = import_uri(uri)
        # search cwd and path for an extant file
        fn = next(
            (
                fn
                for fn in ([uri] + [os.path.join(dn, uri) for dn in reversed(path)])
                if os.path.exists(fn)
            ),
            None,
        )
        if not fn:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), uri)
        # read the document source text
        with open(fn, "r") as infile:
            source_text = infile.read()
        path = path + [os.path.dirname(fn)]
    # parse the document
    doc = _parser.parse_document(source_text, uri=uri)
    assert isinstance(doc, Document)
    # recursively descend into document's imports, and store the imported
    # documents into doc.imports
    # TODO: are we supposed to do something smart for relative imports
    #       within a document loaded by URI?
    for i in range(len(doc.imports)):
        imp = doc.imports[i]
        if import_max_depth <= 1:
            raise Error.ImportError(
                imp.pos, imp.uri, "exceeded import_max_depth; circular imports?"
            )
        try:
            subdoc = load(
                imp.uri,
                path,
                check_quant=check_quant,
                import_uri=import_uri,
                import_max_depth=(import_max_depth - 1),
            )
        except Exception as exn:
            raise Error.ImportError(imp.pos, imp.uri) from exn
        doc.imports[i] = DocImport(
            pos=imp.pos, uri=imp.uri, namespace=imp.namespace, aliases=imp.aliases, doc=subdoc
        )
    try:
        doc.typecheck(check_quant=check_quant)
    except Error.ValidationError as exn:
        exn.source_text = source_text
        raise exn
    except Error.MultipleValidationErrors as multi:
        for exn in multi.exceptions:
            if not exn.source_text:
                exn.source_text = source_text
        raise multi
    return doc


#
# Typechecking helper functions
#


def _decls_and_calls(
    element: Union[Workflow, Scatter, Conditional], exclude_outputs: bool = True
) -> Generator[Union[Decl, Call], None, None]:
    # Yield each Decl and Call in the workflow, including those nested within
    # scatter/conditional sections
    children = element.children
    if isinstance(element, Workflow) and exclude_outputs:
        children = element.inputs if element.inputs else []
        children = children + element.elements
    for ch in children:
        if isinstance(ch, (Decl, Call)):
            yield ch
        elif isinstance(ch, (Scatter, Conditional)):
            for gch in _decls_and_calls(ch):
                yield gch


def _resolve_calls(doc: Document) -> None:
    # Resolve all calls in the workflow (descending into scatter & conditional
    # sections). Also check for call name collisions
    if doc.workflow:
        with Error.multi_context() as errors:
            call_names = set()
            for c in _decls_and_calls(doc.workflow):
                if isinstance(c, Call):
                    errors.try1(lambda c=c: c.resolve(doc, call_names))


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
        if not isinstance(self.expr.type, Type.Array):
            raise Error.NotAnArray(self.expr)
        if isinstance(self.expr.type.item_type, Type.Any):
            raise Error.IndeterminateType(self.expr, "can't infer item type of empty array")
        # bind the scatter variable to the array item type within the body
        try:
            Env.resolve(type_env, [], self.variable)
            raise Error.MultipleDefinitions(
                self, "Name collision for scatter variable " + self.variable
            )
        except KeyError:
            pass
        try:
            Env.resolve_namespace(type_env, [self.variable])
            raise Error.MultipleDefinitions(
                self, "Call name collision for scatter variable " + self.variable
            )
        except KeyError:
            pass
        type_env = Env.bind(type_env, [], self.variable, self.expr.type.item_type, ctx=self)
    elif isinstance(self, Conditional):
        # typecheck the condition
        self.expr.infer_type(type_env, check_quant=check_quant)
        if not self.expr.type.coerces(Type.Boolean()):
            raise Error.StaticTypeMismatch(self.expr, Type.Boolean(), self.expr.type)
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
                    # pyre-ignore
                    child_outer_type_env = sibling.add_to_type_env(
                        doc.struct_typedefs, child_outer_type_env
                    )
            _build_workflow_type_env(doc, check_quant, child, child_outer_type_env)
        elif doc.workflow.inputs is not None and isinstance(child, Decl) and not child.expr:
            raise Error.StrayInputDeclaration(
                self,
                "unbound declaration {} {} outside workflow input{} section".format(
                    str(child.type), child.name, "{}"
                ),
            )

    # finally, populate self._type_env with all our children
    for child in self.elements:
        type_env = child.add_to_type_env(doc.struct_typedefs, type_env)  # pyre-ignore
    self._type_env = type_env


def _typecheck_workflow_elements(
    doc: Document, check_quant: bool, self: Optional[Union[Workflow, Scatter, Conditional]] = None
) -> bool:
    # following _resolve_calls() and _build_workflow_type_env(), typecheck all
    # the declaration expressions and call inputs
    self = self or doc.workflow
    assert self and (self._type_env is not None)
    complete_calls = True
    with Error.multi_context() as errors:
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
        except Error.StaticTypeMismatch as exc:
            expected = exc.expected
            if isinstance(expected, Type.StructInstance):
                for stb in doc.struct_typedefs:
                    assert isinstance(stb, Env.Binding) and isinstance(stb.rhs, StructTypeDef)
                    if id(stb.rhs.members) == id(expected.members):
                        expected = Type.StructInstance(stb.name, optional=expected.optional)
                        expected.members = stb.rhs.members
            actual = exc.actual
            if isinstance(actual, Type.StructInstance):
                for stb in doc.struct_typedefs:
                    assert isinstance(stb, Env.Binding) and isinstance(stb.rhs, StructTypeDef)
                    if id(stb.rhs.members) == id(actual.members):
                        actual = Type.StructInstance(stb.name, optional=actual.optional)
                        actual.members = stb.rhs.members
            raise Error.StaticTypeMismatch(exc.node or exc.pos, expected, actual, exc.args[0])

    return f


def _dependencies(obj: Union[Decl, Call, Expr.Base]) -> Iterable[Union[Decl, Call]]:
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
    elif isinstance(obj, Expr.Ident):
        if isinstance(obj.referee, (Decl, Call)):
            yield obj.referee
        else:
            assert isinstance(obj.referee, Scatter)
    else:
        assert isinstance(obj, Expr.Base)
        for subexpr in obj.children:
            assert isinstance(subexpr, (Decl, Call, Expr.Base))
            for dep in _dependencies(subexpr):
                yield dep


def _dependency_matrix(
    objs: Iterable[Union[Decl, Call]],
    obj_id: Optional[Callable[[Error.SourceNode], int]] = None,
    exclusive: bool = False,
) -> Tuple[Dict[int, Union[Decl, Call]], _util.AdjM]:
    # Given collection of Decl & Call, produce mapping of object ids to the
    # objects and the adjacency matrix for their dependencies
    # obj_id: get unique int id for object, defaults to id()
    # exclusive: if True then exclude dependencies that aren't among objs to
    #            begin with.
    obj_id = obj_id or id
    objs_by_id = dict((obj_id(obj), obj) for obj in objs)
    adj = _util.AdjM()
    for oid in objs_by_id:
        adj.add_node(oid)
        for dep in _dependencies(objs_by_id[oid]):
            did = obj_id(dep)
            if objs_by_id.get(did, not exclusive):
                assert id(objs_by_id.get(did, dep)) == id(dep)
                objs_by_id[did] = dep
                adj.add_edge(did, oid)
    return (objs_by_id, adj)


def _detect_cycles(p: Tuple[Dict[int, SourceNode], _util.AdjM]) -> None:
    # given the result of _dependency_matrix, detect if there exists a cycle
    # and if so, then raise WDL.Error.CircularDependencies with a relevant
    # SourceNode.
    nodes, adj = p
    try:
        _util.topsort(adj)
    except StopIteration as err:
        raise Error.CircularDependencies(nodes[getattr(err, "node")])


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
                raise Error.NoSuchMember(imp.pos, name)
            if alias in imported_structs:
                raise Error.MultipleDefinitions(
                    imp.pos,
                    "struct type alias {} collides with another struct type in the imported document".format(
                        alias
                    ),
                )
            try:
                existing = Env.resolve(doc.struct_typedefs, [], alias)
                raise Error.MultipleDefinitions(
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
                    raise Error.MultipleDefinitions(
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
    pos: Error.SourcePosition, ty: Type.StructInstance, struct_typedefs: Env.StructTypeDefs
):
    # On construction, WDL.Type.StructInstance is not yet resolved to the
    # struct type definition. Here, given the Env.StructTypeDefs computed
    # on document construction, we populate 'members' with the dict of member
    # types and names.
    try:
        struct_typedef = Env.resolve(struct_typedefs, [], ty.type_name)
    except KeyError:
        raise Error.InvalidType(pos, "Unknown type " + ty.type_name) from None
    ty.members = struct_typedef.members


def _resolve_struct_typedefs(
    pos: Error.SourcePosition,
    ty: Type.Base,
    struct_typedefs: Env.StructTypeDefs,
    members_dict_ids: Optional[List[int]] = None,
):
    members_dict_ids = members_dict_ids or []
    # resolve all StructInstance within a potentially compound type
    if isinstance(ty, Type.StructInstance):
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
                raise Error.CircularDependencies(b.rhs) from None


def _add_struct_instance_to_type_env(
    namespace: List[str], ty: Type.StructInstance, type_env: Env.Types, ctx: Any
):
    # populate the type env with a binding for the struct instance and a
    # namespace containing its members (recursing if any members are themselves
    # struct instances)
    assert isinstance(ty.members, dict)
    ans = Env.bind(type_env, namespace[:-1], namespace[-1], ty, ctx)
    for member_name, member_type in ty.members.items():
        if isinstance(member_type, Type.StructInstance):
            ans = _add_struct_instance_to_type_env(namespace + [member_name], member_type, ans, ctx)
        else:
            ans = Env.bind(ans, namespace, member_name, member_type, ctx=ctx)
    return ans
