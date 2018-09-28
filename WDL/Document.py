# pyre-strict
"""
WDL Documents, encompassing Tasks and Workflows

"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Callable, TypeVar, Tuple, Union
import WDL.Type as T
import WDL.Value as V
import WDL.Expr as E
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

    def typecheck(self, type_env : Optional[E.TypeEnv] = None) -> None:
        """Infer and check types on all declarations and the command, including any expression placeholders within the command"""
        if type_env is None:
             type_env = E.TypeEnv()
        for decl in (self.inputs+self.postinputs):
            type_env = _typecheck_decl(decl, type_env)
        self.command.infer_type(type_env).typecheck(T.String())
        for decl in self.outputs:
            type_env = _typecheck_decl(decl, type_env)
        # TODO: check runtime section

def _typecheck_decl(decl : Decl, type_env : E.TypeEnv) -> E.TypeEnv:
    if decl.expr is not None:
        decl.expr.infer_type(type_env).typecheck(decl.type)
    return E.TypeEnv((decl.name, decl.type), type_env)
