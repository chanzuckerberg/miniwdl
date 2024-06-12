# mypy: ignore-errors

# minimal pyre typing stubs for lark

from typing import Any, Callable, Optional, Dict
from . import exceptions

class Token:
    value: str
    line: int
    end_line: int
    column: int
    end_column: int
    ...

class Transformer:
    def transform(self,tree) -> Any:
        ...

class Tree:
    ...

class Lark:
    def __init__(self,grammar,start=None,parser=None,propagate_positions=None,lexer_callbacks=Dict[str,Callable],maybe_placeholders=False):
        ...
    def parse(self,str) -> Tree:
        ...

def v_args(inline: bool = False, meta: bool = False, tree: bool = False, wrapper: Optional[Callable] = None) -> Callable:
    ...
