# minimal pyre typing stubs for lark

from typing import Any

class Transformer:
    def transform(self,tree) -> Any:
        ...

class Tree:
    ...

class Lark:
    def __init__(self,grammar,start=None,parser=None,propagate_positions=None):
        ...
    def parse(self,str) -> Tree:
        ...
