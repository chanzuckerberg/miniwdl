# minimal pyre typing stubs for lark

from typing import Any

class Token:
    value: str
    line: int
    end_line: int
    column: int
    end_column: int
    ...

class Transformer:
    def transform(self,tree) -> Any:
        """
        Transform a transformation of the given tree.

        Args:
            self: (array): write your description
            tree: (array): write your description
        """
        ...

class Tree:
    ...

class Lark:
    def __init__(self,grammar,start=None,parser=None,propagate_positions=None):
        """
        Initialize the parser.

        Args:
            self: (todo): write your description
            grammar: (todo): write your description
            start: (int): write your description
            parser: (todo): write your description
            propagate_positions: (int): write your description
        """
        ...
    def parse(self,str) -> Tree:
        """
        Parse the given tree.

        Args:
            self: (str): write your description
            str: (str): write your description
        """
        ...
