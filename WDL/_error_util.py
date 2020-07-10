from typing import NamedTuple


class SourcePosition(
    NamedTuple(
        "SourcePosition",
        [
            ("uri", str),
            ("abspath", str),
            ("line", int),
            ("column", int),
            ("end_line", int),
            ("end_column", int),
        ],
    )
):
    """
    Source position attached to AST nodes and exceptions; NamedTuple of ``uri`` the filename/URI
    passed to :func:`WDL.load` or a WDL import statement, which may be relative; ``abspath`` the
    absolute filename/URI; and one-based int positions ``line`` ``end_line`` ``column``
    ``end_column``
    """
