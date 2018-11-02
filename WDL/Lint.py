"""
Linting: annotate WDL AST with hygiene warning
"""
import WDL
from typing import Any, Optional

class Linter(WDL.Walker.Base):
    """
    Linters are Walkers which annotate each tree node with
        ``lint : List[Tuple[SourcePosition,str,str]]``
    providing lint warnings with a position (possibly more-specific than the
    node's), short codename, and message.
    """

    def add(self, obj : WDL.SourceNode, message : str, pos : Optional[WDL.SourcePosition] = None):
        if pos is None:
            pos = obj.pos
        if not hasattr(obj, 'lint'):
            obj.lint = []
        obj.lint.append((pos, self.__class__.__name__, message))

class ImpliedStringCoercion(Linter):
    def decl(self, obj : WDL.Decl) -> Any:
        if isinstance(obj.type, WDL.Type.String) \
            and obj.expr is not None \
            and not isinstance(obj.expr.type, WDL.Type.String) \
            and not isinstance(obj.expr.type, WDL.Type.File):
            self.add(obj, "String {} = <{}>".format(obj.name, str(obj.expr.type)))
        # TODO: recurse into obj.expr to find coercions in function applications
