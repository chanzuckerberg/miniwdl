"""
Linting: annotate WDL AST with hygiene warning
"""
import WDL
from typing import Any, Optional

class Linter(WDL.Walker.Base):
    """
    Linters are Walkers which annotate each tree node with
        ``lint : List[Tuple[SourceNode,str,str]]``
    providing lint warnings with a node (possibly more-specific than the
    node it's attached to), short codename, and message.
    """

    def add(self, obj : WDL.SourceNode, message : str, subnode : Optional[WDL.SourceNode] = None):
        if not hasattr(obj, 'lint'):
            obj.lint = []
        obj.lint.append((subnode or obj, self.__class__.__name__, message))

class ImpliedStringCoercion(Linter):
    def decl(self, obj : WDL.Decl) -> Any:
        if isinstance(obj.type, WDL.Type.String) \
            and obj.expr is not None \
            and not isinstance(obj.expr.type, WDL.Type.String) \
            and not isinstance(obj.expr.type, WDL.Type.File):
            self.add(obj, "String {} = <{}>".format(obj.name, str(obj.expr.type)))
        # TODO: recurse into obj.expr to find coercions in function applications
