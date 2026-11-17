import ast
from datetime import datetime
from pathlib import Path
from typing import get_type_hints

from relay_runtime.typing import HandlerContext

# The public HandlerContext contract is pinned against the bootstrap module
# that defines the class. The class lives in the embedded _relay_context module
# beside the bootstrap (both ship under /relay), so the source of truth for the
# fields and method is that file, now that the bootstrap is split.
CONTEXT = (
    Path(__file__).resolve().parents[3]
    / "internal/runtime/python/_relay_context.py"
)
CONTEXT_TYPES = {
    "str": str,
    "int": int,
    "datetime": datetime,
    "str | None": str | None,
}


def _context_class() -> ast.ClassDef:
    assert CONTEXT.is_file(), f"missing Relay context module: {CONTEXT}"
    tree = ast.parse(CONTEXT.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "HandlerContext":
            return node
    raise AssertionError("_relay_context.py no longer defines HandlerContext")


def _context_fields() -> dict[str, str]:
    # Relay's context module is parsed, never imported: importing it would be
    # harmless, but parsing is the same mechanism the runtime uses on its own
    # embedded sources and keeps the contract independent of any installation.
    cls = _context_class()
    return {
        stmt.target.id: ast.unparse(stmt.annotation)
        for stmt in cls.body
        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)
    }


def test_bootstrap_fields_match_the_protocol() -> None:
    # given the concrete per-invocation context Relay's runtime module declares
    fields = _context_fields()
    # then the public Protocol documents exactly those fields and types
    properties = {
        name: get_type_hints(getattr(HandlerContext, name).fget)["return"]
        for name in fields
    }
    expected = {name: CONTEXT_TYPES[type_name] for name, type_name in fields.items()}
    assert expected == properties


def test_bootstrap_method_matches_the_protocol() -> None:
    # given the methods on the runtime's HandlerContext
    cls = _context_class()
    methods = [node.name for node in cls.body if isinstance(node, ast.FunctionDef)]
    # then the Protocol declares the same single method with an int return
    assert methods == ["get_remaining_time_ms"]
    annotations = get_type_hints(HandlerContext.get_remaining_time_ms)
    assert annotations == {"return": int}
