import ast
from datetime import datetime
from pathlib import Path
from typing import get_type_hints

from relay_runtime.typing import HandlerContext

BOOTSTRAP = Path(__file__).resolve().parents[3] / "internal/runtime/python/bootstrap.py"
BOOTSTRAP_TYPES = {
    "str": str,
    "int": int,
    "datetime": datetime,
    "str | None": str | None,
}


def _bootstrap_context() -> ast.ClassDef:
    assert BOOTSTRAP.is_file(), f"missing Relay bootstrap: {BOOTSTRAP}"
    tree = ast.parse(BOOTSTRAP.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "HandlerContext":
            return node
    raise AssertionError("bootstrap.py no longer defines HandlerContext")


def _bootstrap_fields() -> dict[str, str]:
    # Relay's bootstrap is parsed, never imported: importing it would mutate
    # sys.path and the process's stdout settings.
    cls = _bootstrap_context()
    return {
        stmt.target.id: ast.unparse(stmt.annotation)
        for stmt in cls.body
        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)
    }


def test_bootstrap_fields_match_the_protocol() -> None:
    # given the concrete per-invocation context Relay's bootstrap declares
    fields = _bootstrap_fields()
    # then the public Protocol documents exactly those fields and types
    properties = {
        name: get_type_hints(getattr(HandlerContext, name).fget)["return"]
        for name in fields
    }
    expected = {name: BOOTSTRAP_TYPES[type_name] for name, type_name in fields.items()}
    assert expected == properties


def test_bootstrap_method_matches_the_protocol() -> None:
    # given the methods on the bootstrap's HandlerContext
    cls = _bootstrap_context()
    methods = [node.name for node in cls.body if isinstance(node, ast.FunctionDef)]
    # then the Protocol declares the same single method with an int return
    assert methods == ["get_remaining_time_ms"]
    annotations = get_type_hints(HandlerContext.get_remaining_time_ms)
    assert annotations == {"return": int}
