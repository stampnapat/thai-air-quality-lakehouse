"""Load only the definitions from a Databricks source notebook.

Notebooks mix definitions (imports, CONSTANTS, functions) with code that
runs the pipeline (fetching the API, starting streams, writing tables).
Tests need the definitions without the side effects, so this loader keeps:
  - import statements
  - assignments to UPPER_CASE names (constants)
  - function and class definitions
and drops everything else.
"""
from __future__ import annotations

import ast
from pathlib import Path

NOTEBOOK_DIR = Path(__file__).resolve().parents[1] / "src" / "notebooks"


class _FakeWidgets:
    """Stands in for dbutils.widgets: every widget returns the test catalog."""

    def get(self, name: str) -> str:
        return {"catalog": "air_quality_test"}.get(name, "")


class _FakeDbutils:
    widgets = _FakeWidgets()


def _is_constant(node: ast.stmt) -> bool:
    if isinstance(node, ast.Assign):
        targets = node.targets
    elif isinstance(node, ast.AnnAssign):
        targets = [node.target]
    else:
        return False
    return all(isinstance(t, ast.Name) and t.id.isupper() for t in targets)


def load_definitions(notebook: str, **overrides) -> dict:
    """Return a namespace with the notebook's definitions.

    `overrides` replace constants after loading (e.g. LANDING_DIR=tmp_path).
    Functions read globals from this namespace, so overrides take effect.
    """
    source = (NOTEBOOK_DIR / f"{notebook}.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    keep = (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.ClassDef)
    tree.body = [n for n in tree.body if isinstance(n, keep) or _is_constant(n)]
    namespace: dict = {"__name__": f"notebook_{notebook}", "dbutils": _FakeDbutils()}
    exec(compile(tree, notebook, "exec"), namespace)
    namespace.update(overrides)
    return namespace
