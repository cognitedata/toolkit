"""Visual tests for diff_table — run with `pytest -s` to see rendered output."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from rich.console import Console

from cognite_toolkit._cdf_tk.ui import ToolkitPanel, diff_table
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump

_FUNCTION_YAML = (
    Path(__file__).resolve().parents[5] / "cognite_toolkit/modules/operator/functions/toolkit.Function.yaml"
)


def _load() -> dict:
    return yaml.safe_load(_FUNCTION_YAML.read_text())


def _render(title: str, old: dict, new: dict, console: Console) -> None:
    old_lines = yaml_safe_dump(old).splitlines()
    new_lines = yaml_safe_dump(new).splitlines()
    console.print(ToolkitPanel(diff_table(old_lines, new_lines), title=title))


@pytest.fixture
def console() -> Console:
    return Console(width=120)


class TestDiffTableVariations:
    def test_field_value_changed(self, console: Console) -> None:
        local = _load()
        cdf = {**local, "runtime": "py311", "cpu": 1.0}
        _render("Field value changed (runtime + cpu)", local, cdf, console)

    def test_field_added_locally(self, console: Console) -> None:
        local = _load()
        cdf = {k: v for k, v in local.items() if k not in {"envVars", "memory"}}
        _render("Fields added locally (envVars + memory missing in CDF)", local, cdf, console)

    def test_field_removed_locally(self, console: Console) -> None:
        local = {k: v for k, v in _load().items() if k != "description"}
        cdf = _load()
        _render("Field removed locally (description in CDF only)", local, cdf, console)

    def test_minimal_cdf_state(self, console: Console) -> None:
        local = _load()
        cdf = {"externalId": local["externalId"], "status": "Failed"}
        _render("Minimal CDF state (failed function, full local definition)", local, cdf, console)

    def test_no_diff(self, console: Console) -> None:
        resource = _load()
        _render("No diff (identical)", resource, resource, console)
