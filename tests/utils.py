from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit.cdf_tk.templates import read_yaml_file
from cognite_toolkit.cdf_tk.utils import load_yaml_inject_variables


def mock_read_yaml_file(file_content_by_name: dict[str, dict | list], monkeypatch: MonkeyPatch) -> None:
    def fake_read_yaml_file(
        filepath: Path, expected_output: Literal["list", "dict"] = "dict"
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if file_content := file_content_by_name.get(filepath.name):
            return file_content
        return read_yaml_file(filepath, expected_output)

    def fake_load_yaml_inject_variables(
        filepath: Path, variables: dict[str, str | None], required_return_type: Literal["any", "list", "dict"] = "any"
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if file_content := file_content_by_name.get(filepath.name):
            return file_content
        return load_yaml_inject_variables(filepath, variables, required_return_type)

    monkeypatch.setattr("cognite_toolkit.cdf_tk.templates.read_yaml_file", fake_read_yaml_file)
    monkeypatch.setattr("cognite_toolkit.cdf.read_yaml_file", fake_read_yaml_file)
    monkeypatch.setattr("cognite_toolkit.cdf_tk.utils.load_yaml_inject_variables", fake_load_yaml_inject_variables)
    monkeypatch.setattr(
        "cognite_toolkit.cdf_tk.load._resource_loaders.load_yaml_inject_variables", fake_load_yaml_inject_variables
    )
