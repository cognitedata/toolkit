from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit.cdf_tk.utils import load_yaml_inject_variables, read_yaml_file


def mock_read_yaml_file(
    file_content_by_name: dict[str, dict | list], monkeypatch: MonkeyPatch, modify: bool = False
) -> None:
    def fake_read_yaml_file(
        filepath: Path, expected_output: Literal["list", "dict"] = "dict"
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if file_content := file_content_by_name.get(filepath.name):
            if modify:
                source = read_yaml_file(filepath, expected_output)
                file_content = source.update(file_content)
            return file_content
        return read_yaml_file(filepath, expected_output)

    def fake_load_yaml_inject_variables(
        filepath: Path, variables: dict[str, str | None], required_return_type: Literal["any", "list", "dict"] = "any"
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if file_content := file_content_by_name.get(filepath.name):
            if modify:
                source = load_yaml_inject_variables(filepath, variables, required_return_type)
                file_content = source.update(file_content)
            return file_content
        return load_yaml_inject_variables(filepath, variables, required_return_type)

    monkeypatch.setattr("cognite_toolkit.cdf_tk.utils.read_yaml_file", fake_read_yaml_file)
    monkeypatch.setattr("cognite_toolkit.cdf.read_yaml_file", fake_read_yaml_file)
    monkeypatch.setattr("cognite_toolkit.cdf_tk.utils.load_yaml_inject_variables", fake_load_yaml_inject_variables)
    monkeypatch.setattr(
        "cognite_toolkit.cdf_tk.load._base_loaders.load_yaml_inject_variables", fake_load_yaml_inject_variables
    )
    monkeypatch.setattr(
        "cognite_toolkit.cdf_tk.load._resource_loaders.load_yaml_inject_variables", fake_load_yaml_inject_variables
    )
