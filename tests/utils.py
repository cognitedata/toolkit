from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit.cdf_tk.templates import read_yaml_file


def mock_read_yaml_file(file_content_by_name: dict[str, dict | list], monkeypatch: MonkeyPatch) -> None:
    def fake_read_yaml_file(
        filepath: Path, expected_output: Literal["list", "dict"] = "dict"
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if file_content := file_content_by_name.get(filepath.name):
            return file_content
        return read_yaml_file(filepath, expected_output)

    monkeypatch.setattr("cognite_toolkit.cdf_tk.templates.read_yaml_file", fake_read_yaml_file)
    monkeypatch.setattr("cognite_toolkit.cdf.read_yaml_file", fake_read_yaml_file)
