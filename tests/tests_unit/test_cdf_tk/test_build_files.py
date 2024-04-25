from __future__ import annotations

from pathlib import Path

import typer
from pytest import MonkeyPatch

from cognite_toolkit._cdf import build
from tests.tests_unit.utils import mock_read_yaml_file


def mock_environments_yaml_file(module_name: str, monkeypatch: MonkeyPatch) -> None:
    return mock_read_yaml_file(
        {
            "config.dev.yaml": {
                "environment": {
                    "name": "dev",
                    "project": "pytest-project",
                    "type": "dev",
                    "selected_modules_and_packages": [module_name],
                }
            }
        },
        monkeypatch,
        modify=True,
    )


def test_files_templates(
    local_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    typer_context: typer.Context,
    init_project: Path,
) -> None:
    mock_environments_yaml_file("cdf_oid_example_data", monkeypatch)

    build(
        typer_context,
        source_dir=str(init_project),
        build_dir=str(local_tmp_path / "files_build"),
        build_name="dev",
        no_clean=False,
    )

    assert Path(local_tmp_path / "files_build").exists()
    assert (local_tmp_path / "files_build" / "files").is_dir()
    assert (local_tmp_path / "files_build" / "files" / "PH-ME-P-0003-001.pdf").is_file()
