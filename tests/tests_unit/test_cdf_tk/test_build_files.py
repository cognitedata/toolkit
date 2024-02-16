from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
import typer
from pytest import MonkeyPatch

from cognite_toolkit.cdf import build
from cognite_toolkit.cdf_tk.templates import COGNITE_MODULES, iterate_modules
from tests.constants import REPO_ROOT
from tests.tests_unit.utils import mock_read_yaml_file


def find_all_modules() -> Iterator[Path]:
    for module, _ in iterate_modules(REPO_ROOT / "cognite_toolkit" / COGNITE_MODULES):
        yield pytest.param(module, id=f"{module.parent.name}/{module.name}")


def mock_environments_yaml_file(module_path: Path, monkeypatch: MonkeyPatch) -> None:
    return mock_read_yaml_file(
        {
            "config.dev.yaml": {
                "environment": {
                    "name": "dev",
                    "project": "pytest-project",
                    "type": "dev",
                    "selected_modules_and_packages": ["cdf_oid_example_data"],
                    "common_function_code": Path(REPO_ROOT / "cognite_toolkit/common_function_code").as_posix(),
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
    mock_environments_yaml_file(REPO_ROOT / "cognite_toolkit/cognite_modules", monkeypatch)

    build(
        typer_context,
        source_dir=str(init_project),
        build_dir=str(local_tmp_path / "files_build"),
        build_env="dev",
        clean=True,
    )

    assert Path(local_tmp_path / "files_build").exists()
    assert (local_tmp_path / "files_build" / "files").is_dir()
    assert (local_tmp_path / "files_build" / "files" / "PH-ME-P-0003-001.pdf").is_file()
