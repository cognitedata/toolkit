"""
Approval test takes a snapshot of the results and then compare them to last run, ref https://approvaltests.com/,
and fails if they have changed.

If the changes are desired, you can update the snapshot by running `pytest --force-regen`.
"""
from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import typer
from cognite.client import CogniteClient
from pytest import MonkeyPatch

from cognite_toolkit.cdf import Common, build, clean, deploy
from cognite_toolkit.cdf_tk.templates import TMPL_DIRS, read_yaml_files
from cognite_toolkit.cdf_tk.utils import CDFToolConfig

REPO_ROOT = Path(__file__).parent.parent

SNAPSHOTS_DIR = REPO_ROOT / "tests" / "test_approval_modules_snapshots"
SNAPSHOTS_DIR.mkdir(exist_ok=True)
SNAPSHOTS_DIR_CLEAN = REPO_ROOT / "tests" / "test_approval_modules_snapshots_clean"
SNAPSHOTS_DIR_CLEAN.mkdir(exist_ok=True)


@contextlib.contextmanager
def chdir(new_dir: Path) -> Iterator[None]:
    """
    Change directory to new_dir and return to the original directory when exiting the context.

    Args:
        new_dir: The new directory to change to.

    """
    current_working_dir = Path.cwd()
    os.chdir(new_dir)

    try:
        yield

    finally:
        os.chdir(current_working_dir)


def find_all_modules() -> Iterator[Path]:
    for tmpl_dir in TMPL_DIRS:
        for module in (REPO_ROOT / f"./cognite_toolkit/{tmpl_dir}").iterdir():
            if module.is_dir():
                yield pytest.param(module, id=f"{module.parent.name}/{module.name}")


@pytest.fixture
def local_tmp_path():
    return SNAPSHOTS_DIR.parent / "tmp"


@pytest.fixture
def cdf_tool_config(cognite_client_approval: CogniteClient, monkeypatch: MonkeyPatch) -> CDFToolConfig:
    monkeypatch.setenv("CDF_PROJECT", "pytest-project")
    monkeypatch.setenv("IDP_TOKEN_URL", "dummy")
    monkeypatch.setenv("IDP_CLIENT_ID", "dummy")
    monkeypatch.setenv("IDP_CLIENT_SECRET", "dummy")

    with chdir(REPO_ROOT):
        # Build must always be executed from root of the project
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval
        cdf_tool.verify_capabilities.return_value = cognite_client_approval
        cdf_tool.failed = False

        cdf_tool.verify_dataset.return_value = 42
        cdf_tool.data_set_id = 999
        yield cdf_tool


@pytest.fixture
def typer_context(cdf_tool_config: CDFToolConfig) -> typer.Context:
    context = MagicMock(spec=typer.Context)
    context.obj = Common(
        verbose=False,
        override_env=True,
        cluster="pytest",
        project="pytest-project",
        mockToolGlobals=cdf_tool_config,
    )
    return context


def mock_read_yaml_files(module_path: Path, monkeypatch: MonkeyPatch) -> None:
    def fake_read_yaml_files(
        yaml_dirs: list[str],
        name: str | None = None,
    ) -> dict[str, Any]:
        if name == "local.yaml":
            return {"test": {"project": "pytest-project", "type": "dev", "deploy": [module_path.name]}}
        return read_yaml_files(yaml_dirs, name)

    monkeypatch.setattr("cognite_toolkit.cdf_tk.templates.read_yaml_files", fake_read_yaml_files)


@pytest.mark.parametrize("module_path", list(find_all_modules()))
def test_deploy_module_approval(
    module_path: Path,
    local_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    cognite_client_approval: CogniteClient,
    cdf_tool_config: CDFToolConfig,
    typer_context: typer.Context,
    data_regression,
) -> None:
    mock_read_yaml_files(module_path, monkeypatch)
    build(
        typer_context,
        source_dir="./cognite_toolkit",
        build_dir=str(local_tmp_path),
        build_env="test",
        clean=True,
    )
    deploy(
        typer_context,
        build_dir=str(local_tmp_path),
        build_env="test",
        interactive=False,
        drop=True,
        dry_run=False,
        include=[],
    )

    dump = cognite_client_approval.dump()
    data_regression.check(dump, fullpath=SNAPSHOTS_DIR / f"{module_path.name}.yaml")


@pytest.mark.parametrize("module_path", list(find_all_modules()))
def test_clean_module_approval(
    module_path: Path,
    local_tmp_path: Path,
    monkeypatch: MonkeyPatch,
    cognite_client_approval: CogniteClient,
    cdf_tool_config: CDFToolConfig,
    typer_context: typer.Context,
    data_regression,
) -> None:
    mock_read_yaml_files(module_path, monkeypatch)
    build(
        typer_context,
        source_dir="./cognite_toolkit",
        build_dir=str(local_tmp_path),
        build_env="test",
        clean=True,
    )
    clean(
        typer_context,
        build_dir=str(local_tmp_path),
        build_env="test",
        interactive=False,
        dry_run=False,
        include=[],
    )

    dump = cognite_client_approval.dump()
    data_regression.check(dump, fullpath=SNAPSHOTS_DIR_CLEAN / f"{module_path.name}.yaml")
