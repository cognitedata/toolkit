from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import typer
from cognite.client.testing import monkeypatch_cognite_client
from pytest import MonkeyPatch

from cognite_toolkit.cdf import Common, main_init
from cognite_toolkit.cdf_tk.utils import CDFToolConfig
from tests.approval_client import ApprovalCogniteClient

THIS_FOLDER = Path(__file__).resolve().parent
REPO_ROOT = THIS_FOLDER.parent


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


@pytest.fixture
def cognite_client_approval() -> ApprovalCogniteClient:
    with monkeypatch_cognite_client() as client:
        approval_client = ApprovalCogniteClient(client)
        yield approval_client


@pytest.fixture
def local_tmp_path() -> Path:
    return THIS_FOLDER / "tmp"


@pytest.fixture(scope="session")
def local_tmp_project_path() -> Path:
    project_path = THIS_FOLDER / "pytest-project"
    project_path.mkdir(exist_ok=True)
    return project_path


@pytest.fixture
def cdf_tool_config(cognite_client_approval: ApprovalCogniteClient, monkeypatch: MonkeyPatch) -> CDFToolConfig:
    monkeypatch.setenv("CDF_PROJECT", "pytest-project")
    monkeypatch.setenv("CDF_CLUSTER", "bluefield")
    monkeypatch.setenv("IDP_TOKEN_URL", "dummy")
    monkeypatch.setenv("IDP_CLIENT_ID", "dummy")
    monkeypatch.setenv("IDP_CLIENT_SECRET", "dummy")

    with chdir(REPO_ROOT):
        real_config = CDFToolConfig(cluster="bluefield", project="pytest-project")
        # Build must always be executed from root of the project
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
        cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
        cdf_tool.failed = False
        cdf_tool.environment_variables.side_effect = real_config.environment_variables
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


@pytest.fixture
def init_project(typer_context: typer.Context, local_tmp_project_path: Path) -> Path:
    main_init(
        typer_context,
        dry_run=False,
        upgrade=False,
        git=None,
        init_dir=str(local_tmp_project_path),
        no_backup=True,
        clean=True,
    )
    return local_tmp_project_path
