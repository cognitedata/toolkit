from __future__ import annotations

import contextlib
import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import typer
from pytest import MonkeyPatch

from cognite_toolkit._cdf import Common, main_init
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.constants import REPO_ROOT
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.utils import PrintCapture

THIS_FOLDER = Path(__file__).resolve().parent
TMP_FOLDER = THIS_FOLDER / "tmp"
TMP_FOLDER.mkdir(exist_ok=True)


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
def toolkit_client_approval() -> ApprovalToolkitClient:
    with monkeypatch_toolkit_client() as client:
        approval_client = ApprovalToolkitClient(client)
        yield approval_client


@pytest.fixture(scope="session")
def build_tmp_path() -> Path:
    pidid = os.getpid()
    build_folder = TMP_FOLDER / f"build-{pidid}"

    if build_folder.exists():
        shutil.rmtree(build_folder, ignore_errors=True)
        build_folder.mkdir(exist_ok=True)
    yield build_folder
    shutil.rmtree(build_folder, ignore_errors=True)


@pytest.fixture(scope="session")
def local_tmp_project_path_immutable() -> Path:
    pidid = os.getpid()
    project_path = TMP_FOLDER / f"pytest-project-{pidid}"
    project_path.mkdir(exist_ok=True)
    yield project_path
    shutil.rmtree(project_path, ignore_errors=True)


@pytest.fixture
def local_tmp_project_path_mutable() -> Path:
    pidid = os.getpid()
    project_path = TMP_FOLDER / f"pytest-project-mutable-{pidid}"
    if project_path.exists():
        shutil.rmtree(project_path, ignore_errors=True)
    project_path.mkdir(exist_ok=True)
    yield project_path
    shutil.rmtree(project_path, ignore_errors=True)


@pytest.fixture
def cdf_tool_config(toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch) -> CDFToolConfig:
    environment_variables = {
        "LOGIN_FLOW": "client_credentials",
        "CDF_PROJECT": "pytest-project",
        "CDF_CLUSTER": "bluefield",
        "IDP_TOKEN_URL": "dummy",
        "IDP_CLIENT_ID": "dummy",
        "IDP_CLIENT_SECRET": "dummy",
        "IDP_TENANT_ID": "dummy",
        "IDP_AUDIENCE": "https://bluefield.cognitedata.com",
        "IDP_SCOPES": "https://bluefield.cognitedata.com/.default",
        "CDF_URL": "https://bluefield.cognitedata.com",
    }
    existing = {}
    for key, value in environment_variables.items():
        existing[key] = os.environ.get(key)
        os.environ[key] = value

    with chdir(REPO_ROOT):
        real_config = CDFToolConfig(cluster="bluefield", project="pytest-project")
        # Build must always be executed from root of the project
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client
        cdf_tool.client = toolkit_client_approval.mock_client
        cdf_tool.toolkit_client = toolkit_client_approval.mock_client

        cdf_tool.environment_variables.side_effect = real_config.environment_variables
        cdf_tool.verify_dataset.return_value = 42
        cdf_tool.data_set_id = 999
        yield cdf_tool

    for key, value in existing.items():
        if value is None:
            del os.environ[key]
        else:
            os.environ[key] = value


@pytest.fixture
def cdf_tool_config_real(toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch) -> CDFToolConfig:
    monkeypatch.setenv("CDF_PROJECT", "pytest-project")
    monkeypatch.setenv("CDF_CLUSTER", "bluefield")
    monkeypatch.setenv("IDP_TOKEN_URL", "dummy")
    monkeypatch.setenv("IDP_CLIENT_ID", "dummy")
    monkeypatch.setenv("IDP_CLIENT_SECRET", "dummy")

    return CDFToolConfig(cluster="bluefield", project="pytest-project")


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


@pytest.fixture(scope="session")
def typer_context_no_cdf_tool_config() -> typer.Context:
    context = MagicMock(spec=typer.Context)
    context.obj = Common(
        verbose=False, override_env=True, cluster="pytest", project="pytest-project", mockToolGlobals=None
    )
    return context


@pytest.fixture(scope="session")
def init_project(typer_context_no_cdf_tool_config: typer.Context, local_tmp_project_path_immutable: Path) -> Path:
    main_init(
        typer_context_no_cdf_tool_config,
        dry_run=False,
        upgrade=False,
        git_branch=None,
        init_dir=str(local_tmp_project_path_immutable),
        no_backup=True,
        clean=True,
    )
    return local_tmp_project_path_immutable


@pytest.fixture
def init_project_mutable(typer_context_no_cdf_tool_config: typer.Context, local_tmp_project_path_mutable: Path) -> Path:
    main_init(
        typer_context_no_cdf_tool_config,
        dry_run=False,
        upgrade=False,
        git_branch=None,
        init_dir=str(local_tmp_project_path_mutable),
        no_backup=True,
        clean=True,
    )
    return local_tmp_project_path_mutable


@pytest.fixture
def capture_print(monkeypatch: MonkeyPatch) -> PrintCapture:
    capture = PrintCapture()
    toolkit_path = REPO_ROOT / "cognite_toolkit"
    monkeypatch.setattr("cognite_toolkit._cdf.print", capture)

    # Monkeypatch all print functions in the toolkit automatically
    for folder in ["_cdf_tk", "_api"]:
        for py_file in (toolkit_path / folder).rglob("*.py"):
            file_path = py_file.relative_to(toolkit_path)
            module_path = f"{'.'.join(['cognite_toolkit', *file_path.parts[:-1]])}.{file_path.stem}"
            try:
                monkeypatch.setattr(f"{module_path}.print", capture)
            except AttributeError:
                # The module does not have a print function
                continue

    return capture
