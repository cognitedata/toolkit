from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import typer
from pytest import MonkeyPatch

from cognite_toolkit._cdf_tk.apps._core_app import Common
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import ModulesCommand, RepoCommand
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from cognite_toolkit._cdf_tk.utils.auth2 import EnvironmentVariables
from tests.constants import REPO_ROOT, chdir
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.utils import PrintCapture

THIS_FOLDER = Path(__file__).resolve().parent
TMP_FOLDER = THIS_FOLDER / "tmp"
TMP_FOLDER.mkdir(exist_ok=True)


@pytest.fixture
def toolkit_client_approval() -> Iterator[ApprovalToolkitClient]:
    with monkeypatch_toolkit_client() as toolkit_client:
        approval_client = ApprovalToolkitClient(toolkit_client)
        yield approval_client


@pytest.fixture(scope="function")
def env_vars_with_client(toolkit_client_approval: ApprovalToolkitClient) -> EnvironmentVariables:
    env_vars = EnvironmentVariables(
        CDF_CLUSTER="bluefield",
        CDF_PROJECT="pytest-project",
        LOGIN_FLOW="client_credentials",
        PROVIDER="entra_id",
        IDP_CLIENT_ID="dummy-123",
        IDP_CLIENT_SECRET="dummy-secret",
        IDP_TENANT_ID="dummy-domain",
    )
    env_vars._client = toolkit_client_approval.mock_client
    return env_vars


@pytest.fixture(scope="session")
def build_tmp_path() -> Iterator[Path]:
    pidid = os.getpid()
    build_folder = TMP_FOLDER / f"build-{pidid}"
    if build_folder.exists():
        shutil.rmtree(build_folder, ignore_errors=True)
        build_folder.mkdir(exist_ok=True)
    yield build_folder
    shutil.rmtree(build_folder, ignore_errors=True)


@pytest.fixture(scope="session")
def local_tmp_repo_path() -> Iterator[Path]:
    pidid = os.getpid()
    repo_path = TMP_FOLDER / f"pytest-repo-{pidid}"
    repo_path.mkdir(exist_ok=True)
    RepoCommand(silent=True, skip_git_verify=True).init(repo_path, host="GitHub")
    yield repo_path
    shutil.rmtree(repo_path, ignore_errors=True)


@pytest.fixture(scope="function")
def cdf_tool_mock(
    toolkit_client_approval: ApprovalToolkitClient,
    monkeypatch: MonkeyPatch,
) -> Iterator[CDFToolConfig]:
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
        "IDP_FUN_CLIENT_ID": "dummy",
        "IDP_FUN_CLIENT_SECRET": "dummy",
        "IDP_WF_CLIENT_ID": "dummy",
        "IDP_WF_CLIENT_SECRET": "dummy",
        # The secrets in the cdf_ingestion workflow
        "INGESTION_CLIENT_ID": "this-is-the-ingestion-client-id",
        "INGESTION_CLIENT_SECRET": "this-is-the-ingestion-client-secret",
        "NON-SECRET": "this-is-not-a-secret",
    }
    existing = {}
    for key, value in environment_variables.items():
        existing[key] = os.environ.get(key)
        os.environ[key] = value

    with chdir(REPO_ROOT):
        real_config = CDFToolConfig(cluster="bluefield", project="pytest-project")
        # Build must always be executed from root of the project
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.toolkit_client = toolkit_client_approval.mock_client
        cdf_tool._login_flow = "client_credentials"
        cdf_tool._scopes = ["https://bluefield.cognitedata.com/.default"]
        cdf_tool._credentials_args = {
            "client_id": "dummy-123",
            "client_secret": "dummy-secret",
            "token_url": "dummy-url",
        }
        cdf_tool._project = "pytest-project"
        cdf_tool._client_name = "pytest"
        cdf_tool._cdf_url = "https://bluefield.cognitedata.com"
        cdf_tool._token_url = "dummy-url"

        cdf_tool.environment_variables.side_effect = real_config.environment_variables

        yield cdf_tool

    for key, value in existing.items():
        if value is None:
            del os.environ[key]
        else:
            os.environ[key] = value


@pytest.fixture
def cdf_tool_real(toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch) -> CDFToolConfig:
    monkeypatch.setenv("CDF_PROJECT", "pytest-project")
    monkeypatch.setenv("CDF_CLUSTER", "bluefield")
    monkeypatch.setenv("IDP_TOKEN_URL", "dummy")
    monkeypatch.setenv("IDP_CLIENT_ID", "dummy")
    monkeypatch.setenv("IDP_CLIENT_SECRET", "dummy")

    return CDFToolConfig(cluster="bluefield", project="pytest-project")


@pytest.fixture
def typer_context(cdf_tool_mock: CDFToolConfig, toolkit_client_approval: ApprovalToolkitClient) -> typer.Context:
    context = MagicMock(spec=typer.Context)
    context.obj = Common(
        override_env=True, mockToolGlobals=cdf_tool_mock, mock_client=toolkit_client_approval.mock_client
    )
    return context


@pytest.fixture(scope="session")
def typer_context_without_cdf_tool() -> typer.Context:
    context = MagicMock(spec=typer.Context)
    context.obj = Common(override_env=True, mockToolGlobals=None)
    return context


@pytest.fixture(scope="session")
def organization_dir(
    typer_context_without_cdf_tool: typer.Context,
    local_tmp_repo_path: Path,
) -> Path:
    organization_folder = "pytest-org"
    organization_dir = local_tmp_repo_path / organization_folder
    ModulesCommand(silent=True).init(
        organization_dir,
        select_all=True,
        clean=True,
    )

    return organization_dir


@pytest.fixture
def organization_dir_mutable(
    typer_context_without_cdf_tool: typer.Context,
    local_tmp_repo_path: Path,
) -> Path:
    """This is used in tests were the source module files are modified. For example, cdf pull commands."""
    organization_dir = local_tmp_repo_path / "pytest-org-mutable"

    ModulesCommand(silent=True).init(
        organization_dir,
        select_all=True,
        clean=True,
    )

    return organization_dir


@pytest.fixture
def capture_print(monkeypatch: MonkeyPatch) -> PrintCapture:
    capture = PrintCapture()
    toolkit_path = REPO_ROOT / "cognite_toolkit"
    builtin_modules_path = toolkit_path / "_cdf_tk" / "_packages"
    monkeypatch.setattr("cognite_toolkit._cdf.print", capture)

    # Monkeypatch all print functions in the toolkit automatically
    for folder in ["_cdf_tk", "_api"]:
        for py_file in (toolkit_path / folder).rglob("*.py"):
            if py_file.is_relative_to(builtin_modules_path):
                # Don't monkeypatch the function code of the
                # builtin modules
                continue
            file_path = py_file.relative_to(toolkit_path)
            module_path = f"{'.'.join(['cognite_toolkit', *file_path.parts[:-1]])}.{file_path.stem}"
            try:
                monkeypatch.setattr(f"{module_path}.print", capture)
            except AttributeError:
                # The module does not have a print function
                continue
            except ImportError:
                raise

    return capture
