import json
import platform
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from cognite.client.data_classes.capabilities import (
    AllProjectsScope,
    ProjectCapability,
    ProjectCapabilityList,
)
from cognite.client.data_classes.iam import Group, GroupList, ProjectSpec, TokenInspection
from pytest import MonkeyPatch

from cognite_toolkit.cdf_tk.bootstrap import check_auth
from cognite_toolkit.cdf_tk.utils import CDFToolConfig
from tests.conftest import ApprovalCogniteClient

THIS_FOLDER = Path(__file__).resolve().parent

TEST_PREFIX = "auth"
DATA_FOLDER = THIS_FOLDER / f"{TEST_PREFIX}_data"
SNAPSHOTS_DIR = THIS_FOLDER / f"{TEST_PREFIX}_data_snapshots"
SNAPSHOTS_DIR.mkdir(exist_ok=True)


@pytest.fixture
def cdf_tool(
    cognite_client_approval: ApprovalCogniteClient,
    monkeypatch: MonkeyPatch,
) -> CDFToolConfig:
    monkeypatch.setenv("CDF_PROJECT", "pytest-project")
    monkeypatch.setenv("CDF_CLUSTER", "bluefield")
    monkeypatch.setenv("IDP_TOKEN_URL", "dummy")
    monkeypatch.setenv("IDP_CLIENT_ID", "dummy")
    monkeypatch.setenv("IDP_CLIENT_SECRET", "dummy")
    monkeypatch.setenv("IDP_TENANT_ID", "dummy")

    real_config = CDFToolConfig(cluster="bluefield", project="pytest-project")
    # Build must always be executed from root of the project
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.failed = False
    cdf_tool.environment_variables.side_effect = real_config.environment_variables
    cdf_tool.cognite_approval_client = cognite_client_approval
    cdf_tool.client = cognite_client_approval.mock_client
    # Load the rw-group and add it to the mock client as an existing resource
    group = Group.load(yaml.safe_load((DATA_FOLDER / "rw-group.yaml").read_text()))
    project_capabilities: ProjectCapabilityList = []
    for cap in group.capabilities:
        project_capabilities.append(ProjectCapability(capability=cap, project_scope=AllProjectsScope()))
    inspect_result = TokenInspection(
        subject="test@test.com",
        projects=[ProjectSpec("pytest-project", groups=[1234567890])],
        capabilities=project_capabilities,
    )
    # This is to mock the iam.token.inspect call
    cognite_client_approval.append(TokenInspection, inspect_result)
    # This is to mock the iam.groups.create call
    cognite_client_approval.append(Group, GroupList([group]))

    # Mock the get call to return the project info
    def mock_get_json(*args, **kwargs):
        mock = MagicMock()
        mock.json.return_value = json.loads(Path(DATA_FOLDER / "project_info.json").read_text())
        return mock

    # Set the mock get call to return the project info
    cognite_client_approval.client.get.side_effect = mock_get_json
    return cdf_tool


def test_auth_verify(
    data_regression,
    file_regression,
    cdf_tool: CDFToolConfig,
    capfd,
):
    check_auth(cdf_tool, group_file=DATA_FOLDER / "rw-group.yaml")

    out, _ = capfd.readouterr()
    if platform.system() == "Windows":
        # Windows console use different characters for tables in rich.
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify_windows.txt"
    else:
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify.txt"
    file_regression.check(out, encoding="utf-8", fullpath=fullpath)

    dump = cdf_tool.cognite_approval_client.dump()
    assert dump == {}
    # calls = cdf_tool.cognite_approval_client.retrieve_calls()
    # data_regression.check(calls, fullpath=SNAPSHOTS_DIR / "auth_verify.yaml")
