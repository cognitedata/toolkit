import json
import platform
from pathlib import Path
from typing import Union
from unittest.mock import MagicMock

import pytest
import yaml
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.data_classes.capabilities import (
    AllProjectsScope,
    ProjectCapability,
    ProjectCapabilityList,
)
from cognite.client.data_classes.iam import Group, GroupList, ProjectSpec, TokenInspection
from pytest import MonkeyPatch

from cognite_toolkit._cdf_tk.bootstrap import check_auth
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.tests_unit.conftest import ApprovalCogniteClient

THIS_FOLDER = Path(__file__).resolve().parent

TEST_PREFIX = "auth"
DATA_FOLDER = THIS_FOLDER / f"{TEST_PREFIX}_data"
SNAPSHOTS_DIR = THIS_FOLDER / f"{TEST_PREFIX}_data_snapshots"
SNAPSHOTS_DIR.mkdir(exist_ok=True)


def to_fullpath(file_name: str) -> Path:
    if platform.system() == "Windows":
        # Windows console use different characters for tables in rich.
        return SNAPSHOTS_DIR / f"{file_name}_windows.txt"
    else:
        return SNAPSHOTS_DIR / f"{file_name}.txt"


@pytest.fixture
def cdf_tool_config(
    monkeypatch: MonkeyPatch,
) -> CDFToolConfig:
    monkeypatch.setenv("CDF_PROJECT", "pytest-project")
    monkeypatch.setenv("CDF_CLUSTER", "bluefield")
    monkeypatch.setenv("IDP_TOKEN_URL", "dummy")
    monkeypatch.setenv("IDP_CLIENT_ID", "dummy")
    monkeypatch.setenv("IDP_CLIENT_SECRET", "dummy")
    monkeypatch.setenv("IDP_TENANT_ID", "dummy")

    real_config = CDFToolConfig(cluster="bluefield", project="pytest-project")
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.failed = False
    cdf_tool.environment_variables.side_effect = real_config.environment_variables
    return cdf_tool


@pytest.fixture
def cdf_resources() -> dict[CogniteResource, Union[CogniteResource, CogniteResourceList]]:
    # Load the rw-group and add it to the mock client as an existing resource
    group = Group.load(yaml.safe_load((DATA_FOLDER / "rw-group.yaml").read_text()))
    project_capabilities: ProjectCapabilityList = []
    for cap in group.capabilities:
        project_capabilities.append(ProjectCapability(capability=cap, project_scope=AllProjectsScope()))
    inspect_result = TokenInspection(
        subject="test@test.com",
        projects=[ProjectSpec("pytest-project", groups=[1234567890])],
        capabilities=ProjectCapabilityList(project_capabilities),
    )
    return {
        TokenInspection: inspect_result,
        Group: GroupList([group]),
        # NOTE! If you add more resources to be pre-loaded in the CDF project, add them here.
    }


@pytest.fixture
def auth_cognite_approval_client(
    cognite_client_approval: ApprovalCogniteClient,
) -> ApprovalCogniteClient:
    # Mock the get call to return the project info
    def mock_get_json(*args, **kwargs):
        mock = MagicMock()
        mock.json.return_value = json.loads(Path(DATA_FOLDER / "project_info.json").read_text())
        return mock

    # Set the mock get call to return the project info
    cognite_client_approval.client.get.side_effect = mock_get_json
    return cognite_client_approval


def test_auth_verify_happypath(
    file_regression,
    cdf_tool_config: CDFToolConfig,
    auth_cognite_approval_client: ApprovalCogniteClient,
    cdf_resources: dict[CogniteResource, Union[CogniteResource, CogniteResourceList]],
    capfd,
):
    # First add the pre-loaded data to the approval_client
    for resource, data in cdf_resources.items():
        auth_cognite_approval_client.append(resource, data)
    # Then make sure that the CogniteClient used is the one mocked by
    # the approval_client
    cdf_tool_config.client = auth_cognite_approval_client.mock_client
    check_auth(cdf_tool_config, group_file=Path(DATA_FOLDER / "rw-group.yaml"))
    out, _ = capfd.readouterr()
    # Strip trailing spaces
    out = "\n".join([line.rstrip() for line in out.splitlines()])
    file_regression.check(out, encoding="utf-8", fullpath=to_fullpath(f"{TEST_PREFIX}_auth_verify_happypath"))

    dump = auth_cognite_approval_client.dump()
    assert dump == {}


def test_auth_verify_wrong_capabilities(
    file_regression,
    cdf_tool_config: CDFToolConfig,
    auth_cognite_approval_client: ApprovalCogniteClient,
    cdf_resources: dict[CogniteResource, Union[CogniteResource, CogniteResourceList]],
    capfd,
):
    # Remove the last 3 capabilities from the inspect result to make a test case
    # with wrong capabilities in the current CDF group.
    for _ in range(1, 4):
        del cdf_resources[TokenInspection].capabilities[-1]
    # Add the pre-loaded data to the approval_client
    for resource, data in cdf_resources.items():
        auth_cognite_approval_client.append(resource, data)
    # Then make sure that the CogniteClient used is the one mocked by
    # the approval_client
    cdf_tool_config.client = auth_cognite_approval_client.mock_client
    check_auth(cdf_tool_config, group_file=Path(DATA_FOLDER / "rw-group.yaml"))
    out, _ = capfd.readouterr()
    # Strip trailing spaces
    out = "\n".join([line.rstrip() for line in out.splitlines()])
    file_regression.check(out, encoding="utf-8", fullpath=to_fullpath(f"{TEST_PREFIX}_auth_verify_wrong_capabilities"))

    dump = auth_cognite_approval_client.dump()
    assert dump == {}


def test_auth_verify_two_groups(
    file_regression,
    cdf_tool_config: CDFToolConfig,
    auth_cognite_approval_client: ApprovalCogniteClient,
    cdf_resources: dict[CogniteResource, Union[CogniteResource, CogniteResourceList]],
    capfd,
):
    # Add another group
    cdf_resources[Group].append(Group.load(yaml.safe_load((DATA_FOLDER / "rw-group.yaml").read_text())))
    cdf_resources[Group][1].name = "2nd group"
    # Add the pre-loaded data to the approval_client
    for resource, data in cdf_resources.items():
        auth_cognite_approval_client.append(resource, data)
    # Then make sure that the CogniteClient used is the one mocked by
    # the approval_client
    cdf_tool_config.client = auth_cognite_approval_client.mock_client
    check_auth(cdf_tool_config, group_file=Path(DATA_FOLDER / "rw-group.yaml"))
    out, _ = capfd.readouterr()
    # Strip trailing spaces
    out = "\n".join([line.rstrip() for line in out.splitlines()])
    file_regression.check(out, encoding="utf-8", fullpath=to_fullpath(f"{TEST_PREFIX}_auth_verify_two_groups"))

    dump = auth_cognite_approval_client.dump()
    assert dump == {}


def test_auth_verify_no_capabilities(
    file_regression,
    cdf_tool_config: CDFToolConfig,
    auth_cognite_approval_client: ApprovalCogniteClient,
    cdf_resources: dict[CogniteResource, Union[CogniteResource, CogniteResourceList]],
    capfd,
):
    # Add the pre-loaded data to the approval_client
    for resource, data in cdf_resources.items():
        auth_cognite_approval_client.append(resource, data)
    # Then make sure that the CogniteClient used is the one mocked by
    # the approval_client
    cdf_tool_config.client = auth_cognite_approval_client.mock_client

    def mock_verify_client(*args, **kwargs):
        raise Exception("No capabilities")

    cdf_tool_config.verify_client.side_effect = mock_verify_client

    check_auth(cdf_tool_config, group_file=Path(DATA_FOLDER / "rw-group.yaml"))
    out, _ = capfd.readouterr()
    # Strip trailing spaces
    out = "\n".join([line.rstrip() for line in out.splitlines()])
    file_regression.check(out, encoding="utf-8", fullpath=to_fullpath(f"{TEST_PREFIX}_auth_verify_no_capabilities"))

    dump = auth_cognite_approval_client.dump()
    assert dump == {}
