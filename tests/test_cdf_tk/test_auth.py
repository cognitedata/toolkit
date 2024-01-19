import json
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Union
from unittest.mock import MagicMock

import pytest
import yaml
from cognite.client._api.iam import IAMAPI
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
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


@dataclass
class CDFDataFixture:
    cdf_tool: CDFToolConfig = None
    approval_client: ApprovalCogniteClient = None
    # The data dict is used to keep the pre-loaded CDF project data.
    # Each CogniteResource needs to be added to the approval_client after the test
    # has modified the data for its test purposes.
    # E.g.
    #
    # approval_client.append(TokenInspection, modified_inspect_result)
    # approval_client.append(Group, modified_group_list)
    data: dict[CogniteResource, Union[CogniteResource, CogniteResourceList]] = None


@pytest.fixture
def cdf_data_fixture(
    cognite_client_approval: ApprovalCogniteClient,
    monkeypatch: MonkeyPatch,
) -> CDFDataFixture:
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

    # Mock the get call to return the project info
    def mock_get_json(*args, **kwargs):
        mock = MagicMock()
        mock.json.return_value = json.loads(Path(DATA_FOLDER / "project_info.json").read_text())
        return mock

    # Set the mock get call to return the project info
    cognite_client_approval.client.get.side_effect = mock_get_json

    # Set the side effect of the MagicMock to the real method
    cognite_client_approval.client.iam.compare_capabilities.side_effect = IAMAPI.compare_capabilities

    return CDFDataFixture(
        cdf_tool=cdf_tool,
        approval_client=cognite_client_approval,
        data={
            TokenInspection: inspect_result,
            Group: GroupList([group]),
            # NOTE! If you add more resources to be pre-loaded in the CDF project, add them here.
        },
    )


def test_auth_verify_happypath(
    file_regression,
    cdf_data_fixture: CDFDataFixture,
    capfd,
):
    # First add the pre-loaded data to the approval_client
    for resource, data in cdf_data_fixture.data.items():
        cdf_data_fixture.approval_client.append(resource, data)
    # Then make sure that the CogniteClient used is the one mocked by
    # the approval_client
    cdf_data_fixture.cdf_tool.client = cdf_data_fixture.approval_client.mock_client
    check_auth(cdf_data_fixture.cdf_tool, group_file=DATA_FOLDER / "rw-group.yaml")
    out, _ = capfd.readouterr()
    if platform.system() == "Windows":
        # Windows console use different characters for tables in rich.
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify_happypath_windows.txt"
    else:
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify_happypath.txt"
    file_regression.check(out, encoding="utf-8", fullpath=fullpath)

    dump = cdf_data_fixture.approval_client.dump()
    assert dump == {}


def test_auth_verify_wrong_capabilities(
    file_regression,
    cdf_data_fixture: CDFDataFixture,
    capfd,
):
    # Remove the last 3 capabilities from the inspect result to make a test case
    # with wrong capabilities in the current CDF group.
    for _ in range(1, 4):
        del cdf_data_fixture.data[TokenInspection].capabilities[-1]
    # Add the pre-loaded data to the approval_client
    for resource, data in cdf_data_fixture.data.items():
        cdf_data_fixture.approval_client.append(resource, data)
    # Then make sure that the CogniteClient used is the one mocked by
    # the approval_client
    cdf_data_fixture.cdf_tool.client = cdf_data_fixture.approval_client.mock_client
    check_auth(cdf_data_fixture.cdf_tool, group_file=DATA_FOLDER / "rw-group.yaml")
    out, _ = capfd.readouterr()
    if platform.system() == "Windows":
        # Windows console use different characters for tables in rich.
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify_wrong_capabilities_windows.txt"
    else:
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify_wrong_capabilities.txt"
    file_regression.check(out, encoding="utf-8", fullpath=fullpath)

    dump = cdf_data_fixture.approval_client.dump()
    assert dump == {}


def test_auth_verify_two_groups(
    file_regression,
    cdf_data_fixture: CDFDataFixture,
    capfd,
):
    # Add another group
    cdf_data_fixture.data[Group].append(Group.load(yaml.safe_load((DATA_FOLDER / "rw-group.yaml").read_text())))
    cdf_data_fixture.data[Group][1].name = "2nd group"
    # Add the pre-loaded data to the approval_client
    for resource, data in cdf_data_fixture.data.items():
        cdf_data_fixture.approval_client.append(resource, data)
    # Then make sure that the CogniteClient used is the one mocked by
    # the approval_client
    cdf_data_fixture.cdf_tool.client = cdf_data_fixture.approval_client.mock_client
    check_auth(cdf_data_fixture.cdf_tool, group_file=DATA_FOLDER / "rw-group.yaml")
    out, _ = capfd.readouterr()
    if platform.system() == "Windows":
        # Windows console use different characters for tables in rich.
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify_two_groups_windows.txt"
    else:
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify_two_groups.txt"
    file_regression.check(out, encoding="utf-8", fullpath=fullpath)

    dump = cdf_data_fixture.approval_client.dump()
    assert dump == {}


def test_auth_verify_no_capabilities(
    file_regression,
    cdf_data_fixture: CDFDataFixture,
    capfd,
):
    # Add the pre-loaded data to the approval_client
    for resource, data in cdf_data_fixture.data.items():
        cdf_data_fixture.approval_client.append(resource, data)
    # Then make sure that the CogniteClient used is the one mocked by
    # the approval_client
    cdf_data_fixture.cdf_tool.client = cdf_data_fixture.approval_client.mock_client

    def mock_verify_client(*args, **kwargs):
        raise Exception("No capabilities")

    cdf_data_fixture.cdf_tool.verify_client.side_effect = mock_verify_client

    check_auth(cdf_data_fixture.cdf_tool, group_file=DATA_FOLDER / "rw-group.yaml")
    out, _ = capfd.readouterr()
    if platform.system() == "Windows":
        # Windows console use different characters for tables in rich.
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify_no_capabilities_windows.txt"
    else:
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify_no_capabilities.txt"
    file_regression.check(out, encoding="utf-8", fullpath=fullpath)

    dump = cdf_data_fixture.approval_client.dump()
    assert dump == {}
