import json
import platform
from pathlib import Path
from unittest.mock import MagicMock

import yaml
from cognite.client.data_classes.capabilities import (
    AllProjectsScope,
    ProjectCapability,
    ProjectCapabilityList,
)
from cognite.client.data_classes.iam import Group, GroupList, ProjectSpec, TokenInspection

from cognite_toolkit.cdf_tk.bootstrap import check_auth
from cognite_toolkit.cdf_tk.utils import CDFToolConfig
from tests.conftest import ApprovalCogniteClient

THIS_FOLDER = Path(__file__).resolve().parent

TEST_PREFIX = "auth"
DATA_FOLDER = THIS_FOLDER / f"{TEST_PREFIX}_data"
SNAPSHOTS_DIR = THIS_FOLDER / f"{TEST_PREFIX}_data_snapshots"
SNAPSHOTS_DIR.mkdir(exist_ok=True)


def test_auth_verify(
    cognite_client_approval: ApprovalCogniteClient,
    data_regression,
    file_regression,
    cdf_tool_config: CDFToolConfig,
    capfd,
    freezer,
):
    cdf_tool_config.client = cognite_client_approval.mock_client

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
    check_auth(cdf_tool_config, group_file=DATA_FOLDER / "rw-group.yaml")

    out, _ = capfd.readouterr()
    if platform.system() == "Windows":
        # Windows console use different characters for tables in rich.
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify_windows.txt"
    else:
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_auth_verify.txt"
    file_regression.check(out, encoding="utf-8", fullpath=fullpath)

    dump = cognite_client_approval.dump()
    assert dump == {}
    # calls = cognite_client_approval.retrieve_calls()
    # data_regression.check(calls, fullpath=SNAPSHOTS_DIR / "auth_verify.yaml")
