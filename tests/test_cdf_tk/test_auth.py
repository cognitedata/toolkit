import platform
from pathlib import Path
from unittest.mock import MagicMock

import yaml
from cognite.client.data_classes.capabilities import (
    AllProjectsScope,
    ProjectCapability,
    ProjectCapabilityList,
)
from cognite.client.data_classes.iam import GroupWrite, TokenInspection

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
    capfd,
    freezer,
):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.project = "test"
    cdf_tool.client = cognite_client_approval.mock_client
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client

    # cognite_client_approval.append(DataModel, data_models)
    group = GroupWrite.load(yaml.safe_load((DATA_FOLDER / "rw-group.yaml").read_text()))
    project_capabilities: ProjectCapabilityList = []
    for cap in group.capabilities:
        project_capabilities.append(ProjectCapability(capability=cap, project_scope=AllProjectsScope()))
    inspect = TokenInspection(subject="test", projects=["test"], capabilities=project_capabilities)
    cognite_client_approval.append(TokenInspection, [inspect])

    check_auth(cdf_tool, group_file=DATA_FOLDER / "rw-group.yaml")

    out, _ = capfd.readouterr()
    if platform.system() == "Windows":
        # Windows console use different characters for tables in rich.
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_datamodel_windows.txt"
    else:
        fullpath = SNAPSHOTS_DIR / f"{TEST_PREFIX}_datamodel.txt"
    file_regression.check(out, encoding="utf-8", fullpath=fullpath)

    dump = cognite_client_approval.dump()
    assert dump == {}
    calls = cognite_client_approval.retrieve_calls()
    data_regression.check(calls, fullpath=SNAPSHOTS_DIR / "describe_datamodel.yaml")
