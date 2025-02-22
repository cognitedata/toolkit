from __future__ import annotations

import json
from pathlib import Path
from typing import Union
from unittest.mock import MagicMock

import pytest
import yaml
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse
from cognite.client.data_classes.capabilities import (
    AllProjectsScope,
    Capability,
    ProjectCapability,
    ProjectCapabilityList,
)
from cognite.client.data_classes.iam import Group, GroupList, ProjectSpec, TokenInspection

from cognite_toolkit._cdf_tk.commands import AuthCommand
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
    LowSeverityWarning,
    MissingCapabilityWarning,
    ToolkitWarning,
    WarningList,
)
from tests.data import AUTH_DATA
from tests.test_unit.conftest import ApprovalToolkitClient


@pytest.fixture
def cdf_resources() -> dict[type[CogniteResource] | type[CogniteResponse], CogniteResource | CogniteResourceList]:
    # Load the rw-group and add it to the mock client as an existing resource
    group = Group.load(yaml.safe_load((AUTH_DATA / "rw-group.yaml").read_text()))
    project_capabilities: ProjectCapabilityList = ProjectCapabilityList([])
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
    toolkit_client_approval: ApprovalToolkitClient,
) -> ApprovalToolkitClient:
    # Mock the get call to return the project info
    def mock_get_json(*args, **kwargs):
        mock = MagicMock()
        mock.json.return_value = json.loads(Path(AUTH_DATA / "project_info.json").read_text())
        return mock

    # Set the mock get call to return the project info
    toolkit_client_approval.client.get.side_effect = mock_get_json
    # Returning empty list means no capabilities are missing
    toolkit_client_approval.client.iam.verify_capabilities.return_value = []
    return toolkit_client_approval


@pytest.mark.skip("Needs to be rewritten to support interactive")
class TestAuthCommand:
    def test_auth_verify_happy_path(
        self,
        auth_cognite_approval_client: ApprovalToolkitClient,
        cdf_resources: dict[type[CogniteResource], CogniteResource | CogniteResourceList],
    ):
        # First, add the pre-loaded data to the approval_client
        for resource, data in cdf_resources.items():
            auth_cognite_approval_client.append(resource, data)
        # Then make sure that the CogniteClient used is the one mocked by
        # the approval_client
        cmd = AuthCommand(print_warning=False)

        cmd.verify(auth_cognite_approval_client.mock_client, False, no_prompt=True)

        assert list(cmd.warning_list) == []

    def test_auth_verify_wrong_capabilities(
        self,
        auth_cognite_approval_client: ApprovalToolkitClient,
        cdf_resources: dict[type[CogniteResource], CogniteResource | CogniteResourceList],
    ):
        expected_warnings = WarningList[ToolkitWarning]()
        # Remove the last 3 capabilities from the inspect result to make a test case
        # with wrong capabilities in the current CDF group.
        for _ in range(1, 4):
            capability = cdf_resources[TokenInspection].capabilities.pop()
            for cap in capability.capability.as_tuples():
                expected_warnings.append(MissingCapabilityWarning(str(Capability.from_tuple(cap))))
        # Add the pre-loaded data to the approval_client
        for resource, data in cdf_resources.items():
            auth_cognite_approval_client.append(resource, data)
        # Then make sure that the CogniteClient used is the one mocked by
        # the approval_client
        cmd = AuthCommand(print_warning=False)

        cmd.verify(auth_cognite_approval_client.mock_client, False, no_prompt=True)

        assert len(cmd.warning_list) == len(expected_warnings)
        assert set(cmd.warning_list) == set(expected_warnings)

    def test_auth_verify_two_groups(
        self,
        auth_cognite_approval_client: ApprovalToolkitClient,
        cdf_resources: dict[CogniteResource, Union[CogniteResource, CogniteResourceList]],
    ):
        # Add another group
        cdf_resources[Group].append(Group.load(yaml.safe_load((AUTH_DATA / "rw-group.yaml").read_text())))
        cdf_resources[Group][1].name = "2nd group"

        # Add the pre-loaded data to the approval_client
        for resource, data in cdf_resources.items():
            auth_cognite_approval_client.append(resource, data)
        # Then make sure that the CogniteClient used is the one mocked by
        # the approval_client
        cmd = AuthCommand(print_warning=False)
        cmd.verify(auth_cognite_approval_client.mock_client, False, no_prompt=True)

        assert len(cmd.warning_list) == 1
        assert set(cmd.warning_list) == {
            LowSeverityWarning(
                "This service principal/application gets its "
                "access rights from more than one CDF "
                "group.           This is not recommended. The "
                "group matching the group config file is "
                "marked in bold above if it is present."
            )
        }

    def test_auth_verify_no_capabilities(
        self,
        auth_cognite_approval_client: ApprovalToolkitClient,
        cdf_resources: dict[type[CogniteResource], Union[CogniteResource, CogniteResourceList]],
    ):
        # Add the pre-loaded data to the approval_client
        for resource, data in cdf_resources.items():
            auth_cognite_approval_client.append(resource, data)

        def mock_verify_client(*args, **kwargs):
            raise Exception("No capabilities")

        cmd = AuthCommand(print_warning=False)
        with pytest.raises(AuthorizationError) as e:
            cmd.verify(auth_cognite_approval_client.mock_client, False, no_prompt=True)

        assert len(cmd.warning_list) == 1
        assert set(cmd.warning_list) == {
            HighSeverityWarning(
                "The service principal/application configured for this client "
                "does not have the basic group write access rights."
            )
        }
        assert str(e.value) == (
            "Unable to continue, the service principal/application configured for this "
            "client does not have the basic read group access rights."
        )
