from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AllScope,
    AssetsAcl,
    GroupCapability,
    GroupResponse,
    RelationshipsAcl,
    StreamsAcl,
)
from cognite_toolkit._cdf_tk.client.resource_classes.project import (
    Claim,
    OidcConfiguration,
    OrganizationResponse,
    UserProfilesConfiguration,
)
from cognite_toolkit._cdf_tk.client.resource_classes.token import (
    AllProjects,
    InspectCapability,
    InspectProjectInfo,
    InspectResponse,
)
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import AuthCommand
from cognite_toolkit._cdf_tk.constants import TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
    LowSeverityWarning,
    MissingCapabilityWarning,
)
from tests.test_unit.utils import MockQuestionary

CDF_PROJECT = "pytest-project"


def _create_organization_response() -> OrganizationResponse:
    return OrganizationResponse(
        name=CDF_PROJECT,
        url_name=CDF_PROJECT,
        organization="test-org",
        user_profiles_configuration=UserProfilesConfiguration(enabled=True),
        oidc_configuration=OidcConfiguration(
            jwks_url="https://login.windows.net/common/discovery/keys",
            token_url="https://login.windows.net/dummy/oauth2/token",
            issuer="https://sts.windows.net/dummy/",
            audience="https://test.cognitedata.com",
            access_claims=[Claim(claim_name="groups")],
            scope_claims=[Claim(claim_name="scp")],
            log_claims=[Claim(claim_name="appid")],
        ),
    )


def _create_inspect_response() -> InspectResponse:
    return InspectResponse(
        subject="test@test.com",
        projects=[InspectProjectInfo(project_url_name=CDF_PROJECT, groups=[123])],
        capabilities=[
            InspectCapability(
                acl=AssetsAcl(actions=["READ"], scope=AllScope()),
                project_scope=AllProjects(all_projects={}),
            )
        ],
        project=CDF_PROJECT,
    )


def _setup_verify_mocks(
    client: MagicMock,
    user_groups: list[GroupResponse],
    all_cdf_groups: list[GroupResponse] | None = None,
    data_modeling_status: str = "HYBRID",
) -> None:
    if all_cdf_groups is None:
        all_cdf_groups = list(user_groups)

    client.config.project = CDF_PROJECT
    client.config.is_private_link = False

    client.tool.token.inspect.return_value = _create_inspect_response()
    client.tool.token.verify_acls.return_value = []

    _user = user_groups
    _all = all_cdf_groups

    def groups_list(all_groups: bool = False) -> list[GroupResponse]:
        return _all if all_groups else _user

    client.tool.groups.list.side_effect = groups_list

    status_mock = MagicMock()
    status_mock.this_project.data_modeling_status = data_modeling_status
    client.project.status.return_value = status_mock

    client.project.organization.return_value = _create_organization_response()

    func_status = MagicMock()
    func_status.status = "activated"
    client.functions.status.return_value = func_status


class TestAuthCommand:
    def test_auth_verify_happy_path(self) -> None:
        with monkeypatch_toolkit_client() as client:
            required_acls, _ = AuthCommand._get_required_acls(client, "HYBRID")
            group = GroupResponse(
                id=123,
                is_deleted=False,
                name=TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME,
                source_id="test-source-id",
                capabilities=[GroupCapability(acl=acl) for acl in required_acls],
            )
            _setup_verify_mocks(client, user_groups=[group])

            cmd = AuthCommand(print_warning=False)
            cmd.verify(client, False, no_prompt=True)

            assert list(cmd.warning_list) == []

    def test_auth_verify_wrong_capabilities(self) -> None:
        with monkeypatch_toolkit_client() as client:
            required_acls, _ = AuthCommand._get_required_acls(client, "HYBRID")
            group = GroupResponse(
                id=123,
                is_deleted=False,
                name=TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME,
                source_id="test-source-id",
                capabilities=[GroupCapability(acl=acl) for acl in required_acls[:-3]],
            )
            _setup_verify_mocks(client, user_groups=[group])

            cmd = AuthCommand(print_warning=False)
            with pytest.raises(AuthorizationError):
                cmd.verify(client, False, no_prompt=True)

            missing_warnings = [w for w in cmd.warning_list if isinstance(w, MissingCapabilityWarning)]
            assert len(missing_warnings) > 0

    def test_auth_verify_two_groups(self) -> None:
        with monkeypatch_toolkit_client() as client:
            required_acls, _ = AuthCommand._get_required_acls(client, "HYBRID")
            toolkit_group = GroupResponse(
                id=123,
                is_deleted=False,
                name=TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME,
                source_id="test-source-id",
                capabilities=[GroupCapability(acl=acl) for acl in required_acls],
            )
            second_group = GroupResponse(
                id=456,
                is_deleted=False,
                name="second_group",
                source_id="other-source-id",
                capabilities=[],
            )
            _setup_verify_mocks(client, user_groups=[toolkit_group, second_group])

            cmd = AuthCommand(print_warning=False)
            cmd.verify(client, False, no_prompt=True)

            low_warnings = [w for w in cmd.warning_list if isinstance(w, LowSeverityWarning)]
            assert len(low_warnings) == 1

    def test_auth_verify_no_group_access(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.config.project = CDF_PROJECT
            client.tool.token.inspect.return_value = _create_inspect_response()
            client.tool.token.verify_acls.return_value = [AssetsAcl(actions=["READ"], scope=AllScope())]

            cmd = AuthCommand(print_warning=False)
            with pytest.raises(AuthorizationError) as exc_info:
                cmd.verify(client, False, no_prompt=True)

            assert "basic read group access rights" in str(exc_info.value)
            assert len(cmd.warning_list) == 1
            assert isinstance(cmd.warning_list[0], HighSeverityWarning)

    def test_auth_verify_interactive_update_capabilities(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with monkeypatch_toolkit_client() as client:
            required_acls, _ = AuthCommand._get_required_acls(client, "HYBRID")
            group = GroupResponse(
                id=123,
                is_deleted=False,
                name=TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME,
                source_id="test-source-id",
                capabilities=[GroupCapability(acl=acl) for acl in required_acls[:-3]],
            )
            _setup_verify_mocks(client, user_groups=[group])

            updated_group = GroupResponse(
                id=456,
                is_deleted=False,
                name=TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME,
                source_id="test-source-id",
                capabilities=[GroupCapability(acl=acl) for acl in required_acls],
            )
            client.tool.groups.create.return_value = [updated_group]
            client.tool.groups.delete.return_value = None

            monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.auth.Prompt.ask", lambda *a, **k: None)
            with MockQuestionary(
                "cognite_toolkit._cdf_tk.commands.auth",
                monkeypatch,
                answers=[True],
            ):
                cmd = AuthCommand(print_warning=False)
                result = cmd.verify(client, dry_run=False, no_prompt=False)

            assert result.toolkit_group_id is not None
            client.tool.groups.create.assert_called_once()


def test_get_capabilities_by_loader_hybrid_project() -> None:
    with monkeypatch_toolkit_client() as client:
        caps_hybrid, _ = AuthCommand._get_required_acls(client, "HYBRID")

    acl_types_hybrid = {type(c) for c in caps_hybrid}
    assert AssetsAcl in acl_types_hybrid
    assert RelationshipsAcl in acl_types_hybrid


def test_get_capabilities_by_loader_dm_only_project() -> None:
    with monkeypatch_toolkit_client() as client:
        caps_dm_only, _ = AuthCommand._get_required_acls(client, "DATA_MODELING_ONLY")

    acl_types_dm_only = {type(c) for c in caps_dm_only}
    assert AssetsAcl not in acl_types_dm_only
    assert RelationshipsAcl not in acl_types_dm_only


def test_update_missing_capabilities_dm_only_project() -> None:
    missing_acls = [
        StreamsAcl(actions=["READ"], scope=AllScope()),
    ]
    existing = GroupResponse(
        id=42,
        is_deleted=False,
        name=TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME,
        source_id="123",
        capabilities=[
            GroupCapability(acl=AssetsAcl(actions=["READ"], scope=AllScope())),
            GroupCapability(acl=RelationshipsAcl(actions=["READ"], scope=AllScope())),
        ],
    )
    created_response = GroupResponse(
        id=43,
        is_deleted=False,
        name=TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME,
        source_id="123",
        capabilities=[
            GroupCapability(acl=StreamsAcl(actions=["READ"], scope=AllScope())),
        ],
    )
    with monkeypatch_toolkit_client() as client:
        client.tool.groups.create.return_value = [created_response]
        client.tool.groups.delete.return_value = None

        result = AuthCommand()._update_missing_capabilities(
            client,
            existing,
            missing_acls,
            dry_run=False,
            project=CDF_PROJECT,
            data_modeling_status="DATA_MODELING_ONLY",
        )

    assert result is True
    client.tool.groups.create.assert_called_once()
    created_group_request = client.tool.groups.create.call_args[0][0][0]
    assert len(created_group_request.capabilities) == 1
    assert isinstance(created_group_request.capabilities[0].acl, StreamsAcl)
