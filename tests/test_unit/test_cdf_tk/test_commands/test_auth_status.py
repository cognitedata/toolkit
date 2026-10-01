from io import StringIO

import pytest
from rich.console import Console

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AllScope,
    AssetsAcl,
    DataModelInstancesAcl,
    DataModelsAcl,
    DataSetScope,
    EventsAcl,
    FilesAcl,
    FunctionsAcl,
    GroupsAcl,
)
from cognite_toolkit._cdf_tk.client.resource_classes.project import (
    Claim,
    OidcConfiguration,
    OrganizationResponse,
    ProjectStatusList,
    UserProfilesConfiguration,
)
from cognite_toolkit._cdf_tk.client.resource_classes.token import (
    AllProjects,
    FlatCapabilities,
    InspectCapability,
    InspectProjectInfo,
    InspectResponse,
    ProjectList,
)
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands.auth.data_classes import EnvironmentVariables
from cognite_toolkit._cdf_tk.commands.auth.session_store import SessionMetadata, StoredSession
from cognite_toolkit._cdf_tk.commands.auth.status_report import (
    _describe_identity_provider,
    collect_auth_status,
    format_action_access,
    render_auth_status,
    resolve_action_access,
    resource_label,
    resources_from_capabilities,
)
from cognite_toolkit._cdf_tk.resource_ios import AssetIO, GroupIO

CDF_PROJECT = "pytest-project"


def _environment() -> EnvironmentVariables:
    return EnvironmentVariables(
        CDF_CLUSTER="westeurope-1",
        CDF_PROJECT=CDF_PROJECT,
        PROVIDER="entra_id",
        LOGIN_FLOW="client_credentials",
        IDP_TENANT_ID="tenant-from-env",
    )


def _organization(token_url: str) -> OrganizationResponse:
    return OrganizationResponse(
        name=CDF_PROJECT,
        url_name=CDF_PROJECT,
        organization="test-org",
        user_profiles_configuration=UserProfilesConfiguration(enabled=True),
        oidc_configuration=OidcConfiguration(
            jwks_url="https://login.windows.net/common/discovery/keys",
            token_url=token_url,
            issuer="https://sts.windows.net/dummy/",
            audience="https://test.cognitedata.com",
            access_claims=[Claim(claim_name="groups")],
            scope_claims=[Claim(claim_name="scp")],
            log_claims=[Claim(claim_name="appid")],
        ),
    )


def _inspect_response() -> InspectResponse:
    return InspectResponse(
        subject="principal-1",
        projects=[
            InspectProjectInfo(project_url_name=CDF_PROJECT, groups=[11, 12]),
            InspectProjectInfo(project_url_name="other-project", groups=[99]),
        ],
        capabilities=[
            InspectCapability(
                acl=GroupsAcl(actions=["LIST", "READ"], scope=AllScope()),
                project_scope=AllProjects(all_projects={}),
            ),
            InspectCapability(
                acl=FunctionsAcl(actions=["READ"], scope=AllScope()),
                project_scope=AllProjects(all_projects={}),
            ),
            InspectCapability(
                acl=FilesAcl(actions=["READ"], scope=DataSetScope(ids=[7])),
                project_scope=AllProjects(all_projects={}),
            ),
            InspectCapability(
                acl=AssetsAcl(actions=["READ"], scope=DataSetScope(ids=[1])),
                project_scope=ProjectList(projects=[CDF_PROJECT]),
            ),
            InspectCapability(
                acl=AssetsAcl(actions=["READ"], scope=AllScope()),
                project_scope=ProjectList(projects=[CDF_PROJECT]),
            ),
            InspectCapability(
                acl=EventsAcl(actions=["READ"], scope=DataSetScope(ids=[5])),
                project_scope=ProjectList(projects=["other-project"]),
            ),
        ],
        project=CDF_PROJECT,
    )


class TestIdentityProvider:
    @pytest.mark.parametrize(
        "token_url, expected",
        [
            pytest.param(
                "https://login.windows.net/dummy/oauth2/token",
                ("Microsoft Entra ID", "dummy", ("groups",)),
                id="entra windows.net",
            ),
            pytest.param(
                "https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token",
                ("Microsoft Entra ID", "tenant-id", ("groups",)),
                id="entra microsoftonline",
            ),
            pytest.param(
                "https://my-tenant.auth0.com/oauth/token",
                ("Auth0", "my-tenant", ("groups",)),
                id="auth0",
            ),
            pytest.param(
                "https://auth.cognite.com/oauth2/token",
                ("Cognite IDP", None, ("groups",)),
                id="cognite",
            ),
            pytest.param(
                "https://idp.example.com/oauth/token",
                ("idp.example.com", None, ("groups",)),
                id="unknown host",
            ),
        ],
    )
    def test_describe_identity_provider(
        self, token_url: str, expected: tuple[str, str | None, tuple[str, ...]]
    ) -> None:
        described = _describe_identity_provider(_organization(token_url))
        assert (None if described is None else (described.name, described.tenant, described.access_claims)) == expected

    def test_missing_oidc_configuration(self) -> None:
        organization = _organization("https://auth.cognite.com/oauth2/token")
        organization.oidc_configuration = None
        assert _describe_identity_provider(organization) is None


class TestAuthStatus:
    def test_collect_reports_method_projects_and_merged_access(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.tool.token.inspect.return_value = _inspect_response()
            client.project.organization.return_value = _organization("https://login.windows.net/dummy/oauth2/token")
            client.project.status.return_value = ProjectStatusList.model_validate(
                {
                    "items": [
                        {"urlName": CDF_PROJECT, "dataModelingStatus": "HYBRID"},
                        {"urlName": "other-project", "dataModelingStatus": "DATA_MODELING_ONLY"},
                    ]
                }
            )
            report = collect_auth_status(client, _environment())

        current = next(project for project in report.projects if project.is_current)
        other = next(project for project in report.projects if project.name == "other-project")
        current_resources = {item.io_name: item for item in current.resources}
        other_resources = {item.io_name: item for item in other.resources}
        assert {
            "authenticated": report.authenticated,
            "method": report.method_label,
            "provider": None if report.identity_provider is None else report.identity_provider.name,
            "tenant": None if report.identity_provider is None else report.identity_provider.tenant,
            "subject": report.subject,
            "cluster": report.cluster,
            "projects": [
                (project.name, project.capability_count, project.group_count, project.data_modeling_status)
                for project in report.projects
            ],
            "asset_read": format_action_access(current_resources["AssetIO"].read),
            "asset_hidden_on_data_modeling_only": "AssetIO" not in other_resources,
            "function_read": format_action_access(current_resources["FunctionIO"].read),
            "group_read": format_action_access(current_resources["GroupAllScopedIO"].read),
            "events_only_on_other": "EventIO" in other_resources and "EventIO" not in current_resources,
        } == {
            "authenticated": True,
            "method": "Service principal",
            "provider": "Microsoft Entra ID",
            "tenant": "dummy",
            "subject": "principal-1",
            "cluster": "bluefield",
            "projects": [
                (CDF_PROJECT, 4, 2, "HYBRID"),
                ("other-project", 4, 1, "DATA_MODELING_ONLY"),
            ],
            "asset_read": "all",
            "asset_hidden_on_data_modeling_only": True,
            "function_read": "functionsAcl all | filesAcl datasetScope [7]",
            "group_read": "all",
            "events_only_on_other": True,
        }

    def test_invalid_token_is_not_authenticated(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.tool.token.inspect.side_effect = ToolkitAPIError("Invalid token")
            report = collect_auth_status(client, _environment())
        assert (report.authenticated, report.method_label, report.failure) == (
            False,
            "Service principal",
            "Not authenticated. Invalid token",
        )

    def test_missing_credentials(self) -> None:
        report = collect_auth_status()
        assert (report.authenticated, report.method_label, "cdf auth login" in (report.failure or "")) == (
            False,
            "Unknown",
            True,
        )

    def test_session_flow_reports_organization_and_expiry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        metadata = SessionMetadata(
            version=1,
            org="acme",
            access_token_expires_at="2099-01-01T00:00:00.000Z",
            refresh_token_expires_at="2099-02-01T00:00:00.000Z",
        )
        monkeypatch.setattr(StoredSession, "load_metadata", classmethod(lambda cls: metadata))
        environment = EnvironmentVariables(
            CDF_CLUSTER="westeurope-1",
            CDF_PROJECT=CDF_PROJECT,
            PROVIDER="cdf",
            LOGIN_FLOW="session",
        )
        with monkeypatch_toolkit_client() as client:
            client.tool.token.inspect.return_value = _inspect_response()
            client.project.organization.return_value = _organization("https://auth.cognite.com/oauth2/token")
            client.project.status.return_value = ProjectStatusList.model_validate(
                {"items": [{"urlName": CDF_PROJECT, "dataModelingStatus": "HYBRID"}]}
            )
            report = collect_auth_status(client, environment)

        assert (
            report.authenticated,
            report.method_label,
            None if report.identity_provider is None else report.identity_provider.name,
            None if report.session is None else report.session.organization,
            None if report.session is None else report.session.state,
        ) == (True, "Session", "Cognite IDP", "acme", "VALID")

    def test_unrecognized_actions_do_not_hide_deploy_access(self) -> None:
        payloads = [
            {
                "groupsAcl": {
                    "actions": ["LIST", "READ", "CREATE", "UPDATE", "DELETE", "FUTURE"],
                    "scope": {"all": {}},
                }
            },
            {
                "securityCategoriesAcl": {
                    "actions": ["LIST", "MEMBEROF", "CREATE", "UPDATE", "DELETE", "FUTURE"],
                    "scope": {"all": {}},
                }
            },
            {"sessionsAcl": {"actions": ["LIST", "CREATE", "DELETE", "FUTURE"], "scope": {"all": {}}}},
        ]
        inspected = InspectResponse(
            subject="user",
            projects=[InspectProjectInfo(project_url_name=CDF_PROJECT, groups=[1])],
            project=CDF_PROJECT,
            capabilities=[
                InspectCapability.model_validate({**payload, "projectScope": {"allProjects": {}}})
                for payload in payloads
            ],
        )
        resources, _ = resources_from_capabilities(inspected.to_project_capabilities(CDF_PROJECT), "HYBRID")
        assert {item.io_name for item in resources} == {
            "FunctionScheduleIO",
            "GroupAllScopedIO",
            "GroupResourceScopedIO",
            "SecurityCategoryIO",
        }

    def test_group_read_requires_every_action(self) -> None:
        capabilities = FlatCapabilities({(GroupsAcl, "groupsAcl", "READ"): AllScope()}, name=CDF_PROJECT, groups=[])
        access = resolve_action_access(GroupIO, capabilities, "READ")
        assert (access.granted, access.missing) == (False, ["groupsAcl LIST"])

    def test_data_modeling_only_skips_classic_assets(self) -> None:
        capabilities = FlatCapabilities({(AssetsAcl, "assetsAcl", "READ"): AllScope()}, name=CDF_PROJECT, groups=[1])
        hybrid, _ = resources_from_capabilities(capabilities, "HYBRID")
        data_modeling_only, _ = resources_from_capabilities(capabilities, "DATA_MODELING_ONLY")
        assert {
            "hybrid": [item.io_name for item in hybrid],
            "data_modeling_only": [item.io_name for item in data_modeling_only],
        } == {"hybrid": [AssetIO.__name__], "data_modeling_only": []}

    def test_render_verbose_output(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.tool.token.inspect.return_value = _inspect_response()
            client.project.organization.return_value = _organization("https://login.windows.net/dummy/oauth2/token")
            client.project.status.return_value = ProjectStatusList.model_validate(
                {"items": [{"urlName": CDF_PROJECT, "dataModelingStatus": "HYBRID"}]}
            )
            report = collect_auth_status(client, _environment())

        buffer = StringIO()
        console = Console(file=buffer, width=120, force_terminal=False, no_color=True, highlight=False)
        render_auth_status(report, verbose=True, console=console)
        text = buffer.getvalue()
        all_buffer = StringIO()
        all_console = Console(file=all_buffer, width=120, force_terminal=False, no_color=True, highlight=False)
        render_auth_status(report, verbose=True, all_projects=True, console=all_console)
        all_text = all_buffer.getvalue()
        assert {
            "authenticated": "Yes" in text,
            "method": "Service principal" in text,
            "provider": "Microsoft Entra ID" in text,
            "project": CDF_PROJECT in text,
            "capability": "assetsAcl" in text,
            "resource_without_io_suffix": "AssetIO" not in text and "Asset" in text,
            "current_project_only": "eventsAcl" not in text,
            "all_projects": "eventsAcl" in all_text,
            "scope": "all" in text,
            "summary_only_hint": "Run with --verbose" not in text,
        } == {
            "authenticated": True,
            "method": True,
            "provider": True,
            "project": True,
            "capability": True,
            "resource_without_io_suffix": True,
            "current_project_only": True,
            "all_projects": True,
            "scope": True,
            "summary_only_hint": True,
        }

    def test_resource_label_drops_io_suffix(self) -> None:
        assert {
            "asset": resource_label("AssetIO"),
            "view": resource_label("ViewIO"),
            "container": resource_label("ContainerCRUD"),
            "space": resource_label("SpaceCRUD"),
        } == {
            "asset": "Asset",
            "view": "View",
            "container": "ContainerCRUD",
            "space": "SpaceCRUD",
        }

    def test_data_modeling_resources_are_listed_once(self) -> None:
        capabilities = FlatCapabilities(
            {
                (DataModelsAcl, "dataModelsAcl", "READ"): AllScope(),
                (DataModelInstancesAcl, "dataModelInstancesAcl", "READ"): AllScope(),
            },
            name=CDF_PROJECT,
            groups=[1],
        )
        resources, _ = resources_from_capabilities(capabilities, "HYBRID")
        names = [item.io_name for item in resources]
        expected = ["ContainerIO", "DataModelIO", "EdgeIO", "NodeIO", "SpaceIO", "ViewIO"]
        assert {name: names.count(name) for name in expected} == {name: 1 for name in expected}
