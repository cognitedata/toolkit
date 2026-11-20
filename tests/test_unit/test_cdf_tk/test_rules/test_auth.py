from pathlib import Path
from unittest.mock import MagicMock

import pytest
import respx
import yaml

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.resource_ios import WorkflowTriggerIO
from cognite_toolkit._cdf_tk.rules import CredentialsCheck

IDP_URL = "https://login.microsoftonline.com/my-tenant/oauth2/v2.0/token"
INSPECT_URL = "https://bluefield.cognitedata.com/api/v1/token/inspect"


@pytest.fixture
def environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ["CDF_URL", "LOGIN_FLOW", "IDP_TOKEN_URL", "IDP_AUDIENCE", "IDP_SCOPES", "IDP_CLIENT_ID"]:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CDF_CLUSTER", "bluefield")
    monkeypatch.setenv("CDF_PROJECT", "my_project")
    monkeypatch.setenv("PROVIDER", "entra_id")
    monkeypatch.setenv("IDP_TENANT_ID", "my-tenant")
    monkeypatch.setenv("TRIGGER_CLIENT_SECRET", "my-secret")
    monkeypatch.delenv("UNSET_CLIENT_SECRET", raising=False)


def _workflow_trigger(tmp_path: Path, client_secret: str) -> BuiltResource:
    filepath = tmp_path / "workflows" / "my.WorkflowTrigger.yaml"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    content = {
        "externalId": "my_trigger",
        "triggerRule": {"triggerType": "schedule", "cronExpression": "0 0 * * *"},
        "workflowExternalId": "my_workflow",
        "workflowVersion": "v1",
        "authentication": {"clientId": "my-client", "clientSecret": client_secret},
    }
    filepath.write_text(yaml.safe_dump(content))
    return BuiltResource(
        identifier=ExternalId(external_id="my_trigger"),
        source_hash="test-hash",
        type=WorkflowTriggerIO.as_resource_type(),
        source_path=filepath.resolve(),
        build_path=filepath.resolve(),
        crud_cls=WorkflowTriggerIO,
        has_syntax_error=False,
        module_id=ModuleId(id=Path("modules/my"), path=filepath.parent.resolve()),
    )


@pytest.mark.usefixtures("environment")
class TestCredentialsCheck:
    @pytest.mark.parametrize(
        "idp_response, inspect_status, expected_codes",
        [
            pytest.param({"access_token": "abc", "expires_in": 3600}, 200, [], id="valid"),
            pytest.param(
                {"error": "invalid_client", "error_description": "Invalid client secret."},
                200,
                ["INVALID-CREDENTIALS"],
                id="rejected-by-identity-provider",
            ),
            pytest.param(
                {"access_token": "abc", "expires_in": 3600}, 401, ["INVALID-CREDENTIALS"], id="rejected-by-cdf"
            ),
        ],
    )
    def test_validate(
        self,
        tmp_path: Path,
        respx_mock: respx.MockRouter,
        idp_response: dict[str, object],
        inspect_status: int,
        expected_codes: list[str],
    ) -> None:
        respx_mock.post(IDP_URL).respond(json=idp_response)
        respx_mock.get(INSPECT_URL).respond(
            status_code=inspect_status,
            json={"subject": "me", "projects": [], "capabilities": []}
            if inspect_status == 200
            else {"error": {"code": 401, "message": "Unauthorized"}},
        )
        rule = CredentialsCheck([_workflow_trigger(tmp_path, "${TRIGGER_CLIENT_SECRET}")], client=MagicMock())

        insights = list(rule.validate())

        assert [insight.code for insight in insights] == expected_codes

    def test_unset_environment_variable_is_not_verified(self, tmp_path: Path, respx_mock: respx.MockRouter) -> None:
        rule = CredentialsCheck([_workflow_trigger(tmp_path, "${UNSET_CLIENT_SECRET}")], client=MagicMock())

        insights = list(rule.validate())

        assert [(insight.code, respx_mock.calls.call_count) for insight in insights] == [("UNVERIFIED-CREDENTIALS", 0)]
