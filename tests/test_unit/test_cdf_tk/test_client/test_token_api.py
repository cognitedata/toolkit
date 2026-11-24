import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.token import ToolkitTokenAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, AssetsAcl
from cognite_toolkit._cdf_tk.exceptions import SandboxUnavailableError


@pytest.fixture
def sandbox_config(toolkit_config: ToolkitClientConfig) -> ToolkitClientConfig:
    return ToolkitClientConfig(
        client_name=toolkit_config.client_name,
        project=toolkit_config.project,
        base_url=toolkit_config.base_url,
        credentials=toolkit_config.credentials,
        is_sandbox=True,
    )


class TestToolkitTokenAPISandbox:
    def test_verify_acls_and_scopes_raise_without_http_call(
        self, sandbox_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        token_api = ToolkitTokenAPI(HTTPClient(sandbox_config))

        with pytest.raises(SandboxUnavailableError):
            token_api.verify_acls([AssetsAcl(actions=["READ"], scope=AllScope())])
        with pytest.raises(SandboxUnavailableError):
            token_api.check_available_scopes(AssetsAcl, ["READ"])
        assert len(respx_mock.calls) == 0

    def test_inspect_raises(self, sandbox_config: ToolkitClientConfig) -> None:
        with pytest.raises(SandboxUnavailableError):
            ToolkitTokenAPI(HTTPClient(sandbox_config)).inspect()
