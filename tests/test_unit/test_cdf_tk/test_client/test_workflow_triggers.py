import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


@pytest.mark.usefixtures("disable_gzip")
class TestWorkflowTriggersAPI:
    def test_history(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.get(toolkit_config.create_api_url("/workflows/triggers/my_trigger/history")).mock(
            return_value=httpx2.Response(200, json={"items": []})
        )

        toolkit_client.workflows.triggers.history(ExternalId(external_id="my_trigger"), limit=5)

        assert dict(route.calls[0].request.url.params) == {"limit": "5"}

    def test_history_non_positive_limit_skips_request(
        self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter
    ) -> None:
        result = toolkit_client.workflows.triggers.history(ExternalId(external_id="my_trigger"), limit=0)

        assert (result, len(respx_mock.calls)) == ([], 0)
