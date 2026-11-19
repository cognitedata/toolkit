import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig


@pytest.mark.usefixtures("disable_gzip")
class TestAssetsAPI:
    def test_search_sends_filter_and_search(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/assets/search")).mock(
            return_value=httpx2.Response(200, json={"items": []})
        )

        toolkit_client.assets.search("pump", filter={"externalIdPrefix": "my_"}, limit=10)

        assert json.loads(route.calls[0].request.content) == {
            "filter": {"externalIdPrefix": "my_"},
            "search": {"query": "pump"},
            "limit": 10,
        }

    def test_search_rejects_limit_above_max(self, toolkit_client: ToolkitClient) -> None:
        with pytest.raises(ValueError, match="Limit must be between 1 and 1000"):
            toolkit_client.assets.search("pump", limit=1001)
