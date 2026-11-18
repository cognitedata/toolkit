import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig


@pytest.mark.usefixtures("disable_gzip")
class TestDocumentsAPI:
    def test_search_passages_sends_default_expansion_strategy(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/documents/passages/search")).mock(
            return_value=httpx2.Response(200, json={"items": []})
        )

        toolkit_client.documents.search_passages({"semanticSearch": {"property": ["content"], "value": "pump"}})

        assert json.loads(route.calls[0].request.content) == {
            "filter": {"semanticSearch": {"property": ["content"], "value": "pump"}},
            "expansionStrategy": {"strategy": "symmetric", "chunk_count": 1},
            "limit": 10,
        }
