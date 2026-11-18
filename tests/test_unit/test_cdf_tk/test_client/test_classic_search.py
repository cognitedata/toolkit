import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


@pytest.mark.usefixtures("disable_gzip")
class TestClassicSearchAndCount:
    def test_assets_search_sends_filter_and_search(
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

    def test_assets_search_rejects_limit_above_max(self, toolkit_client: ToolkitClient) -> None:
        with pytest.raises(ValueError, match="Limit must be between 1 and 1000"):
            toolkit_client.assets.search("pump", limit=1001)

    def test_datasets_count(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/datasets/aggregate")).mock(
            return_value=httpx2.Response(200, json={"items": [{"count": 4}]})
        )

        count = toolkit_client.datasets.count(external_id_prefix="ds_")

        assert (count, json.loads(route.calls[0].request.content)) == (4, {"filter": {"externalIdPrefix": "ds_"}})

    def test_filemetadata_icon(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.get(toolkit_config.create_api_url("/files/icon")).mock(
            return_value=httpx2.Response(200, content=b"image")
        )

        toolkit_client.filemetadata.icon(ExternalId(external_id="my_image"))

        assert dict(route.calls[0].request.url.params) == {"externalId": "my_image"}
