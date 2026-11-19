import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig


@pytest.mark.usefixtures("disable_gzip")
class TestDataSetsAPI:
    def test_count(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/datasets/aggregate")).mock(
            return_value=httpx2.Response(200, json={"items": [{"count": 4}]})
        )

        count = toolkit_client.datasets.count(external_id_prefix="ds_")

        assert (count, json.loads(route.calls[0].request.content)) == (4, {"filter": {"externalIdPrefix": "ds_"}})
