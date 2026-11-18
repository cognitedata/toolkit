import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig


@pytest.mark.usefixtures("disable_gzip")
class TestRecordsAPI:
    def test_aggregate(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/streams/my_stream/records/aggregate")).mock(
            return_value=httpx2.Response(200, json={"aggregates": {"total": {"count": 5}}})
        )

        toolkit_client.records.aggregate("my_stream", {"total": {"count": {}}}, last_updated_time={"gte": "2d-ago"})

        assert json.loads(route.calls[0].request.content) == {
            "aggregates": {"total": {"count": {}}},
            "lastUpdatedTime": {"gte": "2d-ago"},
        }
