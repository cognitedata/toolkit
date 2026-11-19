import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig


@pytest.mark.usefixtures("disable_gzip")
class TestExtractionPipelineRunsAPI:
    def test_list(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/extpipes/runs/list")).mock(
            return_value=httpx2.Response(200, json={"items": [{"id": 1, "status": "failure", "createdTime": 0}]})
        )

        toolkit_client.extraction_pipelines.runs.list(
            "my_pipeline", statuses=["failure"], message_substring="timeout", limit=10
        )

        assert json.loads(route.calls[0].request.content) == {
            "filter": {"externalId": "my_pipeline", "statuses": ["failure"], "message": {"substring": "timeout"}},
            "limit": 10,
        }
