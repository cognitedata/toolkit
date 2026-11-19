import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig


@pytest.mark.usefixtures("disable_gzip")
class TestSimulatorModelsAPI:
    def test_count(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/simulators/models/aggregate")).mock(
            return_value=httpx2.Response(200, json={"items": [{"count": 4}]})
        )

        count = toolkit_client.simulators.models.count()

        assert (count, json.loads(route.calls[0].request.content)) == (4, {"aggregate": "count"})
