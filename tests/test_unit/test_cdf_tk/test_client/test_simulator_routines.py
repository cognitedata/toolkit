import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.request_classes.filters import SimulatorModelRoutineFilter


@pytest.mark.usefixtures("disable_gzip")
class TestSimulatorRoutinesAPI:
    def test_count_filters_by_kind(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/simulators/routines/aggregate")).mock(
            return_value=httpx2.Response(200, json={"items": [{"count": 2}]})
        )

        count = toolkit_client.simulators.routines.count(SimulatorModelRoutineFilter(kind="long"))

        assert (count, json.loads(route.calls[0].request.content)) == (
            2,
            {"aggregate": "count", "filter": {"kind": "long"}},
        )
