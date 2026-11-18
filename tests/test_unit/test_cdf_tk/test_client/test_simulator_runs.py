import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.simulator import SimulationRunRequest

_SIMULATION_RUN = {
    "id": 1,
    "dataSetId": 2,
    "simulatorExternalId": "sim",
    "modelExternalId": "model",
    "modelRevisionExternalId": "model_rev",
    "routineExternalId": "routine",
    "routineRevisionExternalId": "routine_rev",
    "status": "ready",
    "runType": "external",
    "userId": "user",
    "logId": 3,
    "createdTime": 0,
    "lastUpdatedTime": 0,
}


@pytest.mark.usefixtures("disable_gzip")
class TestSimulatorRunsAPI:
    def test_create(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/simulators/run")).mock(
            return_value=httpx2.Response(200, json={"items": [_SIMULATION_RUN]})
        )

        toolkit_client.simulators.runs.create([SimulationRunRequest(routine_external_id="routine")])

        assert json.loads(route.calls[0].request.content) == {"items": [{"routineExternalId": "routine"}]}
