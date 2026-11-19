import json
from typing import Any

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.request_classes.filters import SimulationRunSort
from cognite_toolkit._cdf_tk.client.resource_classes.simulator import (
    SimulationRunByRoutineRequest,
    SimulationRunByRoutineRevisionRequest,
    SimulationRunRequest,
    SimulationRunRequestAdapter,
)

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
    @pytest.mark.parametrize(
        "request_item, expected_body",
        [
            pytest.param(
                SimulationRunByRoutineRequest(routine_external_id="routine"),
                {"routineExternalId": "routine"},
                id="by routine",
            ),
            pytest.param(
                SimulationRunByRoutineRevisionRequest(
                    routine_revision_external_id="routine_rev", model_revision_external_id="model_rev"
                ),
                {"routineRevisionExternalId": "routine_rev", "modelRevisionExternalId": "model_rev"},
                id="by routine revision",
            ),
        ],
    )
    def test_create(
        self,
        request_item: SimulationRunRequest,
        expected_body: dict[str, Any],
        toolkit_client: ToolkitClient,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/simulators/run")).mock(
            return_value=httpx2.Response(200, json={"items": [_SIMULATION_RUN]})
        )

        toolkit_client.simulators.runs.create([request_item])

        assert json.loads(route.calls[0].request.content) == {"items": [expected_body]}

    def test_list_with_sort(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/simulators/runs/list")).mock(
            return_value=httpx2.Response(200, json={"items": [_SIMULATION_RUN]})
        )

        toolkit_client.simulators.runs.list(limit=10, sort=SimulationRunSort(property="simulationTime", order="desc"))

        assert json.loads(route.calls[0].request.content) == {
            "limit": 10,
            "sort": [{"property": "simulationTime", "order": "desc"}],
        }


class TestSimulationRunRequestAdapter:
    @pytest.mark.parametrize(
        "data, expected_type",
        [
            pytest.param({"routineExternalId": "routine"}, SimulationRunByRoutineRequest, id="by routine"),
            pytest.param(
                {"routineRevisionExternalId": "routine_rev", "modelRevisionExternalId": "model_rev"},
                SimulationRunByRoutineRevisionRequest,
                id="by routine revision",
            ),
        ],
    )
    def test_validate_selects_request_type(self, data: dict[str, Any], expected_type: type) -> None:
        assert type(SimulationRunRequestAdapter.validate_python(data)) is expected_type
