import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

_UNIT = {
    "externalId": "temperature:deg_c",
    "name": "DEG_C",
    "longName": "degree Celsius",
    "symbol": "°C",
    "aliasNames": ["C"],
    "quantity": "Temperature",
    "conversion": {"multiplier": 1.0, "offset": 273.15},
}


@pytest.mark.usefixtures("disable_gzip")
class TestUnitsWorkflowsTransformations:
    def test_units_retrieve(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/units/byids")).mock(
            return_value=httpx2.Response(200, json={"items": [_UNIT]})
        )

        toolkit_client.units.retrieve([ExternalId(external_id="temperature:deg_c")], ignore_unknown_ids=True)

        assert json.loads(route.calls[0].request.content) == {
            "items": [{"externalId": "temperature:deg_c"}],
            "ignoreUnknownIds": True,
        }

    def test_workflow_tasks_update(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/workflows/tasks/task-1/update")).mock(
            return_value=httpx2.Response(200, json={"id": "task-1", "externalId": "my_task", "status": "COMPLETED"})
        )

        toolkit_client.workflows.tasks.update("task-1", "COMPLETED", output={"answer": 42})

        assert json.loads(route.calls[0].request.content) == {"status": "COMPLETED", "output": {"answer": 42}}

    def test_workflow_triggers_history(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.get(toolkit_config.create_api_url("/workflows/triggers/my_trigger/history")).mock(
            return_value=httpx2.Response(200, json={"items": []})
        )

        toolkit_client.workflows.triggers.history(ExternalId(external_id="my_trigger"), limit=5)

        assert dict(route.calls[0].request.url.params) == {"limit": "5"}

    def test_transformations_cancel(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/transformations/cancel")).mock(
            return_value=httpx2.Response(200, json={})
        )

        toolkit_client.transformations.cancel(ExternalId(external_id="my_transformation"))

        assert json.loads(route.calls[0].request.content) == {"externalId": "my_transformation"}
