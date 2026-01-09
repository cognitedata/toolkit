import pytest
from cognite.client.data_classes import DataSet

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.simulator_model import SimulatorModelRequest


@pytest.fixture
def simulator(toolkit_client: ToolkitClient) -> str:
    simulator_external_id = "smoke_test_simulator"

    return simulator_external_id


def test_create_retrieve_update_delete(
    simulator_external_id: str, smoke_dataset: DataSet, toolkit_client: ToolkitClient
) -> None:
    request = SimulatorModelRequest(
        external_id="smoke_test_simulator_model",
        simulator_external_id=simulator_external_id,
        name="Smoke Test Simulator Model",
        data_set_id=smoke_dataset.id,
        type="SteadyState",
    )
    try:
        created = toolkit_client.tool.simulators.models.create([request])
        assert len(created) == 1
        assert created[0].as_request_resource().dump() == request.dump()

        retrieved = toolkit_client.tool.simulators.models.retrieve([request.as_id()])
        assert len(retrieved) == 1
        assert retrieved[0].dump() == created[0].dump()

        update_request = (
            created[0]
            .as_request_resource()
            .model_copy(update={"id": created[0].id, "description": "Updated description"})
        )
        updated = toolkit_client.tool.simulators.models.update([update_request])
        assert len(updated) == 1
        assert updated[0].description == "Updated description"
    finally:
        toolkit_client.tool.simulators.models.delete([request.as_id()])
