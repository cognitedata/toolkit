from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId


class TestSimulatorsModels:
    def test_retrieve_unknown(self, toolkit_client: ToolkitClient, simulator: str) -> None:
        simulator_models = toolkit_client.tool.simulators.models.retrieve(
            [ExternalId(external_id=id) for id in ["unknown_model_1", "unknown_model_2"]], ignore_unknown_ids=True
        )
        assert len(simulator_models) == 0
