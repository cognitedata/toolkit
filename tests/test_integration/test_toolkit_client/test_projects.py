from cognite_toolkit._cdf_tk.client import ToolkitClient


class TestProjectAPI:
    def test_status(self, toolkit_client: ToolkitClient) -> None:
        info = toolkit_client.project.status()
        assert info.data_modeling_status == "HYBRID"
