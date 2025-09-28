from cognite_toolkit._cdf_tk.client import ToolkitClient


class TestProjectAPI:
    def test_status(self, toolkit_client: ToolkitClient) -> None:
        result = toolkit_client.project.status()
        assert result.this_project.url_name == toolkit_client.config.project
        assert result.this_project.data_modeling_status in ("HYBRID", "DATA_MODELING_ONLY")
