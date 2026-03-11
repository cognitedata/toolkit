from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.token import (
    InspectResponse,
)


class TestTokenAPI:
    def test_inspect(self, toolkit_client: ToolkitClient) -> None:
        token = toolkit_client.tool.token.inspect()
        token.to_project_capabilities()
        assert isinstance(token, InspectResponse)
