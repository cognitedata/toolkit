import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.api_classes import PagedResponse


@pytest.fixture()
def a_3d_model(toolkit_client: ToolkitClient) -> None:
    client = toolkit_client
    models = client.three_d.models.list(1)
    if len(models) == 0:
        client.three_d.models.create(
            name="integration_test_3d_model",
            data_set_id=None,
            metadata={"source": "integration_test"},
        )


class Test3DAPI:
    @pytest.mark.usefixtures("a_3d_model")
    def test_iterate(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client
        response = client.tool.three_d.models.iterate(limit=2)

        assert isinstance(response, PagedResponse)
        assert 0 < len(response.items) <= 2
