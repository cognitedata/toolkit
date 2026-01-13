import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse


@pytest.fixture()
def two_3d_models(toolkit_client: ToolkitClient) -> None:
    client = toolkit_client
    models = client.three_d.models.list(limit=2)
    if len(models) == 0:
        client.three_d.models.create(
            name="integration_test_3d_model",
            data_set_id=None,
            metadata={"source": "integration_test"},
        )
    if len(models) == 1:
        client.three_d.models.create(
            name="integration_test_3d_model_2",
            data_set_id=None,
            metadata={"source": "integration_test"},
        )


class Test3DAPI:
    @pytest.mark.usefixtures("two_3d_models")
    def test_iterate(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client
        response = client.tool.three_d.models.iterate(limit=1, include_revision_info=True)
        response2 = client.tool.three_d.models.iterate(limit=1, include_revision_info=True, cursor=response.next_cursor)
        assert isinstance(response, PagedResponse)
        assert isinstance(response2, PagedResponse)
        assert response != response2

        items = response.items + response2.items
        assert 0 < len(items) <= 2
        missing_revision_info = [item for item in items if item.last_revision_info is None]
        assert len(missing_revision_info) == 0

    @pytest.mark.usefixtures("two_3d_models")
    def test_list(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client
        models = client.tool.three_d.models.list(limit=2, include_revision_info=False)
        assert 0 < len(models) <= 2
        missing_revision_info = [model for model in models if model.last_revision_info is not None]
        assert len(missing_revision_info) == 0
