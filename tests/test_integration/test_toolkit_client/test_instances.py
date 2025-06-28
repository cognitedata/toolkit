import pytest
from cognite.client.data_classes.data_modeling import Space
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteTimeSeriesApply

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstancesApplyResultList
from tests.test_integration.constants import RUN_UNIQUE_ID


@pytest.fixture
def toolkit_client_create_limit_1(toolkit_client: ToolkitClient) -> ToolkitClient:
    before = toolkit_client.data_modeling.instances._CREATE_LIMIT
    toolkit_client.data_modeling.instances._CREATE_LIMIT = 1
    yield toolkit_client
    toolkit_client.data_modeling.instances._CREATE_LIMIT = before


@pytest.fixture
def toolkit_client_delete_limit_1(toolkit_client: ToolkitClient) -> ToolkitClient:
    before = toolkit_client.data_modeling.instances._DELETE_LIMIT
    toolkit_client.data_modeling.instances._DELETE_LIMIT = 1
    yield toolkit_client
    toolkit_client.data_modeling.instances._DELETE_LIMIT = before


class TestExtendedInstancesAPI:
    @pytest.mark.usefixtures("toolkit_client_delete_limit_1")
    def test_apply_delete_fast_instances(
        self, toolkit_client_create_limit_1: ToolkitClient, toolkit_space: Space
    ) -> None:
        client = toolkit_client_create_limit_1
        ts = [
            CogniteTimeSeriesApply(
                space=toolkit_space.space,
                external_id=f"toolkit_test_apply_fast_{i}_{RUN_UNIQUE_ID}",
                is_step=False,
                time_series_type="numeric",
                name=f"Toolkit Test Time Series {i}",
            )
            for i in range(4)
        ]
        created: InstancesApplyResultList | None = None
        has_deleted: bool = False
        try:
            created = client.data_modeling.instances.apply_fast(ts)
            assert len(created) == 4

            deleted = client.data_modeling.instances.delete_fast(created.as_ids())

            assert len(deleted) == 4
            has_deleted = True
        finally:
            if created is not None and not has_deleted:
                client.data_modeling.instances.delete(created.as_ids())
