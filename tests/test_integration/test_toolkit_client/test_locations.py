import pytest
from cognite.client.data_classes.data_modeling import DataModel, DataModelApply, Space, ViewId
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.location_filters import (
    LocationFilter,
    LocationFilterList,
    LocationFilterWrite,
)
from tests.test_integration.constants import RUN_UNIQUE_ID

SESSION_EXTERNAL_ID = f"loc_ext_id_{RUN_UNIQUE_ID}"


@pytest.fixture(scope="session", autouse=True)
def cleanup_before_session(toolkit_client: ToolkitClient) -> None:
    # Cleanup previously created locationfilters

    try:
        for location_filter in toolkit_client.search.locations.list():
            if location_filter.external_id.startswith("loc_ext_id_"):
                toolkit_client.search.locations.delete(location_filter.id)
    except CogniteAPIError:
        pass

    yield  #


@pytest.fixture(scope="session")
def existing_location_filter(toolkit_client: ToolkitClient) -> LocationFilter:
    location_filter = LocationFilterWrite(
        name="loc",
        external_id=SESSION_EXTERNAL_ID,
    )
    try:
        retrieved = toolkit_client.search.locations.retrieve(RUN_UNIQUE_ID)
        return retrieved
    except CogniteAPIError:
        created = toolkit_client.search.locations.create(location_filter)
        return created


@pytest.fixture(scope="session")
def my_data_model(toolkit_client: ToolkitClient, toolkit_space: Space) -> DataModel:
    data_model = toolkit_client.data_modeling.data_models.apply(
        DataModelApply(
            space=toolkit_space.space,
            external_id="LocationFilterTest",
            version="v1",
            views=[ViewId("cdf_cdm", "CogniteTimeSeries", "v1")],
        )
    )
    return data_model


class TestLocationFilterAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient, my_data_model: DataModel) -> None:
        location_filter = LocationFilterWrite(
            name="loc",
            external_id=SESSION_EXTERNAL_ID,
            data_models=[my_data_model.as_id()],
            data_modeling_type="DATA_MODELING_ONLY",
        )
        created: LocationFilter | None = None
        try:
            created = toolkit_client.search.locations.create(location_filter)
            assert isinstance(created, LocationFilter)
            assert created.as_write().dump() == location_filter.dump()
            assert created.data_modeling_type is not None

            retrieved = toolkit_client.search.locations.retrieve(created.id)
            assert isinstance(retrieved, LocationFilter)
            assert retrieved.as_write().dump() == location_filter.dump()

            toolkit_client.search.locations.delete(created.id)

            with pytest.raises(CogniteAPIError):
                toolkit_client.search.locations.retrieve(created.id)
        finally:
            if created:
                try:
                    toolkit_client.search.locations.delete(created.id)
                except CogniteAPIError:
                    pass

    def test_list_location_filters(
        self, toolkit_client: ToolkitClient, existing_location_filter: LocationFilter
    ) -> None:
        location_filters = toolkit_client.search.locations.list()
        assert isinstance(location_filters, LocationFilterList)
        assert len(location_filters) > 0

    def test_iterate_location_filters(
        self, toolkit_client: ToolkitClient, existing_location_filter: LocationFilter
    ) -> None:
        for location_filters in toolkit_client.search.locations:
            assert isinstance(location_filters, LocationFilter)
            break
        else:
            pytest.fail("No location filters found")

    def test_update_location_filter(
        self, toolkit_client: ToolkitClient, existing_location_filter: LocationFilter
    ) -> None:
        update = existing_location_filter
        update.description = "New description"
        updated = toolkit_client.search.locations.update(update.id, update.as_write())
        assert updated.description == update.description
