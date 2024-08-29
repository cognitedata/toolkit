import contextlib

import pytest
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

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
        for location_filter in toolkit_client.location_filters.list():
            if location_filter.external_id.startswith("loc_ext_id_"):
                toolkit_client.location_filters.delete(location_filter.id)
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
        retrieved = toolkit_client.location_filters.retrieve(RUN_UNIQUE_ID)
        return retrieved
    except CogniteAPIError:
        created = toolkit_client.location_filters.create(location_filter)
        return created


class TestLocationFilterAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        location_filter = LocationFilterWrite(
            name="loc",
            external_id=SESSION_EXTERNAL_ID,
            parent_id=f"loc_par_{SESSION_EXTERNAL_ID}",
        )
        with contextlib.suppress(CogniteAPIError):  # Should be CogniteDuplicatedError, but API throws 500 ATM
            created = toolkit_client.location_filters.create(location_filter)
            assert isinstance(created, LocationFilter)
            assert created.as_write().dump() == location_filter.dump()

            retrieved = toolkit_client.location_filters.retrieve(created.id)
            assert isinstance(retrieved, LocationFilter)
            assert retrieved.as_write().dump() == location_filter.dump()

            toolkit_client.location_filters.delete(created.id)

            with pytest.raises(CogniteNotFoundError):
                toolkit_client.location_filters.retrieve(created.id)

    def test_list_location_filters(
        self, toolkit_client: ToolkitClient, existing_location_filter: LocationFilter
    ) -> None:
        location_filters = toolkit_client.location_filters.list()
        assert isinstance(location_filters, LocationFilterList)
        assert len(location_filters) > 0

    def test_iterate_location_filters(
        self, toolkit_client: ToolkitClient, existing_location_filter: LocationFilter
    ) -> None:
        for location_filters in toolkit_client.location_filters:
            assert isinstance(location_filters, LocationFilter)
            break
        else:
            pytest.fail("No location filters found")

    def test_update_location_filter(
        self, toolkit_client: ToolkitClient, existing_location_filter: LocationFilter
    ) -> None:
        update = existing_location_filter
        update.description = "New description"
        updated = toolkit_client.location_filters.update(update.id, update.as_write())
        assert updated.description == update.description
