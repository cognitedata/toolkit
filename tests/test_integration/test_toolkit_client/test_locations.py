import contextlib

import pytest
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.locations import (
    LocationFilter,
    LocationFilterList,
    LocationFilterWrite,
)


@pytest.fixture(scope="session")
def existing_location_filter(toolkit_client: ToolkitClient) -> LocationFilter:
    location_filter = LocationFilterWrite(
        name="loc",
        external_id="loc_ext_id",
    )
    try:
        retrieved = toolkit_client.locations.location_filters.retrieve(location_filter.external_id)
        return retrieved
    except CogniteNotFoundError:
        created = toolkit_client.locations.location_filters.create(location_filter)
        return created


class TestLocationFilterAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        location_filter = LocationFilterWrite(
            name="loc",
            external_id="loc_ext_id",
        )
        try:
            with contextlib.suppress(CogniteAPIError):  # Should be CogniteDuplicatedError, but API throws 500 ATM
                created = toolkit_client.locations.location_filters.create(location_filter)
                assert isinstance(created, LocationFilter)
                assert created.as_write().dump() == location_filter.dump()

            retrieved = toolkit_client.locations.location_filters.retrieve(location_filter.external_id)

            assert isinstance(retrieved, LocationFilter)
            assert retrieved.as_write().dump() == location_filter.dump()

        finally:
            toolkit_client.locations.location_filters.delete(retrieved.id)

        with pytest.raises(CogniteNotFoundError):
            toolkit_client.locations.location_filters.retrieve(location_filter.external_id)

    def test_list_location_filters(
        self, toolkit_client: ToolkitClient, existing_location_filter: LocationFilter
    ) -> None:
        location_filters = toolkit_client.locations.location_filters.list()
        assert isinstance(location_filters, LocationFilterList)
        assert len(location_filters) > 0

    def test_iterate_location_filters(
        self, toolkit_client: ToolkitClient, existing_location_filter: LocationFilter
    ) -> None:
        for location_filters in toolkit_client.locations.location_filters:
            assert isinstance(location_filters, LocationFilter)
            break
        else:
            pytest.fail("No location filters found")

    def test_update_location_filter(
        self, toolkit_client: ToolkitClient, existing_location_filter: LocationFilter
    ) -> None:
        update = existing_location_filter.as_write()
        original_description = update.description
        update.description = "New description"
        updated = toolkit_client.locations.location_filters.update(update.as_write())
        assert updated.description == update.description
        # resetting
        update.description = original_description
        toolkit_client.locations.location_filters.update(update.as_write())
