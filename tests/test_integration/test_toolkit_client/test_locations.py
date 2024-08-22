from __future__ import annotations

import contextlib

import pytest
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.locations import (
    LocationFilter,
    LocationFilterList,
    LocationFilterWrite,
)
from tests.test_integration.constants import RUN_UNIQUE_ID

DESCRIPTIONS = ["Original Description", "Updated Description"]


@pytest.fixture(scope="session")
def existing_location_filter(toolkit_client: ToolkitClient) -> LocationFilter:
    location_filter = LocationFilterWrite(
        name="loc",
        external_id=f"loc_ext_id_{RUN_UNIQUE_ID}",
    )
    try:
        retrieved = toolkit_client.locations.location_filters.retrieve(location_filter.external_id)
        return retrieved
    except CogniteNotFoundError:
        created = toolkit_client.locations.location_filters.create(location_filter)
        return created


class TestLocationFilterAPI:
    @pytest.fixture(scope="session")
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        location_filter = LocationFilterWrite(
            name="loc",
            external_id=f"loc_ext_id_{RUN_UNIQUE_ID}",
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

    @pytest.fixture(scope="session")
    @pytest.mark.usefixtures("existing_location_filter")
    def test_list_location_filters(self, toolkit_client: ToolkitClient) -> None:
        location_filters = toolkit_client.locations.location_filters.list()
        assert isinstance(location_filters, LocationFilterList)
        assert len(location_filters) > 0

    @pytest.fixture(scope="session")
    @pytest.mark.usefixtures("existing_location_filter")
    def test_iterate_location_filters(self, toolkit_client: ToolkitClient) -> None:
        for location_filters in toolkit_client.locations.location_filters:
            assert isinstance(location_filters, LocationFilter)
            break
        else:
            pytest.fail("No location filters found")

    @pytest.fixture(scope="session")
    def test_update_location_filter(
        self, toolkit_client: ToolkitClient, existing_location_filter: LocationFilter
    ) -> None:
        update = existing_location_filter
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_location_filter.description)
        updated = toolkit_client.locations.location_filters.update(update.as_write())
        assert updated.description == update.description
