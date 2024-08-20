from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from typing import Any, overload

from cognite.client._api_client import APIClient

from cognite_toolkit._cdf_tk.client.data_classes.locations import (
    LocationFilter,
    LocationFilterList,
    LocationFilterWrite,
)


class LocationFiltersAPI(APIClient):
    _RESOURCE_PATH = f"/apps/v1/projects/{os.environ.get('CDF_PROJECT')}/storage/config/locationfilters"

    @overload
    def __call__(self) -> Iterator[LocationFilter]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[LocationFilterList]: ...

    def __call__(self, chunk_size: int | None = None) -> Iterator[LocationFilter] | Iterator[LocationFilterList]:
        """Iterate over locationfilters.

        Args:
            chunk_size: The number of locationfilters to return in each chunk. None will return all locationfilters.

        Yields:
            LocationFilter or LocationFilterList

        """
        return self._list_generator(
            method="GET", resource_cls=LocationFilter, list_cls=LocationFilterList, chunk_size=chunk_size
        )

    def __iter__(self) -> Iterator[LocationFilter]:
        return self.__call__()

    @overload
    def create(self, location_filter: LocationFilterWrite) -> LocationFilter: ...

    @overload
    def create(self, location_filter: Sequence[LocationFilterWrite]) -> LocationFilterList: ...

    def create(
        self, location_filter: LocationFilterWrite | Sequence[LocationFilterWrite]
    ) -> LocationFilter | LocationFilterList:
        """Create a new LocationFilter.

        Args:
            LocationFilter: LocationFilterWrite or list of LocationFilterWrite.

        Returns:
            LocationFilter object.

        """
        return self._create_multiple(
            list_cls=LocationFilterList,
            resource_cls=LocationFilter,
            items=location_filter,
            input_resource_cls=LocationFilterWrite,
        )

    def retrieve(self, id: int | Sequence[int]) -> LocationFilterList:
        """Retrieve a LocationFilter.

        Args:
            data_set_id: Data set id of the LocationFilter.

        Returns:
            LocationFilter object.

        """
        body = self._create_body(id)
        res = self._post(url_path=self._RESOURCE_PATH + "/byids", json=body)
        return LocationFilterList._load(res.json()["items"], cognite_client=self._cognite_client)

    @staticmethod
    def _create_body(data_set_id: int | Sequence[int]) -> dict:
        ids = [data_set_id] if isinstance(data_set_id, int) else data_set_id
        body = {"items": [{"dataSetId": external_id} for external_id in ids]}
        return body

    @overload
    def update(self, location_filter: LocationFilterWrite) -> LocationFilter: ...

    @overload
    def update(self, location_filter: Sequence[LocationFilterWrite]) -> LocationFilterList: ...

    def update(
        self, location_filter: LocationFilterWrite | Sequence[LocationFilterWrite]
    ) -> LocationFilter | LocationFilterList:
        """Update a LocationFilter.

        Args:
            LocationFilter: LocationFilterWrite or list of LocationFilterWrite.

        Returns:
            LocationFilter object.

        """
        is_single = False
        if isinstance(location_filter, LocationFilterWrite):
            location_filter = [location_filter]
            is_single = True
        elif isinstance(location_filter, Sequence):
            location_filter = list(location_filter)
        else:
            raise ValueError("LocationFilter must be a LocationFilterWrite or a list of LocationFilterWrite")

        # property_spec = LocationFilterUpdate._get_update_properties()
        # update = [
        #     {"dataSetId": r.data_set_id, **self._convert_resource_to_patch_object(r, property_spec)}
        #     for r in LocationFilters
        # ]
        update: dict[str, Any] = {}
        res = self._post(url_path=self._RESOURCE_PATH + "/update", json={"items": update})
        loaded = LocationFilterList._load(res.json()["items"], cognite_client=self._cognite_client)
        return loaded[0] if is_single else loaded

    def delete(self, data_set_id: int | Sequence[int]) -> None:
        """Delete a LocationFilter.

        Args:
            data_set_id: Data set id of the LocationFilter.

        Returns:
            None

        """
        body = self._create_body(data_set_id)
        self._post(url_path=self._RESOURCE_PATH + "/delete", json=body)

    def list(self) -> LocationFilterList:
        """List LocationFilters.

        Returns:
            LocationFilterList

        """
        return self._list(method="GET", resource_cls=LocationFilter, list_cls=LocationFilterList)
