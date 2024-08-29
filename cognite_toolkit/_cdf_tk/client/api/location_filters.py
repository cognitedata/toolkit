from __future__ import annotations

from collections.abc import Iterator
from typing import overload

from cognite.client import CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig

from cognite_toolkit._cdf_tk.client.data_classes.location_filters import (
    LocationFilter,
    LocationFilterList,
    LocationFilterWrite,
)


class LocationFiltersAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        # this is hopefully a very temporary fix to avoid the client concatinating the _RESOURCE_PATH with the ordinary /api/ path.
        self._api_version = None
        self._RESOURCE_PATH = f"/apps/v1/projects/{self._cognite_client.config.project}/storage/config/locationfilters"

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
        return iter(self.list())

    def __iter__(self) -> Iterator[LocationFilter]:
        return self.__call__()

    def create(self, location_filter: LocationFilterWrite) -> LocationFilter:
        """Create a new LocationFilter.

        Args:
            location_filter: LocationFilterWrite


        Returns:
            LocationFilter

        """

        res = self._post(url_path=self._RESOURCE_PATH, json=location_filter.dump())
        return LocationFilter._load(res.json(), cognite_client=self._cognite_client)

    def retrieve(self, id: int) -> LocationFilter:
        """Retrieve a single LocationFilter.

        Args:
            id: The ID of the LocationFilter

        Returns:
            LocationFilter
        """

        res = self._get(url_path=f"{self._RESOURCE_PATH}/{id}")
        return LocationFilter._load(res.json(), cognite_client=self._cognite_client)

    def update(self, id: int, location_filter_update: LocationFilterWrite) -> LocationFilter:
        """Update a new LocationFilter.

        Args:
            id: The ID of the LocationFilter
            location_filter_content: The content of the LocationFilter to update

        Returns:
            LocationFilter

        """

        res = self._put(
            url_path=f"{self._RESOURCE_PATH}/{id}",
            json=location_filter_update.dump(),
        )
        return LocationFilter._load(res.json(), cognite_client=self._cognite_client)

    def delete(self, id: int) -> None:
        self._delete(url_path=f"{self._RESOURCE_PATH}/{id}")

    def list(self) -> LocationFilterList:
        res = self._post(url_path=self._RESOURCE_PATH + "/list", json={"flat": False})
        return LocationFilterList._load(res.json()["items"], cognite_client=self._cognite_client)
