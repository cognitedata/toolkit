from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig
from cognite.client.exceptions import CogniteNotFoundError

from cognite_toolkit._cdf_tk.client.data_classes.locations import (
    LocationFilter,
    LocationFilterList,
    LocationFilterWrite,
)


class LocationFiltersAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        # this is a very temporary fix to avoid the client concatinating the _RESOURCE_PATH with the ordinary /api/ path.
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

    def external_id_to_id_dict(self) -> dict[str, int]:
        return {loc.external_id: loc.id for loc in self.list()}

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
        created = []
        location_filters = location_filter if isinstance(location_filter, Sequence) else [location_filter]
        for location_filter in location_filters:
            payload = location_filter.dump(camel_case=True)
            res = self._post(url_path=self._RESOURCE_PATH, json=payload)
            created.append(LocationFilter._load(res.json(), cognite_client=self._cognite_client))

        if len(created) > 0:
            if isinstance(location_filter, Sequence):
                return LocationFilterList(created)
            else:
                return created[0]

        return LocationFilterList([])

    def retrieve(self, id: int | Sequence[int]) -> LocationFilterList:
        """Retrieve a LocationFilter.

        Args:
            id: id of the LocationFilter.

        Returns:
            LocationFilter object.

        """

        if isinstance(id, Sequence):
            retrieved = []
            for x in id:
                try:
                    retrieved.append(self.retrieve(x))
                except CogniteNotFoundError:
                    continue
            return LocationFilterList(retrieved)

        res = self._get(url_path=self._RESOURCE_PATH + "/{id}")
        return LocationFilterList._load(res.json()["items"], cognite_client=self._cognite_client)

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

        location_filters = location_filter if isinstance(location_filter, Sequence) else [location_filter]
        updated = []
        for location_filter in location_filters:
            payload = location_filter.dump(camel_case=True)
            res = self._put(
                url_path=self._RESOURCE_PATH + f"/{self.external_id_to_id_dict()[location_filter.external_id]}",
                json=payload,
            )
            loc = LocationFilter._load(res.json(), cognite_client=self._cognite_client)
            updated.append(loc)

        return LocationFilterList(updated)

        # update: dict[str, Any] = {}
        # res = self._post(url_path=self._RESOURCE_PATH + "/update", json={"items": update})
        # loaded = LocationFilterList._load(res.json()["items"], cognite_client=self._cognite_client)
        # return loaded[0] if is_single else loaded

    def delete(self, id: int | Sequence[int]) -> None:
        """Delete a LocationFilter.

             Args:
        #         id: id LocationFilter.
        #     Returns:
        #         None
        #"""
        self._delete(url_path=self._RESOURCE_PATH + f"/{id}")

    def list(self) -> LocationFilterList:
        """List LocationFilters.

        Returns:
            LocationFilterList

        """
        res = self._post(url_path=self._RESOURCE_PATH + "/list", json={"flat": False})
        return LocationFilterList._load(res.json()["items"], cognite_client=self._cognite_client)
