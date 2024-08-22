from __future__ import annotations

from collections.abc import Iterator, Sequence
from functools import lru_cache
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

    @property
    @lru_cache(maxsize=1)
    def _ids(self) -> dict[str, int]:
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
            LocationFilter or LocationFilterList

        """

        if isinstance(location_filter, LocationFilterWrite):
            res = self._post(url_path=self._RESOURCE_PATH, json=location_filter.dump())
            return LocationFilter._load(res.json(), cognite_client=self._cognite_client)

        return LocationFilterList(
            [
                LocationFilter._load(
                    self._post(url_path=self._RESOURCE_PATH, json=item.dump()).json(),
                    cognite_client=self._cognite_client,
                )
                for item in location_filter
            ]
        )

    def retrieve(self, external_id: str) -> LocationFilter | None:
        for loc in self.list():
            if loc.external_id == external_id:
                return loc
        raise CogniteNotFoundError(not_found=[external_id])

    @overload
    def update(self, location_filter: LocationFilterWrite) -> LocationFilter: ...

    @overload
    def update(self, location_filter: Sequence[LocationFilterWrite]) -> LocationFilterList: ...

    def update(
        self, location_filter: LocationFilterWrite | Sequence[LocationFilterWrite]
    ) -> LocationFilter | LocationFilterList:
        if isinstance(location_filter, LocationFilterWrite):
            res = self._put(
                url_path=f"{self._RESOURCE_PATH}/{self._ids[location_filter.external_id]}",
                json=location_filter.dump(),
            )
            return LocationFilter._load(res.json(), cognite_client=self._cognite_client)

        return LocationFilterList(
            [
                LocationFilter._load(
                    self._put(url_path=f"{self._RESOURCE_PATH}/{self._ids[item.external_id]}", json=item.dump()).json(),
                    cognite_client=self._cognite_client,
                )
                for item in location_filter
            ]
        )

    def delete(self, ids: int | Sequence[int]) -> int:
        if isinstance(ids, int):
            ids = [ids]

        n = 0
        for id in ids:
            self._delete(url_path=f"{self._RESOURCE_PATH}/{id}")
            n += 1
        return n

    def list(self) -> LocationFilterList:
        res = self._post(url_path=self._RESOURCE_PATH + "/list", json={"flat": False})
        return LocationFilterList._load(res.json()["items"], cognite_client=self._cognite_client)
