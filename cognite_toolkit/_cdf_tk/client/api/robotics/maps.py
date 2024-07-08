from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
    Map,
    MapList,
    MapWrite,
    _MapUpdate,
)

from .utlis import tmp_disable_gzip


class MapsAPI(APIClient):
    _RESOURCE_PATH = "/robotics/maps"

    @overload
    def __call__(self) -> Iterator[Map]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[MapList]: ...

    def __call__(self, chunk_size: int | None = None) -> Iterator[Map] | Iterator[MapList]:
        """Iterate over robot maps.

        Args:
            chunk_size: The number of robot maps to return in each chunk. None will return all robot maps.

        Yields:
            Map or MapList

        """
        return self._list_generator(method="GET", resource_cls=Map, list_cls=MapList, chunk_size=chunk_size)

    def __iter__(self) -> Iterator[Map]:
        return self.__call__()

    @overload
    def create(self, map: MapWrite) -> Map: ...

    @overload
    def create(self, map: Sequence[MapWrite]) -> MapList: ...

    def create(self, map: MapWrite | Sequence[MapWrite]) -> Map | MapList:
        """Create a new robot map.

        Args:
            map: MapWrite or list of MapWrite.

        Returns:
            Map object.

        """
        with tmp_disable_gzip():
            return self._create_multiple(
                list_cls=MapList,
                resource_cls=Map,
                items=map,
                input_resource_cls=MapWrite,
            )

    @overload
    def retrieve(self, external_id: str) -> Map | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> MapList: ...

    def retrieve(self, external_id: str | SequenceNotStr[str]) -> Map | None | MapList:
        """Retrieve a robot map.

        Args:
            external_id: External id of the robot map.

        Returns:
            Map object.

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            return self._retrieve_multiple(
                identifiers=identifiers,
                resource_cls=Map,
                list_cls=MapList,
            )

    @overload
    def update(self, map: MapWrite) -> Map: ...

    @overload
    def update(self, map: Sequence[MapWrite]) -> MapList: ...

    def update(self, map: MapWrite | Sequence[MapWrite]) -> Map | MapList:
        """Update a robot map.

        Args:
            map: MapWrite or list of MapWrite.

        Returns:
            Map object.

        """
        with tmp_disable_gzip():
            return self._update_multiple(
                items=map,
                resource_cls=Map,
                list_cls=MapList,
                update_cls=_MapUpdate,
            )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """Delete a robot map.

        Args:
            external_id: External id of the robot map.

        Returns:
            None

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            self._delete_multiple(identifiers=identifiers, wrap_ids=True)

    def list(self) -> MapList:
        """List robot maps.

        Returns:
            MapList

        """
        with tmp_disable_gzip():
            return self._list(method="GET", resource_cls=Map, list_cls=MapList)
