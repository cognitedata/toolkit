from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
    Location,
    LocationList,
    LocationWrite,
    _LocationUpdate,
)

from .utlis import tmp_disable_gzip


class LocationsAPI(APIClient):
    _RESOURCE_PATH = "/robotics/locations"

    @overload
    def __call__(self) -> Iterator[Location]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[LocationList]: ...

    def __call__(self, chunk_size: int | None = None) -> Iterator[Location] | Iterator[LocationList]:
        """Iterate over robot locations.

        Args:
            chunk_size: The number of robot locations to return in each chunk. None will return all robot locations.

        Yields:
            Location or LocationList

        """
        return self._list_generator(method="GET", resource_cls=Location, list_cls=LocationList, chunk_size=chunk_size)

    def __iter__(self) -> Iterator[Location]:
        return self.__call__()

    @overload
    def create(self, location: LocationWrite) -> Location: ...

    @overload
    def create(self, location: Sequence[LocationWrite]) -> LocationList: ...

    def create(self, location: LocationWrite | Sequence[LocationWrite]) -> Location | LocationList:
        """Create a new robot location.

        Args:
            location: LocationWrite or list of LocationWrite.

        Returns:
            Location object.

        """
        with tmp_disable_gzip():
            return self._create_multiple(
                list_cls=LocationList,
                resource_cls=Location,
                items=location,
                input_resource_cls=LocationWrite,
            )

    @overload
    def retrieve(self, external_id: str) -> Location | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> LocationList: ...

    def retrieve(self, external_id: str | SequenceNotStr[str]) -> Location | None | LocationList:
        """Retrieve a robot location.

        Args:
            external_id: External id of the robot location.

        Returns:
            Location object.

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            return self._retrieve_multiple(
                identifiers=identifiers,
                resource_cls=Location,
                list_cls=LocationList,
            )

    @overload
    def update(self, location: LocationWrite) -> Location: ...

    @overload
    def update(self, location: Sequence[LocationWrite]) -> LocationList: ...

    def update(self, location: LocationWrite | Sequence[LocationWrite]) -> Location | LocationList:
        """Update a robot location.

        Args:
            location: LocationWrite or list of LocationWrite.

        Returns:
            Location object.

        """
        with tmp_disable_gzip():
            return self._update_multiple(
                items=location,
                resource_cls=Location,
                list_cls=LocationList,
                update_cls=_LocationUpdate,
            )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """Delete a robot location.

        Args:
            external_id: External id of the robot location.

        Returns:
            None

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            self._delete_multiple(identifiers=identifiers, wrap_ids=True)

    def list(self) -> LocationList:
        """List robot locations.

        Returns:
            LocationList

        """
        with tmp_disable_gzip():
            return self._list(method="GET", resource_cls=Location, list_cls=LocationList)
