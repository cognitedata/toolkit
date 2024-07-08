from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
    RobotCapability,
    RobotCapabilityList,
    RobotCapabilityWrite,
    _RobotCapabilityUpdate,
)

from .utlis import tmp_disable_gzip


class CapabilitiesAPI(APIClient):
    _RESOURCE_PATH = "/robotics/capabilities"

    @overload
    def __call__(self) -> Iterator[RobotCapability]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[RobotCapabilityList]: ...

    def __call__(self, chunk_size: int | None = None) -> Iterator[RobotCapability] | Iterator[RobotCapabilityList]:
        """Iterate over robot capabilities.

        Args:
            chunk_size: The number of robot capabilities to return in each chunk. None will return all robot capabilities.

        Yields:
            RobotCapability or RobotCapabilityList

        """
        return self._list_generator(
            method="GET", resource_cls=RobotCapability, list_cls=RobotCapabilityList, chunk_size=chunk_size
        )

    def __iter__(self) -> Iterator[RobotCapability]:
        return self.__call__()

    @overload
    def create(self, capability: RobotCapabilityWrite) -> RobotCapability: ...

    @overload
    def create(self, capability: Sequence[RobotCapabilityWrite]) -> RobotCapabilityList: ...

    def create(
        self, capability: RobotCapabilityWrite | Sequence[RobotCapabilityWrite]
    ) -> RobotCapability | RobotCapabilityList:
        """Create a new robot capability.

        Args:
            capability: RobotCapabilityWrite or list of RobotCapabilityWrite.

        Returns:
            RobotCapability object.

        """
        with tmp_disable_gzip():
            return self._create_multiple(
                list_cls=RobotCapabilityList,
                resource_cls=RobotCapability,
                items=capability,
                input_resource_cls=RobotCapabilityWrite,
            )

    @overload
    def retrieve(self, external_id: str) -> RobotCapability | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> RobotCapabilityList: ...

    def retrieve(self, external_id: str | SequenceNotStr[str]) -> RobotCapability | None | RobotCapabilityList:
        """Retrieve a robot capability.

        Args:
            external_id: External id of the robot capability.

        Returns:
            RobotCapability object.

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            return self._retrieve_multiple(
                identifiers=identifiers,
                resource_cls=RobotCapability,
                list_cls=RobotCapabilityList,
            )

    @overload
    def update(self, capability: RobotCapabilityWrite) -> RobotCapability: ...

    @overload
    def update(self, capability: Sequence[RobotCapabilityWrite]) -> RobotCapabilityList: ...

    def update(
        self, capability: RobotCapabilityWrite | Sequence[RobotCapabilityWrite]
    ) -> RobotCapability | RobotCapabilityList:
        """Update a robot capability.

        Args:
            capability: RobotCapabilityWrite or list of RobotCapabilityWrite.

        Returns:
            RobotCapability object.

        """
        with tmp_disable_gzip():
            return self._update_multiple(
                items=capability,
                resource_cls=RobotCapability,
                list_cls=RobotCapabilityList,
                update_cls=_RobotCapabilityUpdate,
            )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """Delete a robot capability.

        Args:
            external_id: External id of the robot capability.

        Returns:
            None

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            self._delete_multiple(identifiers=identifiers, wrap_ids=True)

    def list(self) -> RobotCapabilityList:
        """List robot capabilities.

        Returns:
            RobotCapabilityList

        """
        with tmp_disable_gzip():
            return self._list(method="GET", resource_cls=RobotCapability, list_cls=RobotCapabilityList)
