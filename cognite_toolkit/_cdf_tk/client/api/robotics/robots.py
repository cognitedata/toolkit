from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.api.robotics.utlis import tmp_disable_gzip
from cognite_toolkit._cdf_tk.client.data_classes.robotics import Robot, RobotList, RobotWrite, _RobotUpdate


class RobotsAPI(APIClient):
    _RESOURCE_PATH = "/robotics/robots"

    @overload
    def __call__(self) -> Iterator[Robot]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[RobotList]: ...

    def __call__(self, chunk_size: int | None = None) -> Iterator[Robot] | Iterator[RobotList]:
        """Iterate over robot capabilities.

        Args:
            chunk_size: The number of robot capabilities to return in each chunk. None will return all robot capabilities.

        Yields:
            Robot or RobotList

        """
        return self._list_generator(method="GET", resource_cls=Robot, list_cls=RobotList, chunk_size=chunk_size)

    def __iter__(self) -> Iterator[Robot]:
        return self.__call__()

    @overload
    def create(self, capability: RobotWrite) -> Robot: ...

    @overload
    def create(self, capability: Sequence[RobotWrite]) -> RobotList: ...

    def create(self, capability: RobotWrite | Sequence[RobotWrite]) -> Robot | RobotList:
        """Create a new robot capability.

        Args:
            capability: RobotWrite or list of RobotWrite.

        Returns:
            Robot object.

        """
        with tmp_disable_gzip():
            return self._create_multiple(
                list_cls=RobotList,
                resource_cls=Robot,
                items=capability,
                input_resource_cls=RobotWrite,
            )

    @overload
    def retrieve(self, external_id: str) -> Robot | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> RobotList: ...

    def retrieve(self, external_id: str | SequenceNotStr[str]) -> Robot | None | RobotList:
        """Retrieve a robot capability.

        Args:
            external_id: External id of the robot capability.

        Returns:
            Robot object.

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            return self._retrieve_multiple(
                identifiers=identifiers,
                resource_cls=Robot,
                list_cls=RobotList,
            )

    @overload
    def update(self, capability: RobotWrite) -> Robot: ...

    @overload
    def update(self, capability: Sequence[RobotWrite]) -> RobotList: ...

    def update(self, capability: RobotWrite | Sequence[RobotWrite]) -> Robot | RobotList:
        """Update a robot capability.

        Args:
            capability: RobotWrite or list of RobotWrite.

        Returns:
            Robot object.

        """
        with tmp_disable_gzip():
            return self._update_multiple(
                items=capability,
                resource_cls=Robot,
                list_cls=RobotList,
                update_cls=_RobotUpdate,
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

    def list(self) -> RobotList:
        """List robot capabilities.

        Returns:
            RobotList

        """
        with tmp_disable_gzip():
            return self._list(method="GET", resource_cls=Robot, list_cls=RobotList)
