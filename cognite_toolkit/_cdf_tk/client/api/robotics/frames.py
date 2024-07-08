from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
    Frame,
    FrameList,
    FrameWrite,
    _FrameUpdate,
)

from .utlis import tmp_disable_gzip


class FramesAPI(APIClient):
    _RESOURCE_PATH = "/robotics/frames"

    @overload
    def __call__(self) -> Iterator[Frame]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[FrameList]: ...

    def __call__(self, chunk_size: int | None = None) -> Iterator[Frame] | Iterator[FrameList]:
        """Iterate over robot frames.

        Args:
            chunk_size: The number of robot frames to return in each chunk. None will return all robot frames.

        Yields:
            Frame or FrameList

        """
        return self._list_generator(method="GET", resource_cls=Frame, list_cls=FrameList, chunk_size=chunk_size)

    def __iter__(self) -> Iterator[Frame]:
        return self.__call__()

    @overload
    def create(self, frame: FrameWrite) -> Frame: ...

    @overload
    def create(self, frame: Sequence[FrameWrite]) -> FrameList: ...

    def create(self, frame: FrameWrite | Sequence[FrameWrite]) -> Frame | FrameList:
        """Create a new robot frame.

        Args:
            frame: FrameWrite or list of FrameWrite.

        Returns:
            Frame object.

        """
        with tmp_disable_gzip():
            return self._create_multiple(
                list_cls=FrameList,
                resource_cls=Frame,
                items=frame,
                input_resource_cls=FrameWrite,
            )

    @overload
    def retrieve(self, external_id: str) -> Frame | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> FrameList: ...

    def retrieve(self, external_id: str | SequenceNotStr[str]) -> Frame | None | FrameList:
        """Retrieve a robot frame.

        Args:
            external_id: External id of the robot frame.

        Returns:
            Frame object.

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            return self._retrieve_multiple(
                identifiers=identifiers,
                resource_cls=Frame,
                list_cls=FrameList,
            )

    @overload
    def update(self, frame: FrameWrite) -> Frame: ...

    @overload
    def update(self, frame: Sequence[FrameWrite]) -> FrameList: ...

    def update(self, frame: FrameWrite | Sequence[FrameWrite]) -> Frame | FrameList:
        """Update a robot frame.

        Args:
            frame: FrameWrite or list of FrameWrite.

        Returns:
            Frame object.

        """
        with tmp_disable_gzip():
            return self._update_multiple(
                items=frame,
                resource_cls=Frame,
                list_cls=FrameList,
                update_cls=_FrameUpdate,
            )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """Delete a robot frame.

        Args:
            external_id: External id of the robot frame.

        Returns:
            None

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            self._delete_multiple(identifiers=identifiers, wrap_ids=True)

    def list(self) -> FrameList:
        """List robot frames.

        Returns:
            FrameList

        """
        with tmp_disable_gzip():
            return self._list(method="GET", resource_cls=Frame, list_cls=FrameList)
