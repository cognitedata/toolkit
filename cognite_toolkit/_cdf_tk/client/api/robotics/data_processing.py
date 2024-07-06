from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
    DataProcessing,
    DataProcessingList,
    DataProcessingWrite,
    _DataProcessingUpdate,
)

from .utlis import tmp_disable_gzip


class DataProcessingAPI(APIClient):
    _RESOURCE_PATH = "/robotics/data_processing"

    @overload
    def __call__(self) -> Iterator[DataProcessing]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[DataProcessingList]: ...

    def __call__(self, chunk_size: int | None = None) -> Iterator[DataProcessing] | Iterator[DataProcessingList]:
        """Iterate over robot data_processing.

        Args:
            chunk_size: The number of robot data_processing to return in each chunk. None will return all robot data_processing.

        Yields:
            DataProcessing or DataProcessingList

        """
        return self._list_generator(
            method="GET", resource_cls=DataProcessing, list_cls=DataProcessingList, chunk_size=chunk_size
        )

    def __iter__(self) -> Iterator[DataProcessing]:
        return self.__call__()

    @overload
    def create(self, dataProcessing: DataProcessingWrite) -> DataProcessing: ...

    @overload
    def create(self, dataProcessing: Sequence[DataProcessingWrite]) -> DataProcessingList: ...

    def create(
        self, dataProcessing: DataProcessingWrite | Sequence[DataProcessingWrite]
    ) -> DataProcessing | DataProcessingList:
        """Create a new robot dataProcessing.

        Args:
            dataProcessing: DataProcessingWrite or list of DataProcessingWrite.

        Returns:
            DataProcessing object.

        """
        with tmp_disable_gzip():
            return self._create_multiple(
                list_cls=DataProcessingList,
                resource_cls=DataProcessing,
                items=dataProcessing,
                input_resource_cls=DataProcessingWrite,
            )

    @overload
    def retrieve(self, external_id: str) -> DataProcessing | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> DataProcessingList: ...

    def retrieve(self, external_id: str | SequenceNotStr[str]) -> DataProcessing | None | DataProcessingList:
        """Retrieve a robot dataProcessing.

        Args:
            external_id: External id of the robot dataProcessing.

        Returns:
            DataProcessing object.

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            return self._retrieve_multiple(
                identifiers=identifiers,
                resource_cls=DataProcessing,
                list_cls=DataProcessingList,
            )

    @overload
    def update(self, dataProcessing: DataProcessingWrite) -> DataProcessing: ...

    @overload
    def update(self, dataProcessing: Sequence[DataProcessingWrite]) -> DataProcessingList: ...

    def update(
        self, dataProcessing: DataProcessingWrite | Sequence[DataProcessingWrite]
    ) -> DataProcessing | DataProcessingList:
        """Update a robot dataProcessing.

        Args:
            dataProcessing: DataProcessingWrite or list of DataProcessingWrite.

        Returns:
            DataProcessing object.

        """
        with tmp_disable_gzip():
            return self._update_multiple(
                items=dataProcessing,
                resource_cls=DataProcessing,
                list_cls=DataProcessingList,
                update_cls=_DataProcessingUpdate,
            )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """Delete a robot dataProcessing.

        Args:
            external_id: External id of the robot dataProcessing.

        Returns:
            None

        """
        identifiers = IdentifierSequence.load(external_ids=external_id)
        with tmp_disable_gzip():
            self._delete_multiple(identifiers=identifiers, wrap_ids=True)

    def list(self) -> DataProcessingList:
        """List robot data_processing.

        Returns:
            DataProcessingList

        """
        with tmp_disable_gzip():
            return self._list(method="GET", resource_cls=DataProcessing, list_cls=DataProcessingList)
