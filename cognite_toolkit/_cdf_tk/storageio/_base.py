from abc import ABC, abstractmethod
from collections.abc import Hashable, Iterable
from typing import Generic, TypeVar

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    WriteableCogniteResourceList,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.table_writers import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

T_StorageID = TypeVar("T_StorageID", bound=Hashable)
T_WritableCogniteResourceList = TypeVar("T_WritableCogniteResourceList", bound=WriteableCogniteResourceList)


class StorageIO(ABC, Generic[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList]):
    """This is a base class for all storage classes in Cognite Toolkit"""

    folder_name: str
    kind: str
    display_name: str
    supported_download_formats: frozenset[str]
    supported_compressions: frozenset[str]
    supported_read_formats: frozenset[str]

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @abstractmethod
    def download_iterable(
        self, identifier: T_StorageID, limit: int | None = None
    ) -> Iterable[T_WritableCogniteResourceList]:
        raise NotImplementedError()

    @abstractmethod
    def count(self, identifier: T_StorageID) -> int | None:
        raise NotImplementedError()

    @abstractmethod
    def upload_items(self, data_chunk: T_CogniteResourceList, identifier: T_StorageID) -> None:
        raise NotImplementedError()

    @abstractmethod
    def data_to_json_chunk(self, data_chunk: T_WritableCogniteResourceList) -> list[dict[str, JsonVal]]:
        raise NotImplementedError()

    @abstractmethod
    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> T_CogniteResourceList:
        raise NotImplementedError()


class TableStorageIO(StorageIO[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList], ABC):
    @abstractmethod
    def get_schema(self, identifier: T_StorageID) -> list[SchemaColumn]: ...
