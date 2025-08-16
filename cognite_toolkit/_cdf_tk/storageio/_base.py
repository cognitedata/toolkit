from abc import ABC, abstractmethod
from collections.abc import Hashable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, TypeVar

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    WriteableCogniteResourceList,
)
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.table_writers import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

T_StorageID = TypeVar("T_StorageID", bound=Hashable)
T_WritableCogniteResourceList = TypeVar("T_WritableCogniteResourceList", bound=WriteableCogniteResourceList)


@dataclass
class StorageIOConfig:
    kind: str
    folder_name: str
    value: JsonVal


class StorageIO(ABC, Generic[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList]):
    """This is a base class for all storage classes in Cognite Toolkit"""

    folder_name: str
    kind: str
    display_name: str
    supported_download_formats: frozenset[str]
    supported_compressions: frozenset[str]
    supported_read_formats: frozenset[str]
    chunk_size: int

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

    @abstractmethod
    def configurations(self, identifier: T_StorageID) -> Iterable[StorageIOConfig]:
        """Return configurations for the storage item."""
        raise NotImplementedError()

    @abstractmethod
    def load_identifier(self, datafile: Path) -> T_StorageID:
        """Load the identifier from the storage."""
        raise NotImplementedError()

    @abstractmethod
    def ensure_configurations(self, identifier: T_StorageID, console: Console | None = None) -> None:
        """Ensure that the necessary configurations for the storage item are in place.

        This method should create the necessary configurations in CDF if they do not exist.
        For example, for RAW tables, this will create the RAW database and table.

        For asset-centric storage, this will create labels and data sets.

        Args:
            identifier: The identifier of the storage item.
            console: An optional console for outputting messages during the configuration process.

        """
        raise NotImplementedError()


class TableStorageIO(StorageIO[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList], ABC):
    @abstractmethod
    def get_schema(self, identifier: T_StorageID) -> list[SchemaColumn]: ...
