from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, TypeVar

from cognite.client.data_classes._base import (
    CogniteObject,
    T_CogniteResourceList,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, ItemsRequest
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, JsonVal, T_WritableCogniteResourceList

from .selectors import DataSelector


@dataclass
class StorageIOConfig:
    kind: str
    folder_name: str
    value: JsonVal
    filename: str | None = None


@dataclass
class TmpUploadItem(Generic[T_ID]):
    data: CogniteObject
    as_id_fun: Callable[[CogniteObject], T_ID]

    def dump(self) -> Any:
        return self.data.dump()

    def as_id(self) -> T_ID:
        return self.as_id_fun(self.data)


T_Selector = TypeVar("T_Selector", bound=DataSelector)


class StorageIO(ABC, Generic[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList]):
    """This is a base class for all storage classes in Cognite Toolkit

    It defines the interface for downloading data from CDF. Note this can also be used for multiple
    types of resources, for example, a hierarchy of assets/files/events/time series.

    Attributes
        SUPPORTED_DOWNLOAD_FORMATS: A set of formats that the storage item supports for downloading.
        SUPPORTED_COMPRESSIONS: A set of compression formats that the storage item supports.
        CHUNK_SIZE: The size of the data chunks to be processed during download and upload operations.

    Args:
        client: An instance of ToolkitClient to interact with the CDF API.
    """

    SUPPORTED_DOWNLOAD_FORMATS: ClassVar[frozenset[str]]
    SUPPORTED_COMPRESSIONS: ClassVar[frozenset[str]]
    CHUNK_SIZE: ClassVar[int]
    BASE_SELECTOR: ClassVar[type[DataSelector]]

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @abstractmethod
    def as_id(self, item: dict[str, JsonVal] | object) -> T_ID:
        """Convert an item to its corresponding ID.
        Args:
            item: The item to convert.
        Returns:
            The ID corresponding to the item.
        """
        raise NotImplementedError()

    @abstractmethod
    def stream_data(self, selector: T_Selector, limit: int | None = None) -> Iterable[T_WritableCogniteResourceList]:
        """Download items from the storage given the selection criteria.

        Args:
            selector: The selection criteria to filter the items to download.
            limit: Optional limit on the number of items to download.

        Returns:
            An iterable of writable Cognite resource lists.
        """
        raise NotImplementedError()

    @abstractmethod
    def count(self, selector: T_Selector) -> int | None:
        """Count the number of items in the storage that match the given selector.

        Args:
            selector: The selection criteria to filter the items to count.

        Returns:
            The number of items that match the selection criteria, or None if counting is not supported.
        """
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: T_WritableCogniteResourceList, selector: T_Selector
    ) -> list[dict[str, JsonVal]]:
        """Convert a chunk of data to a JSON-compatible format.

        Args:
            data_chunk: The chunk of data to convert, which should be a writable Cognite resource list.
            selector: The selection criteria to identify the data.

        Returns:
            A list of dictionaries representing the data in a JSON-compatible format.

        """
        return data_chunk.as_write().dump(camel_case=True)


class UploadableStorageIO(StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList], ABC):
    """A base class for storage items that support uploading data to CDF.

    Attributes:
        KIND: The kind of storage item (e.g., "RAW", "AssetCentric").
        SUPPORTED_READ_FORMATS: A set of formats that the storage item supports for reading.
        UPLOAD_ENDPOINT: The API endpoint for uploading data to the storage item.
        UPLOAD_EXTRA_ARGS: Additional arguments to include in the upload request.
    """

    KIND: ClassVar[str]
    SUPPORTED_READ_FORMATS: ClassVar[frozenset[str]]
    UPLOAD_ENDPOINT: ClassVar[str]
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = None

    def upload_items(
        self, data_chunk: T_CogniteResourceList, http_client: HTTPClient, selector: T_Selector | None = None
    ) -> Sequence[HTTPMessage]:
        """Upload a chunk of data to the storage using a custom HTTP client.
        This ensures that even if one item in the chunk fails, the rest will still be uploaded.

        This assumes that the data_chunk is respecting the CHUNK_SIZE of the storage.

        Args:
            data_chunk: The chunk of data to upload, which should be a list of writable Cognite resources.
            http_client: The custom HTTP client to use for the upload.
            selector: Optional selection criteria to identify where to upload the data.
        """
        if not hasattr(self, "UPLOAD_ENDPOINT"):
            raise ToolkitNotImplementedError(f"Upload not implemented for {self.KIND} storage.")
        if len(data_chunk) > self.CHUNK_SIZE:
            raise ValueError(f"Data chunk size {len(data_chunk)} exceeds the maximum CHUNK_SIZE of {self.CHUNK_SIZE}.")

        config = http_client.config
        return http_client.request_with_retries(
            message=ItemsRequest(
                endpoint_url=config.create_api_url(self.UPLOAD_ENDPOINT),
                method="POST",
                items=[TmpUploadItem(item, as_id_fun=self.as_id) for item in data_chunk],
                extra_body_fields=dict(self.UPLOAD_EXTRA_ARGS or {}),
            )
        )

    @abstractmethod
    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> T_CogniteResourceList:
        """Convert a JSON-compatible chunk of data back to a writable Cognite resource list.

        Args:
            data_chunk: A list of dictionaries representing the data in a JSON-compatible format.
        Returns:
            A writable Cognite resource list representing the data.
        """
        raise NotImplementedError()


class ConfigurableStorageIO(
    UploadableStorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList], ABC
):
    """A base class for storage items that support configurations for different storage items."""

    @abstractmethod
    def configurations(self, selector: T_Selector) -> Iterable[StorageIOConfig]:
        """Return configurations for the storage item."""
        raise NotImplementedError()


class TableStorageIO(
    ConfigurableStorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList], ABC
):
    """A base class for storage items that support table schemas."""

    @abstractmethod
    def get_schema(self, selector: T_Selector) -> list[SchemaColumn]:
        """Get the schema of the table associated with the given selector.

        Args:
            selector: The selection criteria to identify the data.

        Returns:
            A list of SchemaColumn objects representing the schema of the table.

        """
