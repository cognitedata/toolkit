from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence, Sized
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, Literal, TypeVar

from pydantic import ConfigDict

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import RequestItem
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsRequest, ItemsResultList
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.protocols import T_ResourceRequest, T_ResourceResponse
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader, SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from .logger import DataLogger, NoOpLogger
from .selectors import DataSelector


@dataclass
class StorageIOConfig:
    kind: str
    folder_name: str
    value: JsonVal
    filename: str | None = None


T_Selector = TypeVar("T_Selector", bound=DataSelector)


@dataclass
class Page(Generic[T_ResourceResponse], Sized):
    worker_id: str
    items: Sequence[T_ResourceResponse]
    next_cursor: str | None = None

    def __len__(self) -> int:
        return len(self.items)


class UploadItem(RequestItem, Generic[T_ResourceRequest]):
    """An item to be uploaded to CDF, consisting of a source ID and the writable Cognite resource.

    Attributes:
        source_id: The source identifier for the item. For example, the line number in a CSV file.
        item: The writable Cognite resource to be uploaded.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_id: str
    item: T_ResourceRequest

    def __str__(self) -> str:
        return self.source_id

    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        return self.item.dump(camel_case=camel_case)


class StorageIO(ABC, Generic[T_Selector, T_ResourceResponse]):
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
        self.logger: DataLogger = NoOpLogger()

    @abstractmethod
    def as_id(self, item: T_ResourceResponse) -> str:
        """Convert an item to its corresponding ID.
        Args:
            item: The item to convert.
        Returns:
            The ID corresponding to the item.
        """
        raise NotImplementedError()

    @abstractmethod
    def stream_data(self, selector: T_Selector, limit: int | None = None) -> Iterable[Page]:
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

    @abstractmethod
    def data_to_json_chunk(
        self, data_chunk: Sequence[T_ResourceResponse], selector: T_Selector | None = None
    ) -> list[dict[str, JsonVal]]:
        """Convert a chunk of data to a JSON-compatible format.

        Args:
            data_chunk: The chunk of data to convert, which should be a writable Cognite resource list.
            selector: Optional selection criteria to identify the data. This is required for some storage types.

        Returns:
            A list of dictionaries representing the data in a JSON-compatible format.

        """
        raise NotImplementedError()


class UploadableStorageIO(
    Generic[T_Selector, T_ResourceResponse, T_ResourceRequest], StorageIO[T_Selector, T_ResourceResponse], ABC
):
    """A base class for storage items that support uploading data to CDF.

    Attributes:
        KIND: The kind of storage item (e.g., "RAW", "AssetCentric").
        SUPPORTED_READ_FORMATS: A set of formats that the storage item supports for reading.
        UPLOAD_ENDPOINT: The API endpoint for uploading data to the storage item.
        UPLOAD_EXTRA_ARGS: Additional arguments to include in the upload request.
    """

    KIND: ClassVar[str]
    SUPPORTED_READ_FORMATS: ClassVar[frozenset[str]]
    UPLOAD_ENDPOINT_TYPE: Literal["app", "api"] = "api"
    UPLOAD_ENDPOINT_METHOD: Literal["GET", "POST", "PATCH", "DELETE", "PUT"] = "POST"
    UPLOAD_ENDPOINT: ClassVar[str]
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = None

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[T_ResourceRequest]],
        http_client: HTTPClient,
        selector: T_Selector | None = None,
    ) -> ItemsResultList:
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
        if self.UPLOAD_ENDPOINT_TYPE == "api":
            url = config.create_api_url(self.UPLOAD_ENDPOINT)
        elif self.UPLOAD_ENDPOINT_TYPE == "app":
            url = config.create_app_url(self.UPLOAD_ENDPOINT)
        else:
            raise ToolkitNotImplementedError(f"Unsupported UPLOAD_ENDPOINT_TYPE {self.UPLOAD_ENDPOINT_TYPE!r}.")

        return http_client.request_items_retries(
            message=ItemsRequest(
                endpoint_url=url,
                method=self.UPLOAD_ENDPOINT_METHOD,
                items=data_chunk,
                extra_body_fields=dict(self.UPLOAD_EXTRA_ARGS or {}),
            )
        )

    def json_chunk_to_data(
        self, data_chunk: list[tuple[str, dict[str, JsonVal]]]
    ) -> Sequence[UploadItem[T_ResourceRequest]]:
        """Convert a JSON-compatible chunk of data back to a writable Cognite resource list.

        Args:
            data_chunk: A list of tuples, each containing a source ID and a dictionary representing
                the data in a JSON-compatible format.
        Returns:
            A writable Cognite resource list representing the data.
        """
        result: list[UploadItem[T_ResourceRequest]] = []
        for source_id, item_json in data_chunk:
            item = self.json_to_resource(item_json)
            result.append(UploadItem(source_id=source_id, item=item))
        return result

    @abstractmethod
    def json_to_resource(self, item_json: dict[str, JsonVal]) -> T_ResourceRequest:
        """Convert a JSON-compatible dictionary back to a writable Cognite resource.

        Args:
            item_json: A dictionary representing the data in a JSON-compatible format.
        Returns:
            A writable Cognite resource representing the data.
        """
        raise NotImplementedError()

    @classmethod
    def read_chunks(
        cls, reader: MultiFileReader, selector: T_Selector
    ) -> Iterable[list[tuple[str, dict[str, JsonVal]]]]:
        """Read data from a MultiFileReader in chunks.

        This method yields chunks of data, where each chunk is a list of tuples. Each tuple contains a source ID
        (e.g., line number or row identifier) and a dictionary representing the data in a JSON-compatible format.

        This method can be overridden by subclasses to customize how data is read and chunked.
        Args:
            reader: An instance of MultiFileReader to read data from.
            selector: The selection criteria to identify the data.
        """
        data_name = "row" if reader.is_table else "line"
        # Include name of line for better error messages
        iterable = ((f"{data_name} {line_no}", item) for line_no, item in reader.read_chunks_with_line_numbers())

        yield from chunker(iterable, cls.CHUNK_SIZE)

    @classmethod
    def count_chunks(cls, reader: MultiFileReader) -> int:
        """Count the number of items in a MultiFileReader.

        This method can be overridden by subclasses to customize how items are counted.

        Args:
            reader: An instance of MultiFileReader to count items from.
        Returns:
            The number of items in the reader.
        """
        return reader.count()


class TableUploadableStorageIO(UploadableStorageIO[T_Selector, T_ResourceResponse, T_ResourceRequest], ABC):
    """A base class for storage items that support uploading data with table schemas."""

    def rows_to_data(
        self, rows: list[tuple[str, dict[str, JsonVal]]], selector: T_Selector | None = None
    ) -> Sequence[UploadItem[T_ResourceRequest]]:
        """Convert a row-based JSON-compatible chunk of data back to a writable Cognite resource list.

        Args:
            rows: A list of tuples, each containing a source ID and a dictionary representing
                the data in a JSON-compatible format.
            selector: Optional selection criteria to identify where to upload the data. This is required for some storage types.

        Returns:
            A writable Cognite resource list representing the data.
        """
        result: list[UploadItem[T_ResourceRequest]] = []
        for source_id, row in rows:
            item = self.row_to_resource(source_id, row, selector=selector)
            result.append(UploadItem(source_id=source_id, item=item))
        return result

    @abstractmethod
    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: T_Selector | None = None
    ) -> T_ResourceRequest:
        """Convert a row-based JSON-compatible dictionary back to a writable Cognite resource.

        Args:
            row: A dictionary representing the data in a JSON-compatible format.
            source_id: The source identifier for the item. For example, the line number in a CSV file.
            selector: Optional selection criteria to identify where to upload the data. This is required for some storage types.
        Returns:
            A writable Cognite resource representing the data.
        """
        raise NotImplementedError()


class ConfigurableStorageIO(StorageIO[T_Selector, T_ResourceResponse], ABC):
    """A base class for storage items that support configurations for different storage items."""

    @abstractmethod
    def configurations(self, selector: T_Selector) -> Iterable[StorageIOConfig]:
        """Return configurations for the storage item."""
        raise NotImplementedError()


class TableStorageIO(StorageIO[T_Selector, T_ResourceResponse], ABC):
    """A base class for storage items that support table schemas."""

    @abstractmethod
    def get_schema(self, selector: T_Selector) -> list[SchemaColumn]:
        """Get the schema of the table associated with the given selector.

        Args:
            selector: The selection criteria to identify the data.

        Returns:
            A list of SchemaColumn objects representing the schema of the table.

        """
        raise NotImplementedError()

    @abstractmethod
    def data_to_row(
        self, data_chunk: Sequence[T_ResourceResponse], selector: T_Selector | None = None
    ) -> list[dict[str, JsonVal]]:
        """Convert a chunk of data to a row-based JSON-compatible format.

        Args:
            data_chunk: The chunk of data to convert, which should be a writable Cognite resource list.
            selector: Optional selection criteria to identify the data. This is required for some storage types.

        Returns:
            A list of dictionaries representing the data in a JSON-compatible format.

        """
        raise NotImplementedError()
