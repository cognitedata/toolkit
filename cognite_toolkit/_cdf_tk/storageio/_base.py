from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence, Sized
from dataclasses import dataclass
from typing import ClassVar, Generic, TypeVar

from cognite.client.data_classes._base import T_CogniteResource

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.fileio import FileReader, SchemaColumn
from cognite_toolkit._cdf_tk.utils.fileio._readers import TableReader
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, ItemsRequest
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal, T_WriteCogniteResource

from .selectors import DataSelector


@dataclass
class StorageIOConfig:
    kind: str
    folder_name: str
    value: JsonVal
    filename: str | None = None


T_Selector = TypeVar("T_Selector", bound=DataSelector)


@dataclass
class Page(Generic[T_CogniteResource], Sized):
    worker_id: str
    items: Sequence[T_CogniteResource]
    next_cursor: str | None = None

    def __len__(self) -> int:
        return len(self.items)


@dataclass
class UploadItem(Generic[T_WriteCogniteResource]):
    """An item to be uploaded to CDF, consisting of a source ID and the writable Cognite resource.

    Attributes:
        source_id: The source identifier for the item. For example, the line number in a CSV file.
        item: The writable Cognite resource to be uploaded.
    """

    source_id: str
    item: T_WriteCogniteResource

    def as_id(self) -> str:
        return self.source_id

    def dump(self) -> JsonVal:
        return self.item.dump(camel_case=True)


class StorageIO(ABC, Generic[T_Selector, T_CogniteResource]):
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
    def as_id(self, item: T_CogniteResource) -> str:
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
        self, data_chunk: Sequence[T_CogniteResource], selector: T_Selector | None = None
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
    Generic[T_Selector, T_CogniteResource, T_WriteCogniteResource], StorageIO[T_Selector, T_CogniteResource], ABC
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
    UPLOAD_ENDPOINT: ClassVar[str]
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = None

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[T_WriteCogniteResource]],
        http_client: HTTPClient,
        selector: T_Selector | None = None,
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
                items=list(data_chunk),
                extra_body_fields=dict(self.UPLOAD_EXTRA_ARGS or {}),
            )
        )

    def json_chunk_to_data(
        self, data_chunk: list[tuple[str, dict[str, JsonVal]]]
    ) -> Sequence[UploadItem[T_WriteCogniteResource]]:
        """Convert a JSON-compatible chunk of data back to a writable Cognite resource list.

        Args:
            data_chunk: A list of tuples, each containing a source ID and a dictionary representing
                the data in a JSON-compatible format.
        Returns:
            A writable Cognite resource list representing the data.
        """
        result: list[UploadItem[T_WriteCogniteResource]] = []
        for source_id, item_json in data_chunk:
            item = self.json_to_resource(item_json)
            result.append(UploadItem(source_id=source_id, item=item))
        return result

    @abstractmethod
    def json_to_resource(self, item_json: dict[str, JsonVal]) -> T_WriteCogniteResource:
        """Convert a JSON-compatible dictionary back to a writable Cognite resource.

        Args:
            item_json: A dictionary representing the data in a JSON-compatible format.
        Returns:
            A writable Cognite resource representing the data.
        """
        raise NotImplementedError()

    @classmethod
    def read_chunks(cls, reader: FileReader) -> Iterable[list[tuple[str, dict[str, JsonVal]]]]:
        data_name = "row" if isinstance(reader, TableReader) else "line"
        # Include name of line for better error messages
        iterable = ((f"{data_name} {line_no}", item) for line_no, item in reader.read_chunks_with_line_numbers())

        yield from chunker(iterable, cls.CHUNK_SIZE)


class TableUploadableStorageIO(UploadableStorageIO[T_Selector, T_CogniteResource, T_WriteCogniteResource], ABC):
    """A base class for storage items that support uploading data with table schemas."""

    def rows_to_data(
        self, rows: list[tuple[str, dict[str, JsonVal]]], selector: T_Selector | None = None
    ) -> Sequence[UploadItem[T_WriteCogniteResource]]:
        """Convert a row-based JSON-compatible chunk of data back to a writable Cognite resource list.

        Args:
            rows: A list of tuples, each containing a source ID and a dictionary representing
                the data in a JSON-compatible format.
            selector: Optional selection criteria to identify where to upload the data. This is required for some storage types.

        Returns:
            A writable Cognite resource list representing the data.
        """
        result: list[UploadItem[T_WriteCogniteResource]] = []
        for source_id, row in rows:
            item = self.row_to_resource(source_id, row, selector=selector)
            result.append(UploadItem(source_id=source_id, item=item))
        return result

    @abstractmethod
    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: T_Selector | None = None
    ) -> T_WriteCogniteResource:
        """Convert a row-based JSON-compatible dictionary back to a writable Cognite resource.

        Args:
            row: A dictionary representing the data in a JSON-compatible format.
            source_id: The source identifier for the item. For example, the line number in a CSV file.
            selector: Optional selection criteria to identify where to upload the data. This is required for some storage types.
        Returns:
            A writable Cognite resource representing the data.
        """
        raise NotImplementedError()


class ConfigurableStorageIO(StorageIO[T_Selector, T_CogniteResource], ABC):
    """A base class for storage items that support configurations for different storage items."""

    @abstractmethod
    def configurations(self, selector: T_Selector) -> Iterable[StorageIOConfig]:
        """Return configurations for the storage item."""
        raise NotImplementedError()


class TableStorageIO(StorageIO[T_Selector, T_CogniteResource], ABC):
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
        self, data_chunk: Sequence[T_CogniteResource], selector: T_Selector | None = None
    ) -> list[dict[str, JsonVal]]:
        """Convert a chunk of data to a row-based JSON-compatible format.

        Args:
            data_chunk: The chunk of data to convert, which should be a writable Cognite resource list.
            selector: Optional selection criteria to identify the data. This is required for some storage types.

        Returns:
            A list of dictionaries representing the data in a JSON-compatible format.

        """
        raise NotImplementedError()
