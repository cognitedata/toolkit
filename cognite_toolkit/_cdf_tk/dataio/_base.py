from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping, Sequence, Sized
from dataclasses import dataclass, field
from typing import Any, ClassVar, Generic, Literal, Protocol, TypeVar, runtime_checkable

from pydantic import ConfigDict, Field

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import RequestItem
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsRequest, ItemsResultList
from cognite_toolkit._cdf_tk.dataio.progress import Bookmark, FileBookmark, NoBookmark
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader, SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from .logger import DataLogger, NoOpLogger
from .selectors import DataSelector


@runtime_checkable
class DataRequestProtocol(Protocol):
    def dump(self, camel_case: bool = True) -> dict[str, Any]: ...


T_DataRequest = TypeVar("T_DataRequest", bound=DataRequestProtocol)
T_DataResponse = TypeVar("T_DataResponse")
T_DataItem = TypeVar("T_DataItem")
T_NewDataItem = TypeVar("T_NewDataItem")


@dataclass
class StorageIOConfig:
    kind: str
    folder_name: str
    value: JsonVal
    filename: str | None = None


class DataItem(RequestItem, Generic[T_DataItem]):
    """This wraps a data item with a tracking ID for better logging and error messages in data operations.

    Attributes:
        tracking_id: The identifier for the item.
        item: The data item, which can be either a dictionary or an object that implements the DataRequestProtocol.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    tracking_id: str = Field(
        description="Identifier of the data item in the source. For example, if the source is a file it can be a line number."
    )
    item: T_DataItem

    def __str__(self) -> str:
        return self.tracking_id

    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        if isinstance(self.item, dict):
            return self.item
        elif isinstance(self.item, DataRequestProtocol):
            return self.item.dump(camel_case=camel_case)
        else:
            raise NotImplementedError(
                f"Cannot dump item of type {type(self.item)!r}. It must be either a dict or implement the DataRequestProtocol."
            )


T_Selector = TypeVar("T_Selector", bound=DataSelector)


@dataclass
class Page(Generic[T_DataItem], Sized):
    worker_id: str
    items: Sequence[DataItem[T_DataItem]]
    bookmark: Bookmark = field(default_factory=NoBookmark)

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self) -> Iterator[DataItem[T_DataItem]]:
        return iter(self.items)

    def as_raw_items(self) -> Sequence[T_DataItem]:
        return [item.item for item in self.items]

    def create_from(self, items: Sequence[DataItem[T_NewDataItem]]) -> "Page[T_NewDataItem]":
        return Page[T_NewDataItem](worker_id=self.worker_id, items=items, bookmark=self.bookmark)


class DataIO(ABC, Generic[T_Selector, T_DataResponse]):
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
        self._logger: DataLogger = NoOpLogger()

    @property
    def logger(self) -> DataLogger:
        return self._logger

    @logger.setter
    def logger(self, new_logger: DataLogger) -> None:
        self._logger = new_logger

    def emit_registered_page(self, page: "Page[T_DataResponse]") -> "Page[T_DataResponse]":
        """Register all item tracking IDs with the current logger, then return the page for yielding."""
        if page.items:
            self.logger.register([item.tracking_id for item in page.items])
        return page

    @abstractmethod
    def stream_data(
        self,
        selector: T_Selector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page[T_DataResponse]]:
        """Download items from the storage given the selection criteria.

        Args:
            selector: The selection criteria to filter the items to download.
            limit: Optional limit on the number of items to download.
            bookmark: Optional bookmark to resume downloading from a previous position.

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
        self, data_chunk: Page[T_DataResponse], selector: T_Selector | None = None
    ) -> Page[dict[str, JsonVal]]:
        """Convert a chunk of data to a JSON-compatible format.

        Args:
            data_chunk: The chunk of data to convert, which should be a writable Cognite resource list.
            selector: Optional selection criteria to identify the data. This is required for some storage types.

        Returns:
            A list of dictionaries representing the data in a JSON-compatible format.

        """
        raise NotImplementedError()


class UploadableDataIO(Generic[T_Selector, T_DataResponse, T_DataRequest], DataIO[T_Selector, T_DataResponse], ABC):
    """A base class for storage items that support uploading data to CDF.

    Attributes:
        KIND: The kind of storage item (e.g., "RAW", "AssetCentric").
        SUPPORTED_READ_FORMATS: A set of formats that the storage item supports for reading.
        UPLOAD_ENDPOINT: The API endpoint for uploading data to the storage item.
        UPLOAD_EXTRA_ARGS: Additional arguments to include in the upload request.
    """

    KIND: ClassVar[str]
    SUPPORTED_READ_FORMATS: ClassVar[frozenset[str]]
    CHUNK_SIZE: ClassVar[int] = 1_000
    UPLOAD_ENDPOINT_TYPE: Literal["app", "api"] = "api"
    UPLOAD_ENDPOINT_METHOD: Literal["GET", "POST", "PATCH", "DELETE", "PUT"] = "POST"
    UPLOAD_ENDPOINT: ClassVar[str]
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = None

    def upload_items(
        self,
        data_chunk: Page[T_DataRequest],
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
                items=data_chunk.items,
                extra_body_fields=dict(self.UPLOAD_EXTRA_ARGS or {}),
            )
        )

    def json_chunk_to_data(self, data_chunk: Page[dict[str, JsonVal]]) -> Page[T_DataRequest]:
        """Convert a JSON-compatible chunk of data to a writable Cognite resource list.

        Args:
            data_chunk: A page with raw data from to convert.

        Returns:
            A page with data from converted to JSON-compatible format.

        """
        result: list[DataItem[T_DataRequest]] = []
        for chunk in data_chunk.items:
            item = self.json_to_resource(chunk.item)
            result.append(DataItem(tracking_id=chunk.tracking_id, item=item))
        return data_chunk.create_from(result)

    @abstractmethod
    def json_to_resource(self, item_json: dict[str, JsonVal]) -> T_DataRequest:
        """Convert a JSON-compatible dictionary back to a writable Cognite resource.

        Args:
            item_json: A dictionary representing the data in a JSON-compatible format.
        Returns:
            A writable Cognite resource representing the data.
        """
        raise NotImplementedError()

    @classmethod
    def read_chunks(cls, reader: MultiFileReader, selector: T_Selector) -> Iterable[Page[dict[str, JsonVal]]]:
        """Read data from a MultiFileReader in chunks.

         This method yields pages of DataItems, where each DataItem contains a tracking ID
         (e.g., line number or row identifier)

        This method can be overridden by subclasses to customize how data is read and chunked.
        Args:
            reader: An instance of MultiFileReader to read data from.
            selector: The selection criteria to identify the data.
        """
        data_name = "row" if reader.is_table else "line"
        batch: list[DataItem[dict[str, JsonVal]]] = []
        line_no: int = -1
        for line_no, item in reader.read_chunks_with_line_numbers():
            batch.append(DataItem(tracking_id=f"{data_name} {line_no}", item=item))
            if len(batch) >= cls.CHUNK_SIZE:
                yield Page(
                    worker_id="main",
                    items=batch,
                    bookmark=FileBookmark(lineno=line_no, filepath=reader.current_file),
                )
                batch = []
        if batch:
            yield Page(
                worker_id="main",
                items=batch,
                bookmark=FileBookmark(lineno=line_no, filepath=reader.current_file),
            )

    @classmethod
    def count_items(cls, reader: MultiFileReader, selector: T_Selector | None = None) -> int:
        """Count the number of items in a MultiFileReader.

        This method can be overridden by subclasses to customize how items are counted.

        Args:
            reader: An instance of MultiFileReader to count items from.
            selector: Optional selection criteria to identify the data. This is required for some storage types.
        Returns:
            The number of items in the reader.
        """
        return reader.count()


class TableUploadableStorageIO(UploadableDataIO[T_Selector, T_DataResponse, T_DataRequest], ABC):
    """A base class for storage items that support uploading data with table schemas."""

    def rows_to_data(self, rows: Page[dict[str, JsonVal]], selector: T_Selector | None = None) -> Page[T_DataRequest]:
        """Convert a row-based JSON-compatible chunk of data back to a writable Cognite resource list.

        Args:
            rows: A page of row dictionaries, each wrapped in a DataItem with a tracking ID.
            selector: Optional selection criteria to identify where to upload the data. This is required for some storage types.

        Returns:
            A writable Cognite resource list representing the data.
        """
        result: list[DataItem[T_DataRequest]] = []
        for row in rows.items:
            request_object = self.row_to_resource(source_id=row.tracking_id, row=row.item, selector=selector)
            result.append(DataItem(tracking_id=row.tracking_id, item=request_object))
        return rows.create_from(result)

    @abstractmethod
    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: T_Selector | None = None
    ) -> T_DataRequest:
        """Convert a row-based JSON-compatible dictionary back to a writable Cognite resource.

        Args:
            row: A dictionary representing the data in a JSON-compatible format.
            source_id: The source identifier for the item. For example, the line number in a CSV file.
            selector: Optional selection criteria to identify where to upload the data. This is required for some storage types.
        Returns:
            A writable Cognite resource representing the data.
        """
        raise NotImplementedError()


class ConfigurableDataIO(DataIO[T_Selector, T_DataResponse], ABC):
    """A base class for storage items that support configurations for different storage items."""

    @abstractmethod
    def configurations(self, selector: T_Selector) -> Iterable[StorageIOConfig]:
        """Return configurations for the storage item."""
        raise NotImplementedError()


class TableDataIO(DataIO[T_Selector, T_DataResponse], ABC):
    """A base class for storage items that support table schemas."""

    @abstractmethod
    def get_schema(self, selector: T_Selector) -> list[SchemaColumn] | None:
        """Get the schema of the table associated with the given selector.

        Args:
            selector: The selection criteria to identify the data.

        Returns:
            A list of SchemaColumn objects representing the schema of the table.
            None indicates that no schema is available, and the data must first be downloaded
            before becoming available.

        """
        raise NotImplementedError()

    @abstractmethod
    def data_to_row(
        self, data_chunk: Page[T_DataResponse], selector: T_Selector | None = None
    ) -> Page[dict[str, JsonVal]]:
        """Convert a chunk of data to a row-based JSON-compatible format.

        Args:
            data_chunk: The chunk of data to convert, which should be a writable Cognite resource list.
            selector: Optional selection criteria to identify the data. This is required for some storage types.

        Returns:
            A list of dictionaries representing the data in a JSON-compatible format.

        """
        raise NotImplementedError()
