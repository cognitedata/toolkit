from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Generic

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
)
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, ItemsRequest
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, JsonVal, T_Selector, T_WritableCogniteResourceList


@dataclass
class StorageIOConfig:
    kind: str
    folder_name: str
    value: JsonVal


class StorageIO(ABC, Generic[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList]):
    """This is a base class for all storage classes in Cognite Toolkit

    It defines the interface for interacting with storage items in CDF, such as downloading,
    uploading, and managing configurations. Each storage type (e.g., RAW, asset-centric)
    should implement this interface to provide specific functionality.

    Attributes:
        FOLDER_NAME: The name of the folder in which the storage item is located.
        KIND: The type of storage (e.g., 'raw', 'assets').
        DISPLAY_NAME: A human-readable name for the storage item.
        SUPPORTED_DOWNLOAD_FORMATS: A set of formats that the storage item supports for downloading.
        SUPPORTED_COMPRESSIONS: A set of compression formats that the storage item supports.
        SUPPORTED_READ_FORMATS: A set of formats that the storage item supports for reading.
        CHUNK_SIZE: The size of the data chunks to be processed during download and upload operations.
        client: An instance of ToolkitClient to interact with the CDF API.
    """

    FOLDER_NAME: str
    KIND: str
    DISPLAY_NAME: str
    SUPPORTED_DOWNLOAD_FORMATS: frozenset[str]
    SUPPORTED_COMPRESSIONS: frozenset[str]
    SUPPORTED_READ_FORMATS: frozenset[str]
    CHUNK_SIZE: int
    UPLOAD_ENDPOINT: ClassVar[str]
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = None

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

    @abstractmethod
    def upload_items(self, data_chunk: T_CogniteResourceList, selector: T_Selector) -> None:
        """Upload a chunk of data to the storage.

        Args:
            data_chunk: The chunk of data to upload, which should be a list of writable Cognite resources.
            selector: The selection criteria to identify where to upload the data.
        """
        raise NotImplementedError()

    def upload_items_force(
        self, data_chunk: T_CogniteResourceList, http_client: HTTPClient, selector: T_Selector | None = None
    ) -> Sequence[HTTPMessage]:
        """Upload a chunk of data to the storage using a custom HTTP client.
        This ensures that even if one item in the chunk fails, the rest will still be uploaded.

        Args:
            data_chunk: The chunk of data to upload, which should be a list of writable Cognite resources.
            http_client: The custom HTTP client to use for the upload.
            selector: Optional selection criteria to identify where to upload the data.
        """
        if not hasattr(self, "UPLOAD_ENDPOINT"):
            raise ToolkitNotImplementedError(f"Upload not implemented for {self.KIND} storage.")

        config = http_client.config
        results: list[HTTPMessage] = []
        for batch in chunker_sequence(data_chunk, self.CHUNK_SIZE):
            batch_results = http_client.request_with_retries(
                message=ItemsRequest(
                    endpoint_url=config.create_api_url(self.UPLOAD_ENDPOINT),
                    method="POST",
                    # The dump method from the PySDK always returns JsonVal, but mypy cannot infer that
                    items=batch.dump(camel_case=True),  # type: ignore[arg-type]
                    extra_body_fields=dict(self.UPLOAD_EXTRA_ARGS or {}),
                    as_id=self.as_id,
                )
            )
            results.extend(batch_results)
        return results

    @abstractmethod
    def data_to_json_chunk(self, data_chunk: T_WritableCogniteResourceList) -> list[dict[str, JsonVal]]:
        """Convert a chunk of data to a JSON-compatible format.

        Args:
            data_chunk: The chunk of data to convert, which should be a writable Cognite resource list.

        Returns:
            A list of dictionaries representing the data in a JSON-compatible format.

        """
        raise NotImplementedError()

    @abstractmethod
    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> T_CogniteResourceList:
        """Convert a JSON-compatible chunk of data back to a writable Cognite resource list.

        Args:
            data_chunk: A list of dictionaries representing the data in a JSON-compatible format.
        Returns:
            A writable Cognite resource list representing the data.
        """
        raise NotImplementedError()

    @abstractmethod
    def configurations(self, selector: T_Selector) -> Iterable[StorageIOConfig]:
        """Return configurations for the storage item."""
        raise NotImplementedError()

    @abstractmethod
    def load_selector(self, datafile: Path) -> T_Selector:
        """Load the selector from adjacent filepath."""
        raise NotImplementedError()

    @abstractmethod
    def ensure_configurations(self, selector: T_Selector, console: Console | None = None) -> None:
        """Ensure that the necessary configurations for the storage item are in place.

        This method should create the necessary configurations in CDF if they do not exist.
        For example, for RAW tables, this will create the RAW database and table.

        For asset-centric storage, this will create labels and data sets.

        Args:
            selector: The selection criteria to find the data.
            console: An optional console for outputting messages during the configuration process.

        """
        raise NotImplementedError()


class TableStorageIO(StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList], ABC):
    @abstractmethod
    def get_schema(self, selector: T_Selector) -> list[SchemaColumn]:
        """Get the schema of the table associated with the given selector.

        Args:
            selector: The selection criteria to identify the data.

        Returns:
            A list of SchemaColumn objects representing the schema of the table.

        """
