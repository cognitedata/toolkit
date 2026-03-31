from abc import ABC
from collections.abc import Iterable
from typing import Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
)
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsResultList
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileRequest, CogniteFileResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import DataSelector, StorageIOConfig, T_DataResponse, T_Selector
from ._base import Bookmark, ConfigurableStorageIO, Page, TableUploadableStorageIO


class FileMetadataContentSelector(DataSelector, ABC):
    kind: Literal["FileMetadataContent"] = "FileMetadataContent"


class CogniteFileContentSelector(DataSelector, ABC):
    kind: Literal["CogniteFileContent"] = "CogniteFileContent"


class FileMetadataContentIO(
    TableUploadableStorageIO[FileMetadataContentSelector, FileMetadataResponse, FileMetadataRequest],
    ConfigurableStorageIO[FileMetadataContentSelector, FileMetadataResponse],
):
    def __init__(self, client: ToolkitClient, overwrite: bool = False) -> None:
        super().__init__(client)
        self.overwrite = overwrite

    def stream_data(
        self, selector: FileMetadataContentSelector, limit: int | None = None, bookmark: Bookmark | None = None
    ) -> Iterable[Page[T_DataResponse]]:
        raise NotImplementedError()

    def count(self, selector: FileMetadataContentSelector) -> int | None:
        raise NotImplementedError()

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: FileMetadataContentSelector | None = None
    ) -> FileMetadataRequest:
        raise NotImplementedError()

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> FileMetadataRequest:
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: Page[FileMetadataResponse], selector: FileMetadataContentSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        raise NotImplementedError()

    def configurations(self, selector: FileMetadataContentSelector) -> Iterable[StorageIOConfig]:
        raise NotImplementedError()

    def upload_items(
        self,
        data_chunk: Page[FileMetadataRequest],
        http_client: HTTPClient,
        selector: T_Selector | None = None,
    ) -> ItemsResultList:
        raise NotImplementedError()


class CogniteFileContentIO(
    TableUploadableStorageIO[CogniteFileContentSelector, CogniteFileResponse, CogniteFileRequest],
    ConfigurableStorageIO[CogniteFileContentSelector, CogniteFileResponse],
):
    def __init__(self, client: ToolkitClient, overwrite: bool = False) -> None:
        super().__init__(client)
        self.overwrite = overwrite

    def stream_data(
        self, selector: CogniteFileContentSelector, limit: int | None = None, bookmark: Bookmark | None = None
    ) -> Iterable[Page[CogniteFileResponse]]:
        raise NotImplementedError()

    def count(self, selector: CogniteFileContentSelector) -> int | None:
        raise NotImplementedError()

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: CogniteFileContentSelector | None = None
    ) -> CogniteFileRequest:
        raise NotImplementedError()

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> CogniteFileRequest:
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: Page[CogniteFileResponse], selector: CogniteFileContentSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        raise NotImplementedError()

    def configurations(self, selector: CogniteFileContentSelector) -> Iterable[StorageIOConfig]:
        raise NotImplementedError()

    def upload_items(
        self,
        data_chunk: Page[CogniteFileRequest],
        http_client: HTTPClient,
        selector: T_Selector | None = None,
    ) -> ItemsResultList:
        raise NotImplementedError()
