from abc import ABC
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal

from pydantic import ConfigDict, DirectoryPath, field_validator, json

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.http_client._item_classes import (
    ItemsFailedRequest,
    ItemsResultList,
)
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileRequest, CogniteFileResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.cruds import FileMetadataCRUD
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import DataSelector, StorageIOConfig, T_DataResponse, T_Selector
from ._base import Bookmark, ConfigurableStorageIO, DataItem, Page, TableUploadableStorageIO
from .selectors._base import SelectorObject

FILENAME_VARIABLE = "$FILENAME"
FILEPATH = "$FILEPATH"


class FileMetadataTemplate(SelectorObject):
    model_config = ConfigDict(extra="allow")
    name: str
    external_id: str

    def create_instance(self, filepath: Path) -> dict[str, Any]:
        json_str = self.model_dump_json(by_alias=True)
        output = json.loads(json_str.replace(FILENAME_VARIABLE, filepath.name))
        output[FILEPATH] = filepath
        return output

    @field_validator("name", "external_id")
    @classmethod
    def _validate_filename_in_fields(cls, v: str) -> str:
        if FILENAME_VARIABLE not in v:
            raise ValueError(
                f"{FILENAME_VARIABLE!s} must be present in 'name' and 'external_id' fields. "
                f"This allows for dynamic substitution based on the file name."
            )
        return v


class FileMetadataContentSelector(DataSelector, ABC):
    kind: Literal["FileMetadataContent"] = "FileMetadataContent"


class FileMetadataTemplateSelector(FileMetadataContentSelector):
    type: Literal["FileMetadataTemplate"] = "FileMetadataTemplate"
    template: FileMetadataTemplate
    file_directory: DirectoryPath

    def __str__(self) -> str:
        return self.type

    def find_data_files(self, input_dir: Path, manifest_file: Path) -> list[Path]:
        return [file for file in self.file_directory.iterdir() if file.is_file()]


class FileMetadataUploadSelector(DataSelector, ABC):
    """Upload all in a given csv/parquest file"""

    kind: Literal["FileMetadataUploadSelector"] = "FileMetadataUploadSelector"


class CogniteFileContentSelector(DataSelector, ABC):
    kind: Literal["CogniteFileContent"] = "CogniteFileContent"


class FileMetadataContentIO(
    TableUploadableStorageIO[FileMetadataContentSelector, FileMetadataResponse, FileMetadataRequest],
    ConfigurableStorageIO[FileMetadataContentSelector, FileMetadataResponse],
):
    CHUNK_SIZE = 10

    def __init__(self, client: ToolkitClient, overwrite: bool = False) -> None:
        super().__init__(client)
        self.overwrite = overwrite
        self._crud = FileMetadataCRUD(client, None, None, support_upload=False)

    def stream_data(
        self, selector: FileMetadataContentSelector, limit: int | None = None, bookmark: Bookmark | None = None
    ) -> Iterable[Page[T_DataResponse]]:
        raise NotImplementedError()

    def count(self, selector: FileMetadataContentSelector) -> int | None:
        raise NotImplementedError()

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: FileMetadataContentSelector | None = None
    ) -> FileMetadataRequest:
        metadata: dict[str, JsonVal] = {}
        cleaned_row: dict[str, JsonVal] = {}
        for key, value in row.items():
            if key.startswith("metadata."):
                metadata_key = key[len("metadata.") :]
                metadata[metadata_key] = value
            else:
                cleaned_row[key] = value
        if metadata:
            cleaned_row["metadata"] = metadata
        return self._crud.load_resource(cleaned_row)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> FileMetadataRequest:
        return self._crud.load_resource(item_json)

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
        selector: FileMetadataContentSelector | None = None,
    ) -> ItemsResultList:
        results = ItemsResultList()
        for item in data_chunk.items:
            request = item.item
            if request.filepath is None:
                results.append(
                    ItemsFailedRequest(
                        ids=[item.tracking_id],
                        error_message=f"Failed to create upload {item.tracking_id}. Not file path provided ({FILENAME_VARIABLE} is missing).",
                    )
                )
                continue

            try:
                created = self.client.tool.filemetadata.create([request], self.overwrite)[0]
            except ToolkitAPIError as error:
                if error.response is not None:
                    results.append(error.response.as_item_response(item.tracking_id))
                    continue
                raise error
            except IndexError:
                results.append(
                    ItemsFailedRequest(
                        ids=[item.tracking_id],
                        error_message=f"No response returned from CDF for item {item.tracking_id}.",
                    )
                )
                continue
            if created.upload_url is None:
                results.append(
                    ItemsFailedRequest(
                        ids=[item.tracking_id],
                        error_message=f"Failed to retrieve upload URL for item {item.tracking_id}.",
                    )
                )
                continue
            try:
                response = self.client.tool.filemetadata.upload_file(
                    request.filepath, created.upload_url, request.mime_type
                )
            except ToolkitAPIError as error:
                if error.response is not None:
                    results.append(error.response.as_item_response(item.tracking_id))
                    continue
                raise error
            else:
                results.append(response.as_item_response(item.tracking_id))

        return results

    @classmethod
    def read_chunks(
        cls, reader: MultiFileReader, selector: FileMetadataContentSelector
    ) -> Iterable[Page[dict[str, JsonVal]]]:
        if not isinstance(selector, FileMetadataTemplateSelector):
            yield from super().read_chunks(reader, selector)
            return
        template = selector.template
        for chunk in chunker_sequence(reader.input_files, cls.CHUNK_SIZE):
            yield Page(
                worker_id="main",
                items=[DataItem(tracking_id=item.as_posix(), item=template.create_instance(item)) for item in chunk],
            )

    @classmethod
    def count_items(cls, reader: MultiFileReader, selector: FileMetadataContentSelector | None = None) -> int:
        if not isinstance(selector, FileMetadataTemplateSelector):
            return super().count_items(reader, selector)
        return len(reader.input_files)


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
