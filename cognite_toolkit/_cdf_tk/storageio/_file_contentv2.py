import json
import mimetypes
from abc import ABC
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal

from pydantic import ConfigDict, DirectoryPath, Field, field_validator

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.http_client._item_classes import (
    ItemsFailedRequest,
    ItemsResultList,
    ItemsResultMessage,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileRequest, CogniteFileResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.cruds import FileMetadataCRUD
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import DataSelector, StorageIOConfig
from ._base import Bookmark, ConfigurableStorageIO, DataItem, Page, TableUploadableStorageIO
from .selectors._base import SelectorObject

FILENAME_VARIABLE = "$FILENAME"
FILEPATH = "$FILEPATH"


class FileMetadataTemplate(SelectorObject):
    model_config = ConfigDict(extra="allow")
    name: str
    external_id: str

    def create_instance(self, filepath: Path, guess_mime_type: bool) -> dict[str, Any]:
        json_str = self.model_dump_json(by_alias=True)
        output = json.loads(json_str.replace(FILENAME_VARIABLE, filepath.name))
        output[FILEPATH] = filepath
        if "mimeType" not in output and guess_mime_type:
            me_type, _ = mimetypes.guess_type(filepath)
            output["mimeType"] = me_type
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
    guess_mime_type: bool

    def __str__(self) -> str:
        return self.type

    def find_data_files(self, input_dir: Path, manifest_file: Path) -> list[Path]:
        return [file for file in self.file_directory.iterdir() if file.is_file()]


class FileMetadataFilesSelector(FileMetadataContentSelector, ABC):
    """Download/upload individual files.

    For download, the ids field must be set.
    For upload, all files in a csv/parquest file are uploaded.

    """

    type: Literal["FileMetadataFiles"] = "FileMetadataFiles"
    ids: tuple[InternalId, ...] | None = Field(None, exclude=True, description="(Only used for download)")

    def __str__(self) -> str:
        return self.type


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
        self._downloaded_data_sets_by_selector: dict[FileMetadataContentSelector | None, set[int]] = defaultdict(set)
        self._downloaded_labels_by_selector: dict[FileMetadataContentSelector | None, set[ExternalId]] = defaultdict(
            set
        )

    def _verify_download_selector(self, selector: FileMetadataContentSelector) -> tuple[InternalId, ...]:
        if isinstance(selector, FileMetadataFilesSelector) and selector.ids:
            return selector.ids
        raise NotImplementedError(f"{selector.type} does not support download")

    def stream_data(
        self, selector: FileMetadataContentSelector, limit: int | None = None, bookmark: Bookmark | None = None
    ) -> Iterable[Page[FileMetadataResponse]]:
        file_ids = self._verify_download_selector(selector)
        for chunk in chunker_sequence(file_ids, self.CHUNK_SIZE):
            file_metadata = self.client.tool.filemetadata.retrieve(list(chunk), ignore_unknown_ids=True)

            if len(file_metadata) != len(chunk) and (
                missing_file_ids := ({file.as_internal_id() for file in file_metadata} - set(chunk))
            ):
                for file_id in missing_file_ids:
                    self.logger.tracker.add_issue(str(file_id.id), "Missing in CDF")
                    self.logger.tracker.finalize_item(str(file_id.id), "failure")

            for file in file_metadata:
                filepath = self._download_content(file)
                file.filepath = filepath

            yield Page(
                worker_id="main",
                items=[DataItem(tracking_id=str(file.id), item=file) for file in file_metadata],
            )

    def count(self, selector: FileMetadataContentSelector) -> int | None:
        return len(self._verify_download_selector(selector))

    def _download_content(self, file_metadata: FileMetadataResponse) -> Path:
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

    def _populate_id_cache(
        self, items: Iterable[FileMetadataResponse], selector: FileMetadataContentSelector | None = None
    ) -> None:
        data_set_ids: set[int] = set()
        asset_ids: set[int] = set()
        security_ids: set[int] = set()
        label_ids: set[ExternalId] = set()
        for item in items:
            if item.data_set_id is not None:
                data_set_ids.add(item.data_set_id)
            if item.asset_ids is not None:
                asset_ids.update(item.asset_ids)
            if item.security_categories is not None:
                security_ids.update(item.security_categories)
            if item.labels:
                label_ids.update(item.labels)
        self._downloaded_data_sets_by_selector[selector].update(data_set_ids)
        self._downloaded_labels_by_selector[selector].update(label_ids)

    def data_to_json_chunk(
        self, data_chunk: Page[FileMetadataResponse], selector: FileMetadataContentSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        # Ensure data sets/assets/security-categories are looked up to populate cache.
        # This is to avoid looking up each data set id individually in the .dump_resource call
        self._populate_id_cache(di.item for di in data_chunk.items)
        dumped: list[DataItem[dict[str, JsonVal]]] = []
        for item in data_chunk.items:
            dumped_item = self._crud.dump_resource(item.item)
            # Preserve filepath
            dumped_item[FILEPATH] = item.item.filepath
            dumped.append(DataItem(tracking_id=item.tracking_id, item=dumped_item))
        return data_chunk.create_from(dumped)

    def configurations(self, selector: FileMetadataContentSelector) -> Iterable[StorageIOConfig]:
        raise NotImplementedError()

    def upload_items(
        self,
        data_chunk: Page[FileMetadataRequest],
        http_client: HTTPClient,
        selector: FileMetadataContentSelector | None = None,
    ) -> ItemsResultList:
        return ItemsResultList([self._upload_single_item(item) for item in data_chunk])

    def _upload_single_item(self, item: DataItem[FileMetadataRequest]) -> ItemsFailedRequest | Any:
        request = item.item
        if request.filepath is None:
            return ItemsFailedRequest(
                ids=[item.tracking_id],
                error_message=f"Failed to create {item.tracking_id}. Not file path provided ({FILENAME_VARIABLE} is missing).",
            )

        created = self._create_file_metadata(item, request)
        if not isinstance(created, FileMetadataResponse):
            return created

        if created.upload_url is None:
            return ItemsFailedRequest(
                ids=[item.tracking_id],
                error_message=f"Failed to retrieve upload URL for item {item.tracking_id}.",
            )

        return self._upload_file_content(request.filepath, created.upload_url, request.mime_type, item.tracking_id)

    def _create_file_metadata(
        self, item: DataItem[FileMetadataRequest], request: FileMetadataRequest
    ) -> FileMetadataResponse | ItemsResultMessage:
        try:
            return self.client.tool.filemetadata.create([request], self.overwrite)[0]
        except ToolkitAPIError as error:
            if error.response is not None:
                return error.response.as_item_response(item.tracking_id)
            raise
        except IndexError:
            return ItemsFailedRequest(
                ids=[item.tracking_id],
                error_message=f"No response returned from CDF for item {item.tracking_id}.",
            )

    def _upload_file_content(self, filepath: Path, upload_url: str, mime_type: str | None, tracking_id: str) -> Any:
        try:
            response = self.client.tool.filemetadata.upload_file(filepath, upload_url, mime_type)
        except ToolkitAPIError as error:
            if error.response is not None:
                return error.response.as_item_response(tracking_id)
            raise
        return response.as_item_response(tracking_id)

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
                items=[
                    DataItem(tracking_id=item.as_posix(), item=template.create_instance(item, selector.guess_mime_type))
                    for item in chunk
                ],
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
        selector: CogniteFileContentSelector | None = None,
    ) -> ItemsResultList:
        raise NotImplementedError()
