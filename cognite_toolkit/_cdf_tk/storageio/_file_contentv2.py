from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

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
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import (
    FILEPATH,
    FileMetadataRequest,
    FileMetadataResponse,
)
from cognite_toolkit._cdf_tk.resource_ios import FileMetadataCRUD
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import StorageIOConfig
from ._base import Bookmark, ConfigurableStorageIO, DataItem, Page, TableUploadableStorageIO
from .selectors import (
    FILENAME_VARIABLE,
    FileMetadataContentSelectorV2,
    FileMetadataFilesSelectorV2,
    FileMetadataTemplateSelectorV2,
)


class FileMetadataContentIO(
    TableUploadableStorageIO[FileMetadataContentSelectorV2, FileMetadataResponse, FileMetadataRequest],
    ConfigurableStorageIO[FileMetadataContentSelectorV2, FileMetadataResponse],
):
    CHUNK_SIZE = 10

    def __init__(self, client: ToolkitClient, overwrite: bool = False) -> None:
        super().__init__(client)
        self.overwrite = overwrite
        self._crud = FileMetadataCRUD(client, None, None, support_upload=False)
        self._downloaded_data_sets_by_selector: dict[FileMetadataContentSelectorV2 | None, set[int]] = defaultdict(set)
        self._downloaded_labels_by_selector: dict[FileMetadataContentSelectorV2 | None, set[ExternalId]] = defaultdict(
            set
        )

    def _verify_download_selector(self, selector: FileMetadataContentSelectorV2) -> tuple[InternalId, ...]:
        if isinstance(selector, FileMetadataFilesSelectorV2) and selector.ids:
            return selector.ids
        raise NotImplementedError(f"{selector.type} does not support download")

    def stream_data(
        self, selector: FileMetadataContentSelectorV2, limit: int | None = None, bookmark: Bookmark | None = None
    ) -> Iterable[Page[FileMetadataResponse]]:
        file_ids = self._verify_download_selector(selector)
        for chunk in chunker_sequence(file_ids, self.CHUNK_SIZE):
            file_metadata = self.client.tool.filemetadata.retrieve(list(chunk), ignore_unknown_ids=True)
            if len(file_metadata) != len(chunk) and (
                missing_file_ids := ({file.as_internal_id() for file in file_metadata} - set(chunk))
            ):
                raise NotImplementedError(
                    f"The following file ids were not found and cannot be downloaded: {', '.join(str(id) for id in missing_file_ids)}"
                )

            for file in file_metadata:
                filepath = self._download_content(file)
                file.filepath = filepath

            yield Page(
                worker_id="main",
                items=[DataItem(tracking_id=str(file.id), item=file) for file in file_metadata],
            )

    def count(self, selector: FileMetadataContentSelectorV2) -> int | None:
        return len(self._verify_download_selector(selector))

    def _download_content(self, file_metadata: FileMetadataResponse) -> Path:
        raise NotImplementedError()

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: FileMetadataContentSelectorV2 | None = None
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
        self, items: Iterable[FileMetadataResponse], selector: FileMetadataContentSelectorV2 | None = None
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
        self, data_chunk: Page[FileMetadataResponse], selector: FileMetadataContentSelectorV2 | None = None
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

    def configurations(self, selector: FileMetadataContentSelectorV2) -> Iterable[StorageIOConfig]:
        raise NotImplementedError()

    def upload_items(
        self,
        data_chunk: Page[FileMetadataRequest],
        http_client: HTTPClient,
        selector: FileMetadataContentSelectorV2 | None = None,
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
        cls, reader: MultiFileReader, selector: FileMetadataContentSelectorV2
    ) -> Iterable[Page[dict[str, JsonVal]]]:
        if not isinstance(selector, FileMetadataTemplateSelectorV2):
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
    def count_items(cls, reader: MultiFileReader, selector: FileMetadataContentSelectorV2 | None = None) -> int:
        if not isinstance(selector, FileMetadataTemplateSelectorV2):
            return super().count_items(reader, selector)
        return len(reader.input_files)
