import mimetypes
from collections import defaultdict
from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path

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
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InstanceId, NameId
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileRequest, CogniteFileResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import (
    FILEPATH,
    FileMetadataRequest,
    FileMetadataResponse,
)
from cognite_toolkit._cdf_tk.resource_ios import (
    CogniteFileCRUD,
    DataSetsIO,
    FileMetadataCRUD,
    LabelIO,
    SecurityCategoryIO,
)
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader, SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import StorageIOConfig
from ._base import Bookmark, ConfigurableDataIO, DataItem, Page, TableDataIO, TableUploadableDataIO
from .logger import LogEntryV2, Severity
from .selectors import (
    CogniteFileContentSelectorV2,
    CogniteFileFilesSelectorV2,
    CogniteFileTemplateSelectorV2,
    FileMetadataContentSelectorV2,
    FileMetadataFilesSelectorV2,
    FileMetadataTemplateSelectorV2,
    InternalWithNameId,
    NodeWithNameId,
)

SINGLE_FILE_LIMIT_BYTES = 5_000 * 1024 * 1024  # 5 GB is the maximum size for a single file upload.
IDEAL_FILE_SIZE = 50 * 1024 * 1024  # We aim to have this size for each file
MULTI_FILE_PART_MIN_SIZE_BYTES = (
    5 * 1024 * 1024
)  # Each part in a multi-part upload must be at least 5 MiB, except for the last part.
MULTI_FILE_PART_MAX_SIZE_BYTES = 4_000 * 1024 * 1024  # Each part in a multi-part upload must be smaller than 4000 MiB.
MULTI_FILE_MAX_PART_COUNT = 250  # Maximum number of parts


class FileMetadataContentIO(
    TableDataIO[FileMetadataContentSelectorV2, FileMetadataResponse],
    TableUploadableDataIO[FileMetadataContentSelectorV2, FileMetadataResponse, FileMetadataRequest],
    ConfigurableDataIO[FileMetadataContentSelectorV2, FileMetadataResponse],
):
    """FileMetadataContentIO

    Args:
        client: ToolkitClient
        config_directory: The directory were the filemetadata and manifest files are stored.
        file_directory: On download, location to store file content.
        overwrite: On upload, whether to overwrite existing files.
    """

    CHUNK_SIZE = 10
    KIND = "FileMetadataContent"
    BASE_SELECTOR = FileMetadataContentSelectorV2

    def __init__(
        self,
        client: ToolkitClient,
        config_directory: Path,
        file_directory: Path | None = None,
        overwrite: bool = False,
    ) -> None:
        super().__init__(client)
        self.overwrite = overwrite
        self._config_directory = config_directory
        self._file_directory = file_directory
        self._crud = FileMetadataCRUD(client, None, None, support_upload=False)
        self._downloaded_data_sets_by_selector: dict[FileMetadataContentSelectorV2 | None, set[int]] = defaultdict(set)
        self._downloaded_labels_by_selector: dict[FileMetadataContentSelectorV2 | None, set[ExternalId]] = defaultdict(
            set
        )
        self._downloaded_security_categories_by_selector: dict[FileMetadataContentSelectorV2 | None, set[int]] = (
            defaultdict(set)
        )
        self._metadata_keys: dict[FileMetadataContentSelectorV2 | None, set[str]] = {}

    def _verify_download_selector(self, selector: FileMetadataContentSelectorV2) -> tuple[InternalWithNameId, ...]:
        if isinstance(selector, FileMetadataFilesSelectorV2) and selector.ids:
            return selector.ids
        raise NotImplementedError(f"{selector.type} does not support download")

    def get_schema(self, selector: FileMetadataContentSelectorV2) -> list[SchemaColumn] | None:
        if selector not in self._metadata_keys:
            self._metadata_keys[selector] = set()
            return None
        metadata_schema: list[SchemaColumn] = []
        if metadata_keys := self._metadata_keys[selector]:
            metadata_schema.extend(
                [SchemaColumn(name=f"metadata.{key}", type="string", is_array=False) for key in sorted(metadata_keys)]
            )
        file_schema = [
            SchemaColumn(name="name", type="string"),
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="directory", type="string"),
            SchemaColumn(name="mimeType", type="string"),
            SchemaColumn(name="dataSetExternalId", type="string"),
            SchemaColumn(name="assetExternalIds", type="string", is_array=True),
            SchemaColumn(name="source", type="string"),
            SchemaColumn(name="sourceCreatedTime", type="integer"),
            SchemaColumn(name="sourceModifiedTime", type="integer"),
            SchemaColumn(name="securityCategories", type="string", is_array=True),
            SchemaColumn(name="labels", type="string", is_array=True),
            SchemaColumn(name="geoLocation", type="json"),
            SchemaColumn(name=FILEPATH, type="string"),
        ]
        return file_schema + metadata_schema

    def stream_data(
        self, selector: FileMetadataContentSelectorV2, limit: int | None = None, bookmark: Bookmark | None = None
    ) -> Iterable[Page[FileMetadataResponse]]:
        file_ids = self._verify_download_selector(selector)
        if self._file_directory is None:
            raise ValueError("Bug in Toolkit: File directory must be specified for downloading file content.")
        self._file_directory.mkdir(exist_ok=True, parents=True)
        for chunk in chunker_sequence(file_ids, self.CHUNK_SIZE):
            self.logger.register([item.display_name for item in chunk])
            file_metadata = self.client.tool.filemetadata.retrieve(list(chunk), ignore_unknown_ids=True)
            retrieved_by_id = {file.id: file for file in file_metadata}
            data_items: list[DataItem[FileMetadataResponse]] = []
            for item in chunk:
                if item.id not in retrieved_by_id:
                    self.logger.log(
                        LogEntryV2(
                            id=item.display_name,
                            severity=Severity.skipped,
                            label="Missing in CDF",
                            message=f"File {item.display_name} not found in CDF, skipping.",
                        )
                    )
                    continue
                file = retrieved_by_id[item.id]
                if not file.uploaded:
                    self.logger.log(
                        LogEntryV2(
                            id=item.display_name,
                            severity=Severity.warning,
                            label="Missing file content",
                            message=f"File {item.display_name} metadata found in CDF, but file content is not uploaded.",
                        )
                    )
                else:
                    filepath = self._file_directory / sanitize_filename(file.name)
                    if (
                        filepath.suffix == ""
                        and file.mime_type
                        and (guessed_extension := mimetypes.guess_extension(file.mime_type))
                    ):
                        # Recover file extension if missing.
                        filepath = filepath.with_suffix(guessed_extension)
                    has_downloaded = self._try_download_content(file, filepath, item.display_name)
                    if has_downloaded:
                        file.filepath = filepath
                data_items.append(DataItem(tracking_id=item.display_name, item=file))

            yield Page(worker_id="main", items=data_items)

    def count(self, selector: FileMetadataContentSelectorV2) -> int | None:
        return len(self._verify_download_selector(selector))

    def _try_download_content(self, file_metadata: FileMetadataResponse, destination: Path, tracking_id: str) -> bool:
        """Tries to download the file content to the destination returns whether it was successful."""
        try:
            result = self.client.tool.filemetadata.get_download_url([file_metadata.as_internal_id()])[0]
            if result.download_url:
                self.client.tool.filemetadata.download_file(download_url=result.download_url, destination=destination)
                return True
            message = f"File content download URL not found for {tracking_id}."
        except ToolkitAPIError as err:
            message = f"File content download failed for {tracking_id} with error: {err.message}."
        except IndexError:
            message = f"File content not downloaded for {tracking_id}. CDF did not return a download URL."
        self.logger.log(
            LogEntryV2(id=tracking_id, label="File content download failed", severity=Severity.warning, message=message)
        )
        return False

    def _populate_internal_id_cache(self, data: Page[dict[str, JsonVal]]) -> None:
        data_set_external_ids: set[str] = set()
        asset_external_ids: set[str] = set()
        security_category_names: set[str] = set()
        for item in data:
            json_chunk = item.item
            if isinstance(data_set_external_id := json_chunk.get("dataSetExternalId"), str):
                data_set_external_ids.add(data_set_external_id)
            if isinstance(asset_external_ids_chunk := json_chunk.get("assetExternalIds"), list):
                asset_external_ids.update(
                    asset_external_id
                    for asset_external_id in asset_external_ids_chunk
                    if isinstance(asset_external_id, str)
                )
            if isinstance(security_categories_chunk := json_chunk.get("securityCategories"), list):
                security_category_names.update(
                    security_category_name
                    for security_category_name in security_categories_chunk
                    if isinstance(security_category_name, str)
                )
        self.client.lookup.data_sets.id(list(data_set_external_ids))
        self.client.lookup.assets.id(list(asset_external_ids))
        self.client.lookup.security_categories.id(list(security_category_names))

    def rows_to_data(
        self, rows: Page[dict[str, JsonVal]], selector: FileMetadataContentSelectorV2 | None = None
    ) -> Page[FileMetadataRequest]:
        self._populate_internal_id_cache(rows)
        return super().rows_to_data(rows, selector)

    def json_chunk_to_data(self, data_chunk: Page[dict[str, JsonVal]]) -> Page[FileMetadataRequest]:
        self._populate_internal_id_cache(data_chunk)
        return super().json_chunk_to_data(data_chunk)

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

    def _populate_external_id_cache(
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

        if asset_ids:
            self.client.lookup.assets.external_id(list(asset_ids))
        if data_set_ids:
            self.client.lookup.data_sets.external_id(list(data_set_ids))
        if security_ids:
            self.client.lookup.security_categories.external_id(list(security_ids))

        self._downloaded_data_sets_by_selector[selector].update(data_set_ids)
        self._downloaded_labels_by_selector[selector].update(label_ids)
        self._downloaded_security_categories_by_selector[selector].update(security_ids)

    def data_to_json_chunk(
        self, data_chunk: Page[FileMetadataResponse], selector: FileMetadataContentSelectorV2 | None = None
    ) -> Page[dict[str, JsonVal]]:
        # Ensure data sets/assets/security-categories are looked up to populate cache.
        # This is to avoid looking up each data set id individually in the .dump_resource call
        self._populate_external_id_cache(di.item for di in data_chunk.items)
        if selector in self._metadata_keys:
            self._metadata_keys[selector].update(
                key for item in data_chunk for key in (item.item.metadata or {}).keys()
            )
        dumped: list[DataItem[dict[str, JsonVal]]] = []
        for item in data_chunk.items:
            dumped_item = self._crud.dump_resource(item.item)
            # Preserve filepath
            if item.item.filepath:
                dumped_filepath = item.item.filepath
                if dumped_filepath.is_relative_to(self._config_directory):
                    dumped_filepath = dumped_filepath.relative_to(self._config_directory)
                dumped_item[FILEPATH] = dumped_filepath.as_posix()
            dumped.append(DataItem(tracking_id=item.tracking_id, item=dumped_item))
        return data_chunk.create_from(dumped)

    def json_to_row(
        self, item_json: dict[str, JsonVal], selector: FileMetadataContentSelectorV2 | None = None
    ) -> dict[str, JsonVal]:
        if "metadata" in item_json and isinstance(item_json["metadata"], dict):
            metadata = item_json.pop("metadata")
            # MyPy does understand that metadata is a dict here due to the check above.
            for key, value in metadata.items():  # type: ignore[union-attr]
                item_json[f"metadata.{key}"] = value
        return item_json

    def configurations(self, selector: FileMetadataContentSelectorV2) -> Iterable[StorageIOConfig]:
        data_set_ids = self._downloaded_data_sets_by_selector[selector]
        if data_set_ids:
            data_set_external_ids = [
                ExternalId(external_id=data_set_external_id)
                for data_set_external_id in self.client.lookup.data_sets.external_id(list(data_set_ids))
            ]
            yield from self._configurations(data_set_external_ids, DataSetsIO.create_loader(self.client))

        labels = self._downloaded_labels_by_selector[selector]
        if labels:
            yield from self._configurations(list(labels), LabelIO.create_loader(self.client))
        if security_categories := self._downloaded_security_categories_by_selector[selector]:
            category_ids: list[NameId] = [
                NameId(name=name)
                for name in self.client.lookup.security_categories.external_id(list(security_categories))
            ]
            yield from self._configurations(category_ids, SecurityCategoryIO.create_loader(self.client))

    @classmethod
    def _configurations(
        cls,
        ids: Sequence[Hashable],
        loader: DataSetsIO | LabelIO | SecurityCategoryIO,
    ) -> Iterable[StorageIOConfig]:
        if not ids:
            return

        items = loader.retrieve(ids)  # type: ignore[arg-type]
        yield StorageIOConfig(
            kind=loader.kind,
            folder_name=loader.folder_name,
            # We know that the items will be labels for LabelLoader and data sets for DataSetsLoader
            value=[loader.dump_resource(item) for item in items],  # type: ignore[arg-type]
        )

    def upload_items(
        self,
        data_chunk: Page[FileMetadataRequest],
        http_client: HTTPClient,
        selector: FileMetadataContentSelectorV2 | None = None,
    ) -> ItemsResultList:
        return ItemsResultList([self._upload_single_item(item) for item in data_chunk])

    def _upload_single_item(self, item: DataItem[FileMetadataRequest]) -> ItemsResultMessage:
        request = item.item
        filepath = request.filepath
        if filepath is None:
            return ItemsFailedRequest(
                ids=[item.tracking_id],
                error_message=f"Failed to create {item.tracking_id}. The {FILEPATH} has not been set.",
            )
        if not filepath.is_file():
            candidate = self._config_directory / filepath
            if candidate.is_file():
                filepath = candidate
            else:
                return ItemsFailedRequest(
                    ids=[item.tracking_id],
                    error_message=f"Failed to create {item.tracking_id}. File path {filepath.as_posix()} does not exist.",
                )
        filesize = filepath.stat().st_size
        if filesize > MULTI_FILE_MAX_PART_COUNT * MULTI_FILE_PART_MAX_SIZE_BYTES:
            self.logger.log(
                LogEntryV2(
                    id=item.tracking_id,
                    label="File too large for upload",
                    severity=Severity.failure,
                    message=f"The {item.tracking_id} is {filesize} bytes, which exceeds the maximum "
                    f"supported file size of {MULTI_FILE_MAX_PART_COUNT * MULTI_FILE_PART_MAX_SIZE_BYTES} bytes for upload.",
                )
            )
            return ItemsFailedRequest(ids=[item.tracking_id], error_message=f"Failed to upload {item.tracking_id}.")

        created = self._create_file_metadata(request, item.tracking_id, filesize)

        if not isinstance(created, FileMetadataResponse):
            return created  # Failed request

        return self._upload_file_content(filepath, created, item.tracking_id)

    def _create_file_metadata(
        self, request: FileMetadataRequest, tracking_id: str, filesize: int
    ) -> FileMetadataResponse | ItemsResultMessage:
        parts = filesize // IDEAL_FILE_SIZE
        try:
            # We aim to have each request the size of 50MiB
            if parts <= 1:
                return self.client.tool.filemetadata.create([request], overwrite=self.overwrite)[0]
            parts = min(parts, MULTI_FILE_MAX_PART_COUNT)
            return self.client.tool.filemetadata.upload_multi_parts(request, overwrite=self.overwrite, parts=parts)
        except ToolkitAPIError as error:
            if error.response is not None:
                return error.response.as_item_response(tracking_id)
            raise
        except IndexError:
            return ItemsFailedRequest(
                ids=[tracking_id],
                error_message=f"No response returned from CDF for item {tracking_id}.",
            )

    def _upload_file_content(
        self, filepath: Path, created: FileMetadataResponse, tracking_id: str
    ) -> ItemsResultMessage:
        try:
            if created.upload_url is not None:
                # Upload single
                response = self.client.tool.filemetadata.upload_file(filepath, created.upload_url, created.mime_type)
            elif created.upload_urls and created.upload_id:
                responses = self.client.tool.filemetadata.upload_file_multiparts(
                    filepath, created.upload_urls, created.mime_type
                )
                self.client.tool.filemetadata.complete_multipart_upload(created.as_internal_id(), created.upload_id)
                response = responses[-1]
            else:
                # This should never happen.
                raise NotImplementedError(
                    f"Unexpected response from CDF for item {tracking_id}. No upload URLs provided."
                )
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


_COGNITE_FILE_BASE_SCHEMA_KEYS = frozenset(
    {
        "space",
        "externalId",
        "name",
        "description",
        "tags",
        "aliases",
        "sourceId",
        "sourceContext",
        "source",
        "sourceCreatedTime",
        "sourceUpdatedTime",
        "sourceCreatedUser",
        "sourceUpdatedUser",
        "assets",
        "mimeType",
        "directory",
        "category",
        "type",
        "existingVersion",
        FILEPATH,
    }
)


class CogniteFileContentIO(
    TableDataIO[CogniteFileContentSelectorV2, CogniteFileResponse],
    TableUploadableDataIO[CogniteFileContentSelectorV2, CogniteFileResponse, CogniteFileRequest],
    ConfigurableDataIO[CogniteFileContentSelectorV2, CogniteFileResponse],
):
    """Upload and download binary content for CogniteFile (Data Modeling) nodes.

    Like ``FileMetadataContentIO`` for classic files, but CogniteFile instances are created via the
    instances API; upload URLs are obtained with ``InstanceId`` through ``get_upload_url`` /
    ``get_multipart_upload_urls``. Dependency lookups used for classic file metadata exports are not used.

    Args:
        client: ToolkitClient
        config_directory: Directory where manifests and file path references are stored.
        file_directory: On download, where to write file bytes.
        overwrite: Unused for CogniteFile upserts (kept for API parity with ``FileMetadataContentIO``).
    """

    CHUNK_SIZE = 10
    KIND = "CogniteFileContent"
    BASE_SELECTOR = CogniteFileContentSelectorV2

    def __init__(
        self,
        client: ToolkitClient,
        config_directory: Path,
        file_directory: Path | None = None,
        overwrite: bool = False,
    ) -> None:
        super().__init__(client)
        self.overwrite = overwrite
        self._config_directory = config_directory
        self._file_directory = file_directory
        self._crud = CogniteFileCRUD(client, None, None, support_upload=False)

    def _verify_download_selector(self, selector: CogniteFileContentSelectorV2) -> tuple[NodeWithNameId, ...]:
        if isinstance(selector, CogniteFileFilesSelectorV2) and selector.ids:
            return selector.ids
        raise NotImplementedError(f"{selector.type} does not support download")

    def get_schema(self, selector: CogniteFileContentSelectorV2) -> list[SchemaColumn] | None:
        return [
            SchemaColumn(name="space", type="string"),
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="name", type="string"),
            SchemaColumn(name="description", type="string"),
            SchemaColumn(name="tags", type="string", is_array=True),
            SchemaColumn(name="aliases", type="string", is_array=True),
            SchemaColumn(name="sourceId", type="string"),
            SchemaColumn(name="sourceContext", type="string"),
            SchemaColumn(name="source", type="json"),
            SchemaColumn(name="sourceCreatedTime", type="timestamp"),
            SchemaColumn(name="sourceUpdatedTime", type="timestamp"),
            SchemaColumn(name="sourceCreatedUser", type="string"),
            SchemaColumn(name="sourceUpdatedUser", type="string"),
            SchemaColumn(name="assets", type="json"),
            SchemaColumn(name="mimeType", type="string"),
            SchemaColumn(name="directory", type="string"),
            SchemaColumn(name="category", type="json"),
            SchemaColumn(name="type", type="json"),
            SchemaColumn(name="existingVersion", type="integer"),
            SchemaColumn(name=FILEPATH, type="string"),
        ]

    def stream_data(
        self, selector: CogniteFileContentSelectorV2, limit: int | None = None, bookmark: Bookmark | None = None
    ) -> Iterable[Page[CogniteFileResponse]]:
        file_ids = self._verify_download_selector(selector)
        if self._file_directory is None:
            raise ValueError("Bug in Toolkit: File directory must be specified for downloading file content.")
        self._file_directory.mkdir(exist_ok=True, parents=True)
        for chunk in chunker_sequence(file_ids, self.CHUNK_SIZE):
            self.logger.register([item.display_name for item in chunk])
            cognite_files = self.client.tool.cognite_files.retrieve(list(chunk))
            retrieved_by_key = {(f.space, f.external_id): f for f in cognite_files}
            data_items: list[DataItem[CogniteFileResponse]] = []
            for item in chunk:
                key = (item.space, item.external_id)
                if key not in retrieved_by_key:
                    self.logger.log(
                        LogEntryV2(
                            id=item.display_name,
                            severity=Severity.skipped,
                            label="Missing in CDF",
                            message=f"CogniteFile {item.display_name} not found in CDF, skipping.",
                        )
                    )
                    continue
                file = retrieved_by_key[key]
                if not file.is_uploaded:
                    self.logger.log(
                        LogEntryV2(
                            id=item.display_name,
                            severity=Severity.warning,
                            label="Missing file content",
                            message=f"CogniteFile {item.display_name} found in CDF, but file content is not uploaded.",
                        )
                    )
                else:
                    filepath = self._file_directory / sanitize_filename(file.name or item.external_id)
                    if (
                        filepath.suffix == ""
                        and file.mime_type
                        and (guessed_extension := mimetypes.guess_extension(file.mime_type))
                    ):
                        filepath = filepath.with_suffix(guessed_extension)
                    has_downloaded = self._try_download_content(file, filepath, item.display_name)
                    if has_downloaded:
                        file.filepath = filepath
                data_items.append(DataItem(tracking_id=item.display_name, item=file))

            yield Page(worker_id="main", items=data_items)

    def count(self, selector: CogniteFileContentSelectorV2) -> int | None:
        return len(self._verify_download_selector(selector))

    def _try_download_content(self, cognite_file: CogniteFileResponse, destination: Path, tracking_id: str) -> bool:
        """Download file bytes via the linked classic file entry, if present."""
        try:
            instance_id = InstanceId(instance_id=cognite_file.as_id())
            linked = self.client.tool.filemetadata.retrieve([instance_id], ignore_unknown_ids=True)
            if not linked or not linked[0].uploaded:
                message = f"No uploaded classic file linked to CogniteFile instance for {tracking_id}."
            else:
                fm = linked[0]
                result = self.client.tool.filemetadata.get_download_url([fm.as_internal_id()])[0]
                if result.download_url:
                    self.client.tool.filemetadata.download_file(
                        download_url=result.download_url, destination=destination
                    )
                    return True
                message = f"File content download URL not found for {tracking_id}."
        except ToolkitAPIError as err:
            message = f"File content download failed for {tracking_id} with error: {err.message}."
        except IndexError:
            message = f"File content not downloaded for {tracking_id}. CDF did not return a download URL."
        self.logger.log(
            LogEntryV2(id=tracking_id, label="File content download failed", severity=Severity.warning, message=message)
        )
        return False

    def rows_to_data(
        self, rows: Page[dict[str, JsonVal]], selector: CogniteFileContentSelectorV2 | None = None
    ) -> Page[CogniteFileRequest]:
        return super().rows_to_data(rows, selector)

    def json_chunk_to_data(self, data_chunk: Page[dict[str, JsonVal]]) -> Page[CogniteFileRequest]:
        return super().json_chunk_to_data(data_chunk)

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: CogniteFileContentSelectorV2 | None = None
    ) -> CogniteFileRequest:
        _ = source_id, selector
        return self.json_to_resource(dict(row))

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> CogniteFileRequest:
        # CogniteFileCRUD.load_resource only restores filepath from YAML-side cache; table/template
        # uploads must take $FILEPATH from the row payload explicitly.
        payload = dict(item_json)
        raw_path = payload.pop(FILEPATH, None)
        request = self._crud.load_resource(payload)
        if raw_path is not None:
            if isinstance(raw_path, Path):
                request.filepath = raw_path
            elif isinstance(raw_path, str):
                request.filepath = Path(raw_path)
        return request

    def json_to_row(
        self, item_json: dict[str, JsonVal], selector: CogniteFileContentSelectorV2 | None = None
    ) -> dict[str, JsonVal]:
        _ = selector
        return dict(item_json)

    def data_to_json_chunk(
        self, data_chunk: Page[CogniteFileResponse], selector: CogniteFileContentSelectorV2 | None = None
    ) -> Page[dict[str, JsonVal]]:
        dumped: list[DataItem[dict[str, JsonVal]]] = []
        for item in data_chunk.items:
            dumped_item = self._crud.dump_resource(item.item)
            if item.item.filepath:
                dumped_filepath = item.item.filepath
                if dumped_filepath.is_relative_to(self._config_directory):
                    dumped_filepath = dumped_filepath.relative_to(self._config_directory)
                dumped_item[FILEPATH] = dumped_filepath.as_posix()
            dumped.append(DataItem(tracking_id=item.tracking_id, item=dumped_item))
        return data_chunk.create_from(dumped)

    def configurations(self, selector: CogniteFileContentSelectorV2) -> Iterable[StorageIOConfig]:
        _ = selector
        yield from ()

    def upload_items(
        self,
        data_chunk: Page[CogniteFileRequest],
        http_client: HTTPClient,
        selector: CogniteFileContentSelectorV2 | None = None,
    ) -> ItemsResultList:
        return ItemsResultList([self._upload_single_item(item) for item in data_chunk])

    def _upload_single_item(self, item: DataItem[CogniteFileRequest]) -> ItemsResultMessage:
        request = item.item
        filepath = request.filepath
        if filepath is None:
            return ItemsFailedRequest(
                ids=[item.tracking_id],
                error_message=f"Failed to create {item.tracking_id}. The {FILEPATH} has not been set.",
            )
        if not filepath.is_file():
            candidate = self._config_directory / filepath
            if candidate.is_file():
                filepath = candidate
            else:
                return ItemsFailedRequest(
                    ids=[item.tracking_id],
                    error_message=f"Failed to create {item.tracking_id}. File path {filepath.as_posix()} does not exist.",
                )
        filesize = filepath.stat().st_size
        if filesize > MULTI_FILE_MAX_PART_COUNT * MULTI_FILE_PART_MAX_SIZE_BYTES:
            self.logger.log(
                LogEntryV2(
                    id=item.tracking_id,
                    label="File too large for upload",
                    severity=Severity.failure,
                    message=f"The {item.tracking_id} is {filesize} bytes, which exceeds the maximum "
                    f"supported file size of {MULTI_FILE_MAX_PART_COUNT * MULTI_FILE_PART_MAX_SIZE_BYTES} bytes for upload.",
                )
            )
            return ItemsFailedRequest(ids=[item.tracking_id], error_message=f"Failed to upload {item.tracking_id}.")

        create_result = self._create_cognite_file(request, item.tracking_id)
        if create_result is not None:
            return create_result

        return self._upload_file_content_with_instance_id(filepath, request, filesize, item.tracking_id)

    def _create_cognite_file(self, request: CogniteFileRequest, tracking_id: str) -> ItemsResultMessage | None:
        try:
            self.client.tool.cognite_files.create([request])
        except ToolkitAPIError as error:
            if error.response is not None:
                return error.response.as_item_response(tracking_id)
            raise
        except IndexError:
            return ItemsFailedRequest(
                ids=[tracking_id],
                error_message=f"No response returned from CDF for item {tracking_id}.",
            )
        return None

    def _upload_file_content_with_instance_id(
        self, filepath: Path, request: CogniteFileRequest, filesize: int, tracking_id: str
    ) -> ItemsResultMessage:
        instance_id = request.as_instance_id()
        part_count = filesize // IDEAL_FILE_SIZE
        try:
            if part_count <= 1:
                url_items = self.client.tool.filemetadata.get_upload_url([instance_id])
                if not url_items:
                    return ItemsFailedRequest(
                        ids=[tracking_id],
                        error_message=f"No upload URL returned from CDF for item {tracking_id}.",
                    )
                url_info = url_items[0]
                if url_info.upload_url is None:
                    return ItemsFailedRequest(
                        ids=[tracking_id],
                        error_message=f"No upload URL returned from CDF for item {tracking_id}.",
                    )
                response = self.client.tool.filemetadata.upload_file(filepath, url_info.upload_url, request.mime_type)
                return response.as_item_response(tracking_id)
            n_parts = min(part_count, MULTI_FILE_MAX_PART_COUNT)
            url_info = self.client.tool.filemetadata.get_multipart_upload_urls(instance_id, n_parts)
            if not url_info.upload_urls or not url_info.upload_id:
                return ItemsFailedRequest(
                    ids=[tracking_id],
                    error_message=f"No multipart upload URLs returned from CDF for item {tracking_id}.",
                )
            responses = self.client.tool.filemetadata.upload_file_multiparts(
                filepath, url_info.upload_urls, request.mime_type
            )
            self.client.tool.filemetadata.complete_multipart_upload(instance_id, url_info.upload_id)
            return responses[-1].as_item_response(tracking_id)
        except ToolkitAPIError as error:
            if error.response is not None:
                return error.response.as_item_response(tracking_id)
            raise

    @classmethod
    def read_chunks(
        cls, reader: MultiFileReader, selector: CogniteFileContentSelectorV2
    ) -> Iterable[Page[dict[str, JsonVal]]]:
        if not isinstance(selector, CogniteFileTemplateSelectorV2):
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
    def count_items(cls, reader: MultiFileReader, selector: CogniteFileContentSelectorV2 | None = None) -> int:
        if not isinstance(selector, CogniteFileTemplateSelectorV2):
            return super().count_items(reader, selector)
        return len(reader.input_files)
