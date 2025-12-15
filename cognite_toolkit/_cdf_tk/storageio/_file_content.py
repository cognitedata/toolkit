import json
import mimetypes
from collections.abc import Iterable, MutableSequence, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import httpx
from cognite.client.data_classes import FileMetadata, FileMetadataWrite
from cognite.client.data_classes.data_modeling import NodeId, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import FileMetadataCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.protocols import ResourceResponseProtocol
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.collection import chunker, chunker_sequence
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader
from cognite_toolkit._cdf_tk.utils.http_client import (
    DataBodyRequest,
    ErrorDetails,
    FailedResponse,
    FailedResponseItems,
    HTTPClient,
    HTTPMessage,
    ResponseList,
    SimpleBodyRequest,
)
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import Page, UploadableStorageIO, UploadItem
from .selectors import FileContentSelector, FileIdentifierSelector, FileMetadataTemplateSelector
from .selectors._file_content import (
    FILEPATH,
    FileDataModelingTemplateSelector,
    FileExternalID,
    FileIdentifier,
    FileInstanceID,
    FileInternalID,
    FileTemplateSelector,
)
from .selectors._file_content import NodeId as SelectorNodeId

COGNITE_FILE_VIEW = ViewId("cdf_cdm", "CogniteFile", "v1")


@dataclass
class UploadFileContentItem(UploadItem[FileMetadataWrite]):
    file_path: Path
    mime_type: str


@dataclass
class MetadataWithFilePath(ResourceResponseProtocol):
    metadata: FileMetadata
    file_path: Path

    def as_write(self) -> FileMetadataWrite:
        return self.metadata.as_write()


class FileContentIO(UploadableStorageIO[FileContentSelector, MetadataWithFilePath, FileMetadataWrite]):
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    CHUNK_SIZE = 10
    BASE_SELECTOR = FileContentSelector
    KIND = "FileContent"
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    UPLOAD_ENDPOINT = "/files"

    def __init__(self, client: ToolkitClient, target_dir: Path = Path.cwd()) -> None:
        super().__init__(client)
        self._crud = FileMetadataCRUD(client, None, None)
        self._target_dir = target_dir

    def as_id(self, item: MetadataWithFilePath) -> str:
        return item.metadata.external_id or str(item.metadata.id)

    def stream_data(
        self, selector: FileContentSelector, limit: int | None = None
    ) -> Iterable[Page[MetadataWithFilePath]]:
        if not isinstance(selector, FileIdentifierSelector):
            raise ToolkitNotImplementedError(
                f"Download with the manifest, {type(selector).__name__}, is not supported for FileContentIO"
            )
        selected_identifiers = selector.identifiers
        if limit is not None and limit < len(selected_identifiers):
            selected_identifiers = selected_identifiers[:limit]
        for identifiers in chunker_sequence(selected_identifiers, self.CHUNK_SIZE):
            metadata = self._retrieve_metadata(identifiers)
            if metadata is None:
                continue
            identifiers_map = self._as_metadata_map(metadata)
            downloaded_files: list[MetadataWithFilePath] = []
            for identifier in identifiers:
                if identifier not in identifiers_map:
                    continue

                meta = identifiers_map[identifier]
                filepath = self._create_filepath(meta, selector)
                download_url = self._retrieve_download_url(identifier)
                if download_url is None:
                    continue
                filepath.parent.mkdir(parents=True, exist_ok=True)
                with httpx.stream("GET", download_url) as response:
                    if response.status_code != 200:
                        continue
                    with filepath.open(mode="wb") as file_stream:
                        for chunk in response.iter_bytes(chunk_size=8192):
                            file_stream.write(chunk)
                downloaded_files.append(
                    MetadataWithFilePath(
                        metadata=meta,
                        file_path=filepath.relative_to(self._target_dir),
                    )
                )
            yield Page(items=downloaded_files, worker_id="Main")

    def _retrieve_metadata(self, identifiers: Sequence[FileIdentifier]) -> Sequence[FileMetadata] | None:
        config = self.client.config
        responses = self.client.http_client.request_with_retries(
            message=SimpleBodyRequest(
                endpoint_url=config.create_api_url("/files/byids"),
                method="POST",
                body_content={
                    "items": [
                        identifier.model_dump(mode="json", by_alias=True, exclude={"id_type"})
                        for identifier in identifiers
                    ],
                    "ignoreUnknownIds": True,
                },
            )
        )
        if responses.has_failed:
            return None
        body = responses.get_first_body()
        items_data = body.get("items", [])
        if not isinstance(items_data, list):
            return None
        # MyPy does not understand that JsonVal is valid dict[Any, Any]
        return [FileMetadata._load(item) for item in items_data]  # type: ignore[arg-type]

    @staticmethod
    def _as_metadata_map(metadata: Sequence[FileMetadata]) -> dict[FileIdentifier, FileMetadata]:
        identifiers_map: dict[FileIdentifier, FileMetadata] = {}
        for item in metadata:
            if item.id is not None:
                identifiers_map[FileInternalID(internal_id=item.id)] = item
            if item.external_id is not None:
                identifiers_map[FileExternalID(external_id=item.external_id)] = item
            if item.instance_id is not None:
                identifiers_map[
                    FileInstanceID(
                        instance_id=SelectorNodeId(
                            space=item.instance_id.space, external_id=item.instance_id.external_id
                        )
                    )
                ] = item
        return identifiers_map

    def _create_filepath(self, meta: FileMetadata, selector: FileIdentifierSelector) -> Path:
        # We now that metadata always have name set
        filename = Path(sanitize_filename(cast(str, meta.name)))
        if len(filename.suffix) == 0 and meta.mime_type:
            if mime_ext := mimetypes.guess_extension(meta.mime_type):
                filename = filename.with_suffix(mime_ext)
        directory = sanitize_filename(selector.file_directory)
        if isinstance(meta.directory, str) and meta.directory != "":
            directory = sanitize_filename(meta.directory.removeprefix("/"))

        counter = 1
        filepath = self._target_dir / directory / filename
        while filepath.exists():
            filepath = self._target_dir / directory / f"{filename} ({counter})"
            counter += 1

        return filepath

    def _retrieve_download_url(self, identifier: FileIdentifier) -> str | None:
        config = self.client.config
        responses = self.client.http_client.request_with_retries(
            message=SimpleBodyRequest(
                endpoint_url=config.create_api_url("/files/downloadlink"),
                method="POST",
                body_content={"items": [identifier.model_dump(mode="json", by_alias=True, exclude={"id_type"})]},
            )
        )
        try:
            body = responses.get_first_body()
        except ValueError:
            return None
        if "items" in body and isinstance(body["items"], list) and len(body["items"]) > 0:
            # The API responses is not following the API docs, this is a workaround
            body = body["items"][0]  # type: ignore[assignment]
        try:
            return cast(str, body["downloadUrl"])
        except (KeyError, IndexError):
            return None

    def count(self, selector: FileContentSelector) -> int | None:
        if isinstance(selector, FileIdentifierSelector):
            return len(selector.identifiers)
        return None

    def data_to_json_chunk(
        self, data_chunk: Sequence[MetadataWithFilePath], selector: FileContentSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        """Convert a writable Cognite resource list to a JSON-compatible chunk of data.

        Args:
            data_chunk: A writable Cognite resource list representing the data.
            selector: The selector used for the data. (Not used in this implementation)
        Returns:
            A list of dictionaries, each representing the data in a JSON-compatible format.
        """
        result: list[dict[str, JsonVal]] = []
        for item in data_chunk:
            item_json = self._crud.dump_resource(item.metadata)
            item_json[FILEPATH] = item.file_path.as_posix()
            result.append(item_json)
        return result

    def json_chunk_to_data(self, data_chunk: list[tuple[str, dict[str, JsonVal]]]) -> Sequence[UploadFileContentItem]:
        """Convert a JSON-compatible chunk of data back to a writable Cognite resource list.

        Args:
            data_chunk: A list of tuples, each containing a source ID and a dictionary representing
                the data in a JSON-compatible format.
        Returns:
            A writable Cognite resource list representing the data.
        """
        result: list[UploadFileContentItem] = []
        for source_id, item_json in data_chunk:
            item = self.json_to_resource(item_json)
            filepath = Path(cast(str | Path, item_json[FILEPATH]))
            mime_type, _ = mimetypes.guess_type(filepath)
            # application/octet-stream is the standard fallback for binary data when the type is unknown. (at least Claude thinks so)
            result.append(
                UploadFileContentItem(
                    source_id=source_id,
                    item=item,
                    file_path=filepath,
                    mime_type=mime_type or "application/octet-stream",
                )
            )
        return result

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> FileMetadataWrite:
        return self._crud.load_resource(item_json)

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[FileMetadataWrite]],
        http_client: HTTPClient,
        selector: FileContentSelector | None = None,
    ) -> Sequence[HTTPMessage]:
        results: MutableSequence[HTTPMessage] = []
        if isinstance(selector, FileMetadataTemplateSelector | FileIdentifierSelector):
            upload_url_getter = self._upload_url_asset_centric
        elif isinstance(selector, FileDataModelingTemplateSelector):
            upload_url_getter = self._upload_url_data_modeling
        elif selector is None:
            raise ToolkitNotImplementedError("Selector must be provided for FileContentIO upload")
        else:
            raise ToolkitNotImplementedError(
                f"Upload for the given selector, {type(selector).__name__}, is not supported for FileContentIO"
            )

        for item in cast(Sequence[UploadFileContentItem], data_chunk):
            if not (upload_url := upload_url_getter(item, http_client, results)):
                continue

            content_bytes = item.file_path.read_bytes()
            upload_response = http_client.request_with_retries(
                message=DataBodyRequest(
                    endpoint_url=upload_url,
                    method="PUT",
                    content_type=item.mime_type,
                    data_content=content_bytes,
                    content_length=len(content_bytes),
                )
            )
            results.extend(upload_response.as_item_responses(item.as_id()))
        return results

    def _upload_url_asset_centric(
        self, item: UploadFileContentItem, http_client: HTTPClient, results: MutableSequence[HTTPMessage]
    ) -> str | None:
        responses = http_client.request_with_retries(
            message=SimpleBodyRequest(
                endpoint_url=http_client.config.create_api_url(self.UPLOAD_ENDPOINT),
                method="POST",
                # MyPy does not understand that .dump is valid json
                body_content=item.dump(),  # type: ignore[arg-type]
            )
        )
        return self._parse_upload_link_response(responses, item, results)

    def _upload_url_data_modeling(
        self,
        item: UploadFileContentItem,
        http_client: HTTPClient,
        results: MutableSequence[HTTPMessage],
        created_node: bool = False,
    ) -> str | None:
        """Get upload URL for data modeling file upload.

        We first try to get the upload link assuming the CogniteFile node already exists.
        If we get a "not found" error, we create the CogniteFile node and try again.

        Args:
            item: The upload item containing file metadata.
            http_client: The HTTP client to use for requests.
            results: A mutable sequence to collect HTTP messages and errors.
            created_node: A flag indicating whether the CogniteFile node has already been created.
                This prevents infinite recursion.

        Returns:
            The upload URL as a string, or None if there was an error.

        """
        # We know that instance_id is always set for data modeling uploads
        instance_id = cast(NodeId, item.item.instance_id)
        responses = http_client.request_with_retries(
            message=SimpleBodyRequest(
                endpoint_url=http_client.config.create_api_url("/files/uploadlink"),
                method="POST",
                body_content={"items": [{"instanceId": instance_id.dump(include_instance_type=False)}]},  # type: ignore[dict-item]
            )
        )
        # We know there is only one response since we only requested one upload link
        response = responses[0]
        if isinstance(response, FailedResponse) and response.error.missing and not created_node:
            if self._create_cognite_file_node(instance_id, http_client, item.as_id(), results):
                return self._upload_url_data_modeling(item, http_client, results, created_node=True)
            else:
                return None

        return self._parse_upload_link_response(responses, item, results)

    @classmethod
    def _create_cognite_file_node(
        cls, instance_id: NodeId, http_client: HTTPClient, upload_id: str, results: MutableSequence[HTTPMessage]
    ) -> bool:
        node_creation = http_client.request_with_retries(
            message=SimpleBodyRequest(
                endpoint_url=http_client.config.create_api_url("/models/instances"),
                method="POST",
                body_content={
                    "items": [
                        {
                            "space": instance_id.space,
                            "externalId": instance_id.external_id,
                            "instanceType": "node",
                            # When we create a node with properties in CogniteFile View even with empty properties,
                            # CDF will fill in empty values for all properties defined in the view (note this is only
                            # possible because CogniteFile view has all properties as optional). This includes properties
                            # in the CogniteFile container, which will trigger the file syncer to create a FileMetadata
                            # and link it to the CogniteFile node.
                            "sources": [{"source": COGNITE_FILE_VIEW.dump(include_type=True), "properties": {}}],  # type: ignore[dict-item]
                        }
                    ]
                },
            )
        )
        try:
            _ = node_creation.get_first_body()
        except ValueError:
            results.extend(node_creation.as_item_responses(upload_id))
            return False
        return True

    @classmethod
    def _parse_upload_link_response(
        cls, responses: ResponseList, item: UploadFileContentItem, results: MutableSequence[HTTPMessage]
    ) -> str | None:
        try:
            body = responses.get_first_body()
        except ValueError:
            results.extend(responses.as_item_responses(item.as_id()))
            return None

        if "items" in body and isinstance(body["items"], list) and len(body["items"]) > 0:
            body = body["items"][0]  # type: ignore[assignment]
        try:
            upload_url = cast(str, body["uploadUrl"])
        except (KeyError, IndexError):
            results.append(
                FailedResponseItems(
                    status_code=200,
                    body=json.dumps(body),
                    error=ErrorDetails(code=200, message="Malformed response"),
                    ids=[item.as_id()],
                )
            )
            return None
        return upload_url

    @classmethod
    def read_chunks(
        cls, reader: MultiFileReader, selector: FileContentSelector
    ) -> Iterable[list[tuple[str, dict[str, JsonVal]]]]:
        if isinstance(selector, FileTemplateSelector):
            for chunk in chunker_sequence(reader.input_files, cls.CHUNK_SIZE):
                batch: list[tuple[str, dict[str, JsonVal]]] = []
                for file_path in chunk:
                    metadata = selector.create_instance(file_path)
                    metadata[FILEPATH] = file_path
                    batch.append((file_path.as_posix(), metadata))
                yield batch
        elif isinstance(selector, FileIdentifierSelector):
            for item_chunk in chunker(reader.read_chunks(), cls.CHUNK_SIZE):
                batch = []
                for item in item_chunk:
                    if FILEPATH not in item:
                        # Todo Log warning
                        continue
                    try:
                        file_path = Path(item[FILEPATH])
                    except KeyError:
                        # Todo Log warning
                        continue
                    if not file_path.is_absolute():
                        file_path = reader.input_file.parent / file_path
                    item[FILEPATH] = file_path
                    batch.append((file_path.as_posix(), item))
                yield batch
        else:
            raise ToolkitNotImplementedError(
                f"Reading with the manifest, {type(selector).__name__}, is not supported for FileContentIO"
            )

    @classmethod
    def count_chunks(cls, reader: MultiFileReader) -> int:
        return len(reader.input_files)
