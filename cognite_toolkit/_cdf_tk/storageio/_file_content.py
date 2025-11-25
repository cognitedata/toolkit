import json
import mimetypes
from collections.abc import Iterable, MutableSequence, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from cognite.client.data_classes import FileMetadata, FileMetadataWrite
from cognite.client.data_classes.data_modeling import NodeId, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import FileMetadataCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
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
from .selectors import FileContentSelector, FileMetadataTemplateSelector
from .selectors._file_content import FILEPATH, FileDataModelingTemplateSelector

COGNITE_FILE_VIEW = ViewId("cdf_cdm", "CogniteFile", "v1")


@dataclass
class UploadFileContentItem(UploadItem[FileMetadataWrite]):
    file_path: Path
    mime_type: str


class FileContentIO(UploadableStorageIO[FileContentSelector, FileMetadata, FileMetadataWrite]):
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    CHUNK_SIZE = 10
    BASE_SELECTOR = FileContentSelector
    KIND = "FileContent"
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    UPLOAD_ENDPOINT = "/files"

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._crud = FileMetadataCRUD(client, None, None)

    def as_id(self, item: FileMetadata) -> str:
        return item.external_id or str(item.id)

    def stream_data(self, selector: FileContentSelector, limit: int | None = None) -> Iterable[Page]:
        raise NotImplementedError("Download of FileContent is not yet supported")

    def count(self, selector: FileContentSelector) -> int | None:
        return None

    def data_to_json_chunk(
        self, data_chunk: Sequence[FileMetadata], selector: FileContentSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError("Download of FileContent is not yet supported")

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
            filepath = cast(Path, item_json[FILEPATH])
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
        if isinstance(selector, FileMetadataTemplateSelector):
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

            upload_response = http_client.request_with_retries(
                message=DataBodyRequest(
                    endpoint_url=upload_url,
                    method="PUT",
                    content_type=item.mime_type,
                    data_content=item.file_path.read_bytes(),
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
        if isinstance(response, FailedResponse) and "not found" in response.error.message and not created_node:
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
                            **instance_id.dump(include_instance_type=True),
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
        for chunk in chunker_sequence(reader.input_files, cls.CHUNK_SIZE):
            batch: list[tuple[str, dict[str, JsonVal]]] = []
            for file_path in chunk:
                metadata = selector.create_instance(file_path)
                metadata[FILEPATH] = file_path
                batch.append((str(file_path), metadata))
            yield batch
