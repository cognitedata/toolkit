import json
import mimetypes
from collections.abc import Iterable, MutableSequence, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from cognite.client.data_classes import FileMetadata, FileMetadataWrite

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import FileMetadataCRUD
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader
from cognite_toolkit._cdf_tk.utils.http_client import (
    DataBodyRequest,
    ErrorDetails,
    FailedResponseItems,
    HTTPClient,
    HTTPMessage,
    SimpleBodyRequest,
)
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import T_Selector, UploadItem
from ._base import Page, UploadableStorageIO
from .selectors import FileContentSelector
from .selectors._file_content import FILEPATH


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
        selector: T_Selector | None = None,
    ) -> Sequence[HTTPMessage]:
        config = http_client.config
        results: MutableSequence[HTTPMessage] = []
        for item in cast(Sequence[UploadFileContentItem], data_chunk):
            responses = http_client.request_with_retries(
                message=SimpleBodyRequest(
                    endpoint_url=config.create_api_url(self.UPLOAD_ENDPOINT),
                    method="POST",
                    # MyPy does not understand that .dump is valid json
                    body_content=item.dump(),  # type: ignore[arg-type]
                )
            )
            try:
                body = responses.get_first_body()
            except ValueError:
                results.extend(responses.as_item_responses(item.as_id()))
                continue
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
