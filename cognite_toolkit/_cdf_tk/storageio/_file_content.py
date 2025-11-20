from collections.abc import Iterable, Sequence

from cognite.client.data_classes import FileMetadata, FileMetadataWrite

from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import Page, UploadableStorageIO
from .selectors import FileContentSelector


class FileContentIO(UploadableStorageIO[FileContentSelector, FileMetadata, FileMetadataWrite]):
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    CHUNK_SIZE = 100
    BASE_SELECTOR = FileContentSelector
    KIND = "FileContent"
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    UPLOAD_ENDPOINT = "/files"

    def as_id(self, item: FileMetadata) -> str:
        raise NotImplementedError()

    def stream_data(self, selector: FileContentSelector, limit: int | None = None) -> Iterable[Page]:
        raise NotImplementedError()

    def count(self, selector: FileContentSelector) -> int | None:
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: Sequence[FileMetadata], selector: FileContentSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError()

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> FileMetadataWrite:
        raise NotImplementedError()
