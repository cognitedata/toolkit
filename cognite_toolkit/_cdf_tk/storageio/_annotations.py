from collections.abc import Iterable, Sequence

from cognite.client.data_classes import Annotation, AnnotationFilter

from cognite_toolkit._cdf_tk.storageio import Page
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._asset_centric import FileMetadataIO
from ._base import StorageIO
from .selectors import AssetCentricSelector


class FileAnnotationIO(StorageIO[AssetCentricSelector, Annotation]):
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    CHUNK_SIZE = 1000
    BASE_SELECTOR = AssetCentricSelector

    def as_id(self, item: Annotation) -> str:
        project = item._cognite_client.config.project
        return f"INTERNAL_ID_project_{project}_{item.id!s}"

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[Page]:
        total = 0
        for file_chunk in FileMetadataIO(self.client).stream_data(selector, None):
            # Todo Support pagination. This is missing in the SDK.
            results = self.client.annotations.list(
                filter=AnnotationFilter(
                    annotated_resource_type="file",
                    annotated_resource_ids=[{"id": file_metadata.id} for file_metadata in file_chunk.items],
                )
            )
            for chunk in chunker_sequence(results, self.CHUNK_SIZE):
                if limit is not None and total >= limit:
                    return
                yield Page(worker_id="main", items=chunk)
                total += len(chunk)
                if limit is not None and total >= limit:
                    return

    def count(self, selector: AssetCentricSelector) -> int | None:
        """There is no efficient way to count annotations in CDF."""
        return None

    def data_to_json_chunk(
        self, data_chunk: Sequence[Annotation], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError("AnnotationIO does not support exporting to JSON.")
