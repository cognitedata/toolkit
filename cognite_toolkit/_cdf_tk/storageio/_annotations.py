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

    MISSING_ID = "<MISSING_RESOURCE_ID>"

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
            if limit is not None and total + len(results) > limit:
                results = results[: limit - total]

            for chunk in chunker_sequence(results, self.CHUNK_SIZE):
                yield Page(worker_id="main", items=chunk)
                total += len(chunk)
            if limit is not None and total >= limit:
                break

    def count(self, selector: AssetCentricSelector) -> int | None:
        """There is no efficient way to count annotations in CDF."""
        return None

    def data_to_json_chunk(
        self, data_chunk: Sequence[Annotation], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError("AnnotationIO does not support exporting to JSON.")

    def dump_annotation_to_json(self, annotation: Annotation) -> dict[str, JsonVal]:
        """Dump annotations to a list of JSON serializable dictionaries.

        Args:
            annotation: The annotations to dump.

        Returns:
            A list of JSON serializable dictionaries representing the annotations.
        """
        dumped = annotation.as_write().dump()
        if "annotatedResourceId" in dumped:
            annotated_resource_id = dumped.pop("annotatedResourceId")
            if isinstance(annotated_resource_id, int):
                external_id = self.client.lookup.files.external_id(annotated_resource_id)
                dumped["annotatedResourceExternalId"] = self.MISSING_ID if external_id is None else external_id
        data = dumped.get("data")
        if isinstance(data, dict):
            if "fileRef" in data:
                file_ref = data["fileRef"]
                if isinstance(file_ref, dict):
                    id_ = file_ref.get("id")
                    if isinstance(id_, int):
                        external_id = self.client.lookup.files.external_id(file_ref.pop("id"))
                        data["externalId"] = self.MISSING_ID if external_id is None else external_id
            if "assetRef" in data:
                asset_ref = data["assetRef"]
                if isinstance(asset_ref, dict):
                    id_ = asset_ref.get("id")
                    if isinstance(id_, int):
                        external_id = self.client.lookup.assets.external_id(asset_ref.pop("id"))
                        data["externalId"] = self.MISSING_ID if external_id is None else external_id
        return dumped
