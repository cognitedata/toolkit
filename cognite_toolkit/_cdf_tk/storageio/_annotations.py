from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.identifiers.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import AnnotationFilter
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import (
    AnnotationResponse,
    AnnotationType,
    AssetLinkData,
    FileLinkData,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._asset_centric import FileMetadataIO
from ._base import Page, StorageIO
from .selectors import AssetCentricSelector


class AnnotationIO(StorageIO[AssetCentricSelector, AnnotationResponse]):
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    CHUNK_SIZE = 1000
    BASE_SELECTOR = AssetCentricSelector

    MISSING_ID = "<MISSING_RESOURCE_ID>"

    def as_id(self, item: AnnotationResponse) -> str:
        project = self.client.config.project
        return f"INTERNAL_ID_project_{project}_{item.id!s}"

    def stream_data(
        self, selector: AssetCentricSelector, limit: int | None = None
    ) -> Iterable[Page[AnnotationResponse]]:
        total = 0
        annotation_types: list[AnnotationType] = ["diagrams.AssetLink", "diagrams.FileLink"]
        for file_chunk in FileMetadataIO(self.client).stream_data(selector, None):
            for annotation_type in annotation_types:
                annotation_filter = AnnotationFilter(
                    annotated_resource_type="file",
                    annotated_resource_ids=[InternalId(id=fm.id) for fm in file_chunk.items],
                    annotation_type=annotation_type,
                )
                remaining = limit - total if limit is not None else None
                for page_items in self.client.tool.annotations.iterate(filter=annotation_filter, limit=remaining):
                    for chunk in chunker_sequence(page_items, self.CHUNK_SIZE):
                        yield Page(worker_id="main", items=chunk)
                        total += len(chunk)
                        if limit is not None and total >= limit:
                            return

    def count(self, selector: AssetCentricSelector) -> int | None:
        """There is no efficient way to count annotations in CDF."""
        return None

    def data_to_json_chunk(
        self, data_chunk: Sequence[AnnotationResponse], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        files_ids: set[int] = set()
        for item in data_chunk:
            if item.annotated_resource_type == "file":
                files_ids.add(item.annotated_resource_id)
            if file_id := self._get_file_id(item.data):
                files_ids.add(file_id)
        self.client.lookup.files.external_id(list(files_ids))  # Preload file external IDs
        asset_ids = {asset_id for item in data_chunk if (asset_id := self._get_asset_id(item.data))}
        self.client.lookup.assets.external_id(list(asset_ids))  # Preload asset external IDs
        return [self.dump_annotation_to_json(item) for item in data_chunk]

    def dump_annotation_to_json(self, annotation: AnnotationResponse) -> dict[str, JsonVal]:
        """Dump annotations to a list of JSON serializable dictionaries.

        Args:
            annotation: The annotations to dump.

        Returns:
            A list of JSON serializable dictionaries representing the annotations.
        """
        dumped = annotation.as_request_resource().dump()
        if isinstance(annotated_resource_id := dumped.pop("annotatedResourceId", None), int):
            external_id = self.client.lookup.files.external_id(annotated_resource_id)
            dumped["annotatedResourceExternalId"] = self.MISSING_ID if external_id is None else external_id

        if isinstance(data := dumped.get("data"), dict):
            if isinstance(file_ref := data.get("fileRef"), dict) and isinstance(file_ref.get("id"), int):
                external_id = self.client.lookup.files.external_id(file_ref.pop("id"))
                file_ref["externalId"] = self.MISSING_ID if external_id is None else external_id
            if isinstance(asset_ref := data.get("assetRef"), dict) and isinstance(asset_ref.get("id"), int):
                external_id = self.client.lookup.assets.external_id(asset_ref.pop("id"))
                asset_ref["externalId"] = self.MISSING_ID if external_id is None else external_id
        return dumped

    @classmethod
    def _get_file_id(cls, data: AssetLinkData | FileLinkData | dict[str, Any]) -> int | None:
        if isinstance(data, FileLinkData) and isinstance(data.file_ref, InternalId):
            return data.file_ref.id
        return None

    @classmethod
    def _get_asset_id(cls, data: AssetLinkData | FileLinkData | dict[str, Any]) -> int | None:
        if isinstance(data, AssetLinkData) and isinstance(data.asset_ref, InternalId):
            return data.asset_ref.id
        return None
