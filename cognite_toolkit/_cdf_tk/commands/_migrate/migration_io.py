from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import ClassVar, Literal, cast

from cognite.client.data_classes import Annotation
from cognite.client.data_classes.data_modeling import EdgeId, InstanceApply, NodeId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    RequestMessage,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.http_client._item_classes import (
    ItemsFailedResponse,
    ItemsRequest,
    ItemsResultList,
    ItemsSuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.pending_instances_ids import PendingInstanceId
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicResponse,
    AssetMappingDMRequest,
    ThreeDModelResponse,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import ThreeDMigrationRequest
from cognite_toolkit._cdf_tk.constants import MISSING_EXTERNAL_ID, MISSING_INSTANCE_SPACE
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError, ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import (
    AnnotationIO,
    HierarchyIO,
    InstanceIO,
    T_Selector,
    UploadableStorageIO,
)
from cognite_toolkit._cdf_tk.storageio._base import Page, UploadItem
from cognite_toolkit._cdf_tk.storageio.selectors import (
    ThreeDModelFilteredSelector,
    ThreeDModelIdSelector,
    ThreeDSelector,
)
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import (
    AssetCentricKindExtended,
    AssetCentricType,
    JsonVal,
)
from cognite_toolkit._cdf_tk.utils.useful_types2 import T_AssetCentricResource

from .data_classes import (
    AnnotationMapping,
    AssetCentricMapping,
    AssetCentricMappingList,
    MigrationMapping,
    MigrationMappingList,
)
from .data_model import INSTANCE_SOURCE_VIEW_ID
from .default_mappings import ASSET_ANNOTATIONS_ID, FILE_ANNOTATIONS_ID
from .selectors import AssetCentricMigrationSelector, MigrateDataSetSelector, MigrationCSVFileSelector


class AssetCentricMigrationIO(
    UploadableStorageIO[AssetCentricMigrationSelector, AssetCentricMapping[T_AssetCentricResource], InstanceApply]
):
    KIND = "AssetCentricMigration"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    CHUNK_SIZE = 1000
    UPLOAD_ENDPOINT = InstanceIO.UPLOAD_ENDPOINT

    PENDING_INSTANCE_ID_ENDPOINT_BY_KIND: ClassVar[Mapping[AssetCentricKindExtended, str]] = {
        "TimeSeries": "/timeseries/set-pending-instance-ids",
        "FileMetadata": "/files/set-pending-instance-ids",
    }

    def __init__(self, client: ToolkitClient, skip_linking: bool = True) -> None:
        super().__init__(client)
        self.hierarchy = HierarchyIO(client)
        self.skip_linking = skip_linking

    def as_id(self, item: AssetCentricMapping) -> str:
        return str(item.mapping.as_asset_centric_id())

    def stream_data(self, selector: AssetCentricMigrationSelector, limit: int | None = None) -> Iterator[Page]:
        if isinstance(selector, MigrationCSVFileSelector):
            iterator = self._stream_from_csv(selector, limit)
        elif isinstance(selector, MigrateDataSetSelector):
            iterator = self._stream_given_dataset(selector, limit)
        else:
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for stream_data")
        yield from (Page(worker_id="main", items=items) for items in iterator)

    def _stream_from_csv(
        self, selector: MigrationCSVFileSelector, limit: int | None = None
    ) -> Iterator[Sequence[AssetCentricMapping[T_AssetCentricResource]]]:
        items = selector.items
        if limit is not None:
            items = MigrationMappingList(items[:limit])
        chunk: list[AssetCentricMapping[T_AssetCentricResource]] = []
        for current_batch in chunker_sequence(items, self.CHUNK_SIZE):
            resources = self.hierarchy.get_resource_io(selector.kind).retrieve(current_batch.get_ids())
            for mapping, resource in zip(current_batch, resources, strict=True):
                chunk.append(AssetCentricMapping(mapping=mapping, resource=resource))
            if chunk:
                yield chunk
                chunk = []

    def count(self, selector: AssetCentricMigrationSelector) -> int | None:
        if isinstance(selector, MigrationCSVFileSelector):
            return len(selector.items)
        elif isinstance(selector, MigrateDataSetSelector):
            return self.hierarchy.count(selector.as_asset_centric_selector())
        else:
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for count")

    def _stream_given_dataset(
        self, selector: MigrateDataSetSelector, limit: int | None = None
    ) -> Iterator[Sequence[AssetCentricMapping[T_AssetCentricResource]]]:
        asset_centric_selector = selector.as_asset_centric_selector()
        for data_chunk in self.hierarchy.stream_data(asset_centric_selector, limit):
            mapping_list = AssetCentricMappingList[T_AssetCentricResource]([])
            for resource in data_chunk.items:
                # We got the resource from a dataset selector, so we know it is there
                data_set_id = cast(int, resource.data_set_id)
                space_source = self.client.migration.space_source.retrieve(data_set_id=data_set_id)
                instance_space = space_source.instance_space if space_source else None
                if instance_space is None:
                    instance_space = MISSING_INSTANCE_SPACE
                external_id = resource.external_id
                if external_id is None:
                    external_id = MISSING_EXTERNAL_ID.format(project=self.client.config.project, id=resource.id)
                mapping = MigrationMapping(
                    resource_type=self._kind_to_resource_type(selector.kind),
                    instance_id=NodeId(
                        space=instance_space,
                        external_id=external_id,
                    ),
                    id=resource.id,
                    data_set_id=resource.data_set_id,
                    ingestion_view=selector.ingestion_mapping,
                    preferred_consumer_view=selector.preferred_consumer_view,
                )
                mapping_list.append(AssetCentricMapping(mapping=mapping, resource=resource))
            yield mapping_list

    @staticmethod
    def _kind_to_resource_type(kind: AssetCentricKindExtended) -> AssetCentricType:
        mapping: dict[AssetCentricKindExtended, AssetCentricType] = {
            "Assets": "asset",
            "Events": "event",
            "TimeSeries": "timeseries",
            "FileMetadata": "file",
        }
        try:
            return mapping[kind]
        except KeyError as e:
            raise ToolkitNotImplementedError(f"Kind '{kind}' is not supported") from e

    def data_to_json_chunk(
        self,
        data_chunk: Sequence[AssetCentricMapping[T_AssetCentricResource]],
        selector: AssetCentricMigrationSelector | None = None,
    ) -> list[dict[str, JsonVal]]:
        return [item.dump() for item in data_chunk]

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> InstanceApply:
        raise NotImplementedError()

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[InstanceApply]],
        http_client: HTTPClient,
        selector: AssetCentricMigrationSelector | None = None,
    ) -> ItemsResultList:
        """Upload items by first linking them using files/set-pending-instance-ids and then uploading the instances."""
        if self.skip_linking:
            return super().upload_items(data_chunk, http_client, None)
        elif selector is None:
            raise ToolkitNotImplementedError(f"Selector must be provided for uploading {self.KIND} items.")
        elif selector.kind not in self.PENDING_INSTANCE_ID_ENDPOINT_BY_KIND:
            return super().upload_items(data_chunk, http_client, None)

        pending_instance_id_endpoint = self.PENDING_INSTANCE_ID_ENDPOINT_BY_KIND[selector.kind]
        results = ItemsResultList()
        to_upload = self.link_asset_centric(data_chunk, http_client, pending_instance_id_endpoint)
        if to_upload:
            results.extend(super().upload_items(to_upload, http_client, None))
        return results

    @classmethod
    def link_asset_centric(
        cls,
        data_chunk: Sequence[UploadItem[InstanceApply]],
        http_client: HTTPClient,
        pending_instance_id_endpoint: str,
    ) -> Sequence[UploadItem[InstanceApply]]:
        """Links asset-centric resources to their (uncreated) instances using the pending-instance-ids endpoint."""
        config = http_client.config
        successful_linked: set[str] = set()
        for batch in chunker_sequence(data_chunk, cls.CHUNK_SIZE):
            batch_results = http_client.request_items_retries(
                message=ItemsRequest(
                    endpoint_url=config.create_api_url(pending_instance_id_endpoint),
                    method="POST",
                    api_version="alpha",
                    items=[
                        UploadItem(source_id=item.source_id, item=cls.as_pending_instance_id(item.item))
                        for item in batch
                    ],
                )
            )
            for res in batch_results:
                if isinstance(res, ItemsSuccessResponse):
                    successful_linked.update(res.ids)
        to_upload = [item for item in data_chunk if item.source_id in successful_linked]
        return to_upload

    @staticmethod
    def as_pending_instance_id(item: InstanceApply) -> PendingInstanceId:
        """Convert an InstanceApply to a PendingInstanceId for linking."""
        source = next((source for source in item.sources if source.source == INSTANCE_SOURCE_VIEW_ID), None)
        if source is None:
            raise ValueError(f"Cannot extract ID from item of type {type(item).__name__!r}")
        if not isinstance(source.properties["id"], int):
            raise ValueError(f"Unexpected ID type: {type(source.properties['id']).__name__!r}")
        id_ = source.properties["id"]
        return PendingInstanceId(
            pending_instance_id=NodeId(item.space, item.external_id),
            id=id_,
        )


class AnnotationMigrationIO(
    UploadableStorageIO[AssetCentricMigrationSelector, AssetCentricMapping[Annotation], InstanceApply]
):
    """IO class for migrating Annotations.

    Args:
        client: The ToolkitClient to use for CDF interactions.
        instance_space: The instance space to use for the migrated annotations.
        default_asset_annotation_mapping: The default ingestion mapping to use for asset-linked annotations.
        default_file_annotation_mapping: The default ingestion mappingto use for file-linked annotations.

    """

    KIND = "AnnotationMigration"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    CHUNK_SIZE = 1000
    UPLOAD_ENDPOINT = InstanceIO.UPLOAD_ENDPOINT

    SUPPORTED_ANNOTATION_TYPES = frozenset({"diagrams.AssetLink", "diagrams.FileLink"})

    def __init__(
        self,
        client: ToolkitClient,
        instance_space: str | None = None,
        default_asset_annotation_mapping: str | None = None,
        default_file_annotation_mapping: str | None = None,
    ) -> None:
        super().__init__(client)
        self.annotation_io = AnnotationIO(client)
        self.instance_space = instance_space
        self.default_asset_annotation_mapping = default_asset_annotation_mapping or ASSET_ANNOTATIONS_ID
        self.default_file_annotation_mapping = default_file_annotation_mapping or FILE_ANNOTATIONS_ID

    def as_id(self, item: AssetCentricMapping[Annotation]) -> str:
        return f"Annotation_{item.mapping.id}"

    def count(self, selector: AssetCentricMigrationSelector) -> int | None:
        if isinstance(selector, MigrationCSVFileSelector):
            return len(selector.items)
        else:
            # There is no efficient way to count annotations in CDF.
            return None

    def stream_data(self, selector: AssetCentricMigrationSelector, limit: int | None = None) -> Iterable[Page]:
        if isinstance(selector, MigrateDataSetSelector):
            iterator = self._stream_from_dataset(selector, limit)
        elif isinstance(selector, MigrationCSVFileSelector):
            iterator = self._stream_from_csv(selector, limit)
        else:
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for stream_data")
        yield from (Page(worker_id="main", items=items) for items in iterator)

    def _stream_from_dataset(
        self, selector: MigrateDataSetSelector, limit: int | None = None
    ) -> Iterator[Sequence[AssetCentricMapping[Annotation]]]:
        if self.instance_space is None:
            raise ToolkitValueError("Instance space must be provided for dataset-based annotation migration.")
        asset_centric_selector = selector.as_asset_centric_selector()
        for data_chunk in self.annotation_io.stream_data(asset_centric_selector, limit):
            mapping_list = AssetCentricMappingList[Annotation]([])
            for resource in data_chunk.items:
                if resource.annotation_type not in self.SUPPORTED_ANNOTATION_TYPES:
                    # This should not happen, as the annotation_io should already filter these out.
                    # This is just in case.
                    continue
                mapping = AnnotationMapping(
                    instance_id=EdgeId(space=self.instance_space, external_id=f"annotation_{resource.id!r}"),
                    id=resource.id,
                    ingestion_view=self._get_mapping(selector.ingestion_mapping, resource),
                    preferred_consumer_view=selector.preferred_consumer_view,
                    # The PySDK is poorly typed.
                    annotation_type=resource.annotation_type,  # type: ignore[arg-type]
                )
                mapping_list.append(AssetCentricMapping(mapping=mapping, resource=resource))
            yield mapping_list

    def _stream_from_csv(
        self, selector: MigrationCSVFileSelector, limit: int | None = None
    ) -> Iterator[Sequence[AssetCentricMapping[Annotation]]]:
        items = selector.items
        if limit is not None:
            items = MigrationMappingList(items[:limit])
        chunk: list[AssetCentricMapping[Annotation]] = []
        for current_batch in chunker_sequence(items, self.CHUNK_SIZE):
            resources = self.client.annotations.retrieve_multiple(current_batch.get_ids())
            resources_by_id = {resource.id: resource for resource in resources}
            not_found = 0
            incorrect_type_count = 0
            for mapping in current_batch:
                resource = resources_by_id.get(mapping.id)
                if resource is None:
                    not_found += 1
                    continue
                if resource.annotation_type not in self.SUPPORTED_ANNOTATION_TYPES:
                    incorrect_type_count += 1
                    continue
                mapping.ingestion_view = self._get_mapping(mapping.ingestion_view, resource)
                chunk.append(AssetCentricMapping(mapping=mapping, resource=resource))
            if chunk:
                yield chunk
                chunk = []
            if not_found:
                MediumSeverityWarning(
                    f"Could not find {not_found} annotations referenced in the CSV file. They will be skipped during migration."
                ).print_warning(include_timestamp=True, console=self.client.console)
            if incorrect_type_count:
                MediumSeverityWarning(
                    f"Found {incorrect_type_count} annotations with unsupported types. Only 'diagrams.AssetLink' and "
                    "'diagrams.FileLink' are supported. These annotations will be skipped during migration."
                ).print_warning(include_timestamp=True, console=self.client.console)

    def _get_mapping(self, current_mapping: str | None, resource: Annotation) -> str:
        try:
            return (
                current_mapping
                or {
                    "diagrams.AssetLink": self.default_asset_annotation_mapping,
                    "diagrams.FileLink": self.default_file_annotation_mapping,
                }[resource.annotation_type]
            )
        except KeyError as e:
            raise ToolkitValueError(
                f"Could not determine default ingestion view for annotation type '{resource.annotation_type}'. "
                "Please specify the ingestion view explicitly in the CSV file."
            ) from e

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> InstanceApply:
        raise NotImplementedError("Deserializing Annotation Migrations from JSON is not supported.")

    def data_to_json_chunk(
        self,
        data_chunk: Sequence[AssetCentricMapping[Annotation]],
        selector: AssetCentricMigrationSelector | None = None,
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError("Serializing Annotation Migrations to JSON is not supported.")


class ThreeDMigrationIO(UploadableStorageIO[ThreeDSelector, ThreeDModelResponse, ThreeDMigrationRequest]):
    """IO class for downloading and migrating 3D models.

    Args:
        client: The ToolkitClient to use for CDF interactions.
        data_model_type: The type of 3D data model to download. Either "classic" or "DM".

    """

    KIND = "3DMigration"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    DOWNLOAD_LIMIT = 1000
    CHUNK_SIZE = 1
    UPLOAD_ENDPOINT = "/3d/migrate/models"
    REVISION_ENDPOINT = "/3d/migrate/revisions"

    def __init__(self, client: ToolkitClient, data_model_type: Literal["classic", "data modeling"] = "classic") -> None:
        super().__init__(client)
        self.data_model_type = data_model_type

    def as_id(self, item: ThreeDModelResponse) -> str:
        return item.name

    def _is_selected(self, item: ThreeDModelResponse, included_models: set[int] | None) -> bool:
        return self._is_correct_type(item) and (included_models is None or item.id in included_models)

    def _is_correct_type(self, item: ThreeDModelResponse) -> bool:
        if self.data_model_type == "classic":
            return item.space is None
        else:
            return item.space is not None

    def stream_data(self, selector: ThreeDSelector, limit: int | None = None) -> Iterable[Page[ThreeDModelResponse]]:
        published: bool | None = None
        if isinstance(selector, ThreeDModelFilteredSelector):
            published = selector.published
        included_models: set[int] | None = None
        if isinstance(selector, ThreeDModelIdSelector):
            included_models = set(selector.ids)
        cursor: str | None = None
        total = 0
        while True:
            request_limit = min(self.DOWNLOAD_LIMIT, limit - total) if limit is not None else self.DOWNLOAD_LIMIT
            response = self.client.tool.three_d.models_classic.paginate(
                published=published, include_revision_info=True, limit=request_limit, cursor=cursor
            )
            items = [item for item in response.items if self._is_selected(item, included_models)]
            total += len(items)
            if items:
                yield Page(worker_id="main", items=items, next_cursor=response.next_cursor)
            if response.next_cursor is None:
                break
            cursor = response.next_cursor

    def count(self, selector: ThreeDSelector) -> int | None:
        # There is no efficient way to count 3D models in CDF.
        return None

    def data_to_json_chunk(
        self, data_chunk: Sequence[ThreeDModelResponse], selector: ThreeDSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError("Deserializing Annotation Migrations from JSON is not supported.")

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> ThreeDMigrationRequest:
        raise NotImplementedError("Deserializing ThreeD Migrations from JSON is not supported.")

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[ThreeDMigrationRequest]],
        http_client: HTTPClient,
        selector: ThreeDSelector | None = None,
    ) -> ItemsResultList:
        """Migrate 3D models by uploading them to the migrate/models endpoint."""
        if len(data_chunk) > self.CHUNK_SIZE:
            raise RuntimeError(f"Uploading more than {self.CHUNK_SIZE} 3D models at a time is not supported.")

        results = ItemsResultList()
        responses = http_client.request_items_retries(
            message=ItemsRequest(
                endpoint_url=self.client.config.create_api_url(self.UPLOAD_ENDPOINT),
                method="POST",
                items=data_chunk,
            )
        )
        if (
            failed_response := next((res for res in responses if isinstance(res, ItemsFailedResponse)), None)
        ) and failed_response.status_code == 400:
            raise ToolkitAPIError("3D model migration failed. You need to enable the 3D migration alpha feature flag.")

        results.extend(responses)
        success_ids = {id for res in responses if isinstance(res, ItemsSuccessResponse) for id in res.ids}
        for data in data_chunk:
            if data.source_id not in success_ids:
                continue
            revision = http_client.request_single_retries(
                message=RequestMessage(
                    endpoint_url=self.client.config.create_api_url(self.REVISION_ENDPOINT),
                    method="POST",
                    body_content={"items": [data.item.revision.dump(camel_case=True)]},
                )
            )
            results.append(revision.as_item_response(data.source_id))
        return results


class ThreeDAssetMappingMigrationIO(
    UploadableStorageIO[ThreeDSelector, AssetMappingClassicResponse, AssetMappingDMRequest]
):
    KIND = "3DMigrationAssetMapping"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    DOWNLOAD_LIMIT = 1000
    CHUNK_SIZE = 100
    UPLOAD_ENDPOINT = "/3d/models/{modelId}/revisions/{revisionId}/mappings"

    def __init__(self, client: ToolkitClient, object_3D_space: str, cad_node_space: str) -> None:
        super().__init__(client)
        self.object_3D_space = object_3D_space
        self.cad_node_space = cad_node_space
        # We can only migrate asset mappings for 3D models that are already migrated to data modeling.
        self._3D_io = ThreeDMigrationIO(client, data_model_type="data modeling")

    def as_id(self, item: AssetMappingClassicResponse) -> str:
        return f"AssetMapping_{item.model_id!s}_{item.revision_id!s}_{item.asset_id!s}"

    def stream_data(
        self, selector: ThreeDSelector, limit: int | None = None
    ) -> Iterable[Page[AssetMappingClassicResponse]]:
        total = 0
        for three_d_page in self._3D_io.stream_data(selector, None):
            for model in three_d_page.items:
                if model.last_revision_info is None or model.last_revision_info.revision_id is None:
                    # No revisions, so no asset mappings to
                    continue
                cursor: str | None = None
                while True:
                    request_limit = (
                        min(self.DOWNLOAD_LIMIT, limit - total) if limit is not None else self.DOWNLOAD_LIMIT
                    )
                    if limit is not None and total >= limit:
                        return
                    response = self.client.tool.three_d.asset_mappings_classic.paginate(
                        model_id=model.id,
                        revision_id=model.last_revision_info.revision_id,
                        cursor=cursor,
                        limit=request_limit,
                    )
                    items = response.items
                    total += len(items)
                    if items:
                        yield Page(worker_id="main", items=items, next_cursor=response.next_cursor)
                    if response.next_cursor is None:
                        break
                    cursor = response.next_cursor

    def count(self, selector: ThreeDSelector) -> int | None:
        # There is no efficient way to count 3D asset mappings in CDF.
        return None

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[AssetMappingDMRequest]],
        http_client: HTTPClient,
        selector: T_Selector | None = None,
    ) -> ItemsResultList:
        """Migrate 3D asset mappings by uploading them to the migrate/asset-mappings endpoint."""
        if not data_chunk:
            return ItemsResultList()
        # Assume all items in the chunk belong to the same model and revision, they should
        # if the .stream_data method is used for downloading.
        first = data_chunk[0]
        model_id = first.item.model_id
        revision_id = first.item.revision_id
        endpoint = self.UPLOAD_ENDPOINT.format(modelId=model_id, revisionId=revision_id)
        return http_client.request_items_retries(
            ItemsRequest(
                endpoint_url=self.client.config.create_api_url(endpoint),
                method="POST",
                items=data_chunk,
                extra_body_fields={
                    "dmsContextualizationConfig": {
                        "object3DSpace": self.object_3D_space,
                        "cadNodeSpace": self.cad_node_space,
                    }
                },
            )
        )

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> AssetMappingDMRequest:
        raise NotImplementedError("Deserializing 3D Asset Mappings from JSON is not supported.")

    def data_to_json_chunk(
        self, data_chunk: Sequence[AssetMappingClassicResponse], selector: ThreeDSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError("Serializing 3D Asset Mappings to JSON is not supported.")
