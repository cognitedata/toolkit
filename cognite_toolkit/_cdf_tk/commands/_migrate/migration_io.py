from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    RequestMessage,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.http_client._item_classes import (
    ItemsFailedRequest,
    ItemsFailedResponse,
    ItemsRequest,
    ItemsResultList,
    ItemsSuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import InternalId, SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import EdgeId, NodeId, NodeOrEdgeRequest
from cognite_toolkit._cdf_tk.client.resource_classes.migration import SpaceSource
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamResponse
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicResponse,
    AssetMappingDMRequestId,
    ThreeDModelClassicResponse,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import ThreeDMigrationRequest
from cognite_toolkit._cdf_tk.constants import MISSING_EXTERNAL_ID
from cognite_toolkit._cdf_tk.dataio import (
    AnnotationIO,
    HierarchyIO,
    InstanceIO,
    T_Selector,
    UploadableDataIO,
)
from cognite_toolkit._cdf_tk.dataio._base import Bookmark, DataItem, Page
from cognite_toolkit._cdf_tk.dataio.logger import Severity
from cognite_toolkit._cdf_tk.dataio.progress import CursorBookmark, FileBookmark, NoBookmark
from cognite_toolkit._cdf_tk.dataio.selectors import (
    ThreeDModelFilteredSelector,
    ThreeDModelIdSelector,
    ThreeDSelector,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError, ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.streams import StreamIO
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence, humanize_collection
from cognite_toolkit._cdf_tk.utils.useful_types import (
    AssetCentricKindExtended,
    AssetCentricType,
    JsonVal,
)
from cognite_toolkit._cdf_tk.utils.useful_types2 import T_AssetCentricResource

from .data_classes import (
    AnnotationMapping,
    AssetCentricMapping,
    MigrationMapping,
    MigrationMappingList,
)
from .data_model import INSTANCE_SOURCE_VIEW_ID
from .default_mappings import ASSET_ANNOTATIONS_ID, FILE_ANNOTATIONS_ID
from .issues import MigrationEntryV2
from .selectors import AssetCentricMigrationSelector, MigrateDataSetSelector, MigrationCSVFileSelector


class AssetCentricMigrationIO(
    UploadableDataIO[AssetCentricMigrationSelector, AssetCentricMapping[T_AssetCentricResource], NodeOrEdgeRequest]
):
    KIND = "AssetCentricMigration"
    CHUNK_SIZE = 1000
    UPLOAD_ENDPOINT = InstanceIO.UPLOAD_ENDPOINT

    PENDING_INSTANCE_ID_ENDPOINT_BY_KIND: ClassVar[Mapping[AssetCentricKindExtended, str]] = {
        "TimeSeries": "/timeseries/set-pending-instance-ids",
        "FileMetadata": "/files/set-pending-instance-ids",
    }

    def __init__(self, client: ToolkitClient, skip_linking: bool = True, skip_existing: bool = False) -> None:
        super().__init__(client)
        self.hierarchy = HierarchyIO(client)
        self.skip_linking = skip_linking
        self.skip_existing = skip_existing

    def stream_data(
        self,
        selector: AssetCentricMigrationSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterator[Page]:
        file_location = bookmark if isinstance(bookmark, FileBookmark) else None

        if isinstance(selector, MigrationCSVFileSelector):
            instance_spaces = list({SpaceId(space=item.instance_id.space) for item in selector.items})
            iterator = self._stream_from_csv(selector, limit, file_location)
        elif isinstance(selector, MigrateDataSetSelector):
            space_source = self.client.migration.space_source.retrieve(
                data_set_external_id=selector.data_set_external_id
            )
            if space_source is None:
                raise ToolkitValueError(
                    f"Missing instance space that maps to {selector.data_set_external_id!r}. Have you run `cdf migrate data-sets`?"
                )
            instance_spaces = [SpaceId(space=space_source.space)]
            iterator = self._stream_given_dataset(selector, space_source, limit)
        else:
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for stream_data")
        existing = self.client.tool.spaces.retrieve(instance_spaces)
        if missing := set(instance_spaces).difference({item.as_id() for item in existing}):
            raise ToolkitValueError(
                f"The following instance spaces do not exist in CDF: {humanize_collection(missing)}. Please create these spaces before running the migration."
            )

        for items in iterator:
            page = Page(
                worker_id="main",
                items=[DataItem(tracking_id=str(item.mapping.as_asset_centric_id()), item=item) for item in items],
            )
            yield self.emit_registered_page(page)

    def _stream_from_csv(
        self,
        selector: MigrationCSVFileSelector,
        limit: int | None = None,
        file_location: FileBookmark | None = None,
    ) -> Iterator[Sequence[AssetCentricMapping[T_AssetCentricResource]]]:
        items = selector.items
        if file_location is not None:
            items = MigrationMappingList(items[file_location.lineno :])
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
        self, selector: MigrateDataSetSelector, space_source: SpaceSource, limit: int | None = None
    ) -> Iterator[Sequence[AssetCentricMapping[T_AssetCentricResource]]]:
        asset_centric_selector = selector.as_asset_centric_selector()
        instance_space = space_source.instance_space
        for data_chunk in self.hierarchy.stream_data(asset_centric_selector, limit):
            mapping_list: list[AssetCentricMapping[T_AssetCentricResource]] = []
            for data_item in data_chunk.items:
                resource = data_item.item
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
                    ingestion_mapping=selector.ingestion_mapping,
                    preferred_consumer_view=selector.preferred_consumer_view,
                )
                mapping_list.append(AssetCentricMapping(mapping=mapping, resource=resource))  # type: ignore[arg-type]
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
        data_chunk: Page[AssetCentricMapping[T_AssetCentricResource]],
        selector: AssetCentricMigrationSelector | None = None,
    ) -> Page[dict[str, JsonVal]]:
        return data_chunk.create_from(
            [DataItem(tracking_id=item.tracking_id, item=item.item.dump()) for item in data_chunk.items]
        )

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> NodeOrEdgeRequest:
        raise NotImplementedError()

    def upload_items(
        self,
        data_chunk: Page[NodeOrEdgeRequest],
        http_client: HTTPClient,
        selector: AssetCentricMigrationSelector | None = None,
    ) -> ItemsResultList:
        """Upload items by first linking them using files/set-pending-instance-ids and then uploading the instances."""
        if self.skip_existing:
            data_chunk = self._remove_existing(data_chunk)
            if not data_chunk:
                return ItemsResultList()

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

    def _remove_existing(self, data_chunk: Page[NodeOrEdgeRequest]) -> Page[NodeOrEdgeRequest]:
        """Remove items from the chunk that already exist in CDF to avoid upload failures."""
        data_by_instance_id = {item.item.as_id(): item for item in data_chunk.items}
        existing_ids = {item.as_id() for item in self.client.tool.instances.retrieve(list(data_by_instance_id.keys()))}
        to_create: list[DataItem[NodeOrEdgeRequest]] = []
        skipped_entries: list[MigrationEntryV2] = []
        for instance_id, data in data_by_instance_id.items():
            if instance_id in existing_ids:
                skipped_entries.append(
                    MigrationEntryV2(
                        id=data.tracking_id,
                        label="Skipped",
                        message="Instance already exists in CDF.",
                        severity=Severity.skipped,
                        source=self.KIND,
                        destination="instances",
                    )
                )
            else:
                to_create.append(data)
        if skipped_entries:
            self.logger.log(skipped_entries)

        return data_chunk.create_from(to_create)

    def link_asset_centric(
        self,
        data_chunk: Page[NodeOrEdgeRequest],
        http_client: HTTPClient,
        pending_instance_id_endpoint: str,
    ) -> Page[NodeOrEdgeRequest]:
        """Links asset-centric resources to their (uncreated) instances using the pending-instance-ids endpoint."""
        config = http_client.config
        successful_linked: set[str] = set()
        failure_entries: list[MigrationEntryV2] = []
        for batch in chunker_sequence(data_chunk.items, self.CHUNK_SIZE):
            batch_results = http_client.request_items_retries(
                message=ItemsRequest(
                    endpoint_url=config.create_api_url(pending_instance_id_endpoint),
                    method="POST",
                    api_version="alpha",
                    items=[
                        DataItem(tracking_id=item.tracking_id, item=self.as_pending_instance_id(item.item))
                        for item in batch
                    ],
                )
            )
            for res in batch_results:
                if isinstance(res, ItemsSuccessResponse):
                    successful_linked.update(res.ids)
                    continue
                for id_ in res.ids:
                    msg = (
                        res.error_message if isinstance(res, ItemsFailedResponse | ItemsFailedRequest) else "<unknown>"
                    )
                    failure_entries.append(
                        MigrationEntryV2(
                            id=id_,
                            label="Pending instance ID link failed",
                            message=msg,
                            severity=Severity.failure,
                            source="AssetCentric linking",
                            destination=self.KIND,
                        )
                    )
        if failure_entries:
            self.logger.log(failure_entries)
        to_upload = [item for item in data_chunk.items if item.tracking_id in successful_linked]
        return data_chunk.create_from(to_upload)

    @staticmethod
    def as_pending_instance_id(item: NodeOrEdgeRequest) -> PendingInstanceId:
        """Convert an InstanceApply to a PendingInstanceId for linking."""
        source = next((source for source in item.sources or [] if source.source == INSTANCE_SOURCE_VIEW_ID), None)
        if source is None:
            raise ValueError(f"Cannot extract ID from item of type {type(item).__name__!r}")
        if source.properties is None:
            raise ValueError("Source properties cannot be None when linking asset-centric resources.")
        if not isinstance(source.properties["id"], int):
            raise ValueError(f"Unexpected ID type: {type(source.properties['id']).__name__!r}")
        id_ = source.properties["id"]
        return PendingInstanceId(
            pending_instance_id=NodeId(space=item.space, external_id=item.external_id),
            id=id_,
        )


class RecordsMigrationIO(AssetCentricMigrationIO):
    """IO class for migrating asset-centric resources to records.

    Inherits all read-side logic (streaming, counting) from AssetCentricMigrationIO
    and overrides only the upload path to target a records stream.
    """

    KIND = "RecordsMigration"
    CHUNK_SIZE = 500
    UPLOAD_ENDPOINT = "/streams/{streamId}/records"
    UPSERT_ENDPOINT = "/streams/{streamId}/records/upsert"

    def __init__(self, client: ToolkitClient, stream: StreamResponse, skip_existing: bool = False) -> None:
        super().__init__(client)
        self.stream = stream
        self.skip_existing = skip_existing
        self._last_updated_time_windows: list[dict[str, int] | None] | None = None

    def _remove_existing(self, data_chunk: Page[RecordRequest]) -> Page[RecordRequest]:  # type: ignore[override]
        """Return a page with items whose (space, externalId) are not already in the stream.

        Logs skipped items on the migration logger.
        """
        if not data_chunk.items:
            return data_chunk

        if self._last_updated_time_windows is None:
            self._last_updated_time_windows = StreamIO.last_updated_time_windows(self.stream)
        last_updated_time_windows = self._last_updated_time_windows

        record_ids = [upload_item.item.as_id() for upload_item in data_chunk.items]
        existing_pairs: set[tuple[str, str]] = set()
        for last_updated_time in last_updated_time_windows:
            for record in self.client.records.retrieve(
                stream_external_id=self.stream.external_id,
                items=record_ids,
                last_updated_time=last_updated_time,
            ):
                existing_pairs.add((record.space, record.external_id))

        to_upload: list[DataItem[RecordRequest]] = []
        skipped_records: list[MigrationEntryV2] = []
        for upload_item in data_chunk.items:
            pair = (upload_item.item.space, upload_item.item.external_id)
            if pair in existing_pairs:
                skipped_records.append(
                    MigrationEntryV2(
                        id=upload_item.tracking_id,
                        label="Skipped",
                        message="Record already exists in the stream.",
                        severity=Severity.skipped,
                        source=self.KIND,
                        destination="records",
                    )
                )
            else:
                to_upload.append(upload_item)

        if skipped_records:
            self.logger.log(skipped_records)

        return data_chunk.create_from(to_upload)

    def upload_items(  # type: ignore[override]
        self,
        data_chunk: Page[RecordRequest],
        http_client: HTTPClient,
        selector: AssetCentricMigrationSelector | None = None,
    ) -> ItemsResultList:
        if self.skip_existing:
            data_chunk = self._remove_existing(data_chunk)
            if not data_chunk.items:
                return ItemsResultList()

        endpoint_template = self.UPSERT_ENDPOINT if self.stream.type == "Mutable" else self.UPLOAD_ENDPOINT
        endpoint = endpoint_template.format(streamId=self.stream.external_id)
        return http_client.request_items_retries(
            message=ItemsRequest(
                endpoint_url=self.client.config.create_api_url(endpoint),
                method="POST",
                items=data_chunk.items,
            )
        )


class AnnotationMigrationIO(
    UploadableDataIO[AssetCentricMigrationSelector, AssetCentricMapping[AnnotationResponse], NodeOrEdgeRequest]
):
    """IO class for migrating Annotations.

    Args:
        client: The ToolkitClient to use for CDF interactions.
        instance_space: The instance space to use for the migrated annotations.
        default_asset_annotation_mapping: The default ingestion mapping to use for asset-linked annotations.
        default_file_annotation_mapping: The default ingestion mappingto use for file-linked annotations.

    """

    KIND = "AnnotationMigration"
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

    def count(self, selector: AssetCentricMigrationSelector) -> int | None:
        if isinstance(selector, MigrationCSVFileSelector):
            return len(selector.items)
        else:
            # There is no efficient way to count annotations in CDF.
            return None

    def stream_data(
        self,
        selector: AssetCentricMigrationSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page]:
        file_location = bookmark if isinstance(bookmark, FileBookmark) else None
        if isinstance(selector, MigrateDataSetSelector):
            iterator = self._stream_from_dataset(selector, limit)
        elif isinstance(selector, MigrationCSVFileSelector):
            iterator = self._stream_from_csv(selector, limit, file_location)
        else:
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for stream_data")
        for items in iterator:
            page = Page(
                worker_id="main",
                items=[DataItem(tracking_id=str(item.mapping.as_asset_centric_id()), item=item) for item in items],
            )
            yield self.emit_registered_page(page)

    def _stream_from_dataset(
        self, selector: MigrateDataSetSelector, limit: int | None = None
    ) -> Iterator[Sequence[AssetCentricMapping[AnnotationResponse]]]:
        if self.instance_space is None:
            raise ToolkitValueError("Instance space must be provided for dataset-based annotation migration.")
        asset_centric_selector = selector.as_asset_centric_selector()
        for data_chunk in self.annotation_io.stream_data(asset_centric_selector, limit):
            mapping_list: list[AssetCentricMapping[AnnotationResponse]] = []
            for data_item in data_chunk.items:
                resource = data_item.item
                if resource.annotation_type not in self.SUPPORTED_ANNOTATION_TYPES:
                    # This should not happen, as the annotation_io should already filter these out.
                    # This is just in case.
                    continue
                mapping = AnnotationMapping(
                    instance_id=EdgeId(space=self.instance_space, external_id=f"annotation_{resource.id!r}"),
                    id=resource.id,
                    ingestion_mapping=self._get_mapping(selector.ingestion_mapping, resource),
                    preferred_consumer_view=selector.preferred_consumer_view,
                    annotation_type=resource.annotation_type,  # type: ignore[arg-type]
                )
                mapping_list.append(AssetCentricMapping(mapping=mapping, resource=resource))
            yield mapping_list

    def _stream_from_csv(
        self,
        selector: MigrationCSVFileSelector,
        limit: int | None = None,
        file_location: FileBookmark | None = None,
    ) -> Iterator[Sequence[AssetCentricMapping[AnnotationResponse]]]:
        items = selector.items
        if file_location is not None:
            items = MigrationMappingList(items[file_location.lineno :])
        if limit is not None:
            items = MigrationMappingList(items[:limit])
        chunk: list[AssetCentricMapping[AnnotationResponse]] = []
        for current_batch in chunker_sequence(items, self.CHUNK_SIZE):
            resources = self.client.tool.annotations.retrieve([InternalId(id=id_) for id_ in current_batch.get_ids()])
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
                mapping.ingestion_mapping = self._get_mapping(mapping.ingestion_mapping, resource)
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

    def _get_mapping(self, current_mapping: str | None, resource: AnnotationResponse) -> str:
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

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> NodeOrEdgeRequest:
        raise NotImplementedError("Deserializing Annotation Migrations from JSON is not supported.")

    def data_to_json_chunk(
        self,
        data_chunk: Page[AssetCentricMapping[AnnotationResponse]],
        selector: AssetCentricMigrationSelector | None = None,
    ) -> Page[dict[str, JsonVal]]:
        raise NotImplementedError("Serializing Annotation Migrations to JSON is not supported.")


class ThreeDMigrationIO(UploadableDataIO[ThreeDSelector, ThreeDModelClassicResponse, ThreeDMigrationRequest]):
    """IO class for downloading and migrating 3D models.

    Args:
        client: The ToolkitClient to use for CDF interactions.
        data_model_type: The type of 3D data model to download. Either "classic" or "DM".

    """

    KIND = "3DMigration"
    DOWNLOAD_LIMIT = 1000
    CHUNK_SIZE = 1
    UPLOAD_ENDPOINT = "/3d/migrate/models"
    REVISION_ENDPOINT = "/3d/migrate/revisions"

    def __init__(self, client: ToolkitClient, data_model_type: Literal["classic", "data modeling"] = "classic") -> None:
        super().__init__(client)
        self.data_model_type = data_model_type

    def _is_selected(self, item: ThreeDModelClassicResponse, included_models: set[int] | None) -> bool:
        return self._is_correct_type(item) and (included_models is None or item.id in included_models)

    def _is_correct_type(self, item: ThreeDModelClassicResponse) -> bool:
        if self.data_model_type == "classic":
            return item.space is None
        else:
            return item.space is not None

    def stream_data(
        self,
        selector: ThreeDSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page[ThreeDModelClassicResponse]]:
        published: bool | None = None
        if isinstance(selector, ThreeDModelFilteredSelector):
            published = selector.published
        included_models: set[int] | None = None
        if isinstance(selector, ThreeDModelIdSelector):
            included_models = set(selector.ids)
        cursor: str | None = bookmark.cursor if isinstance(bookmark, CursorBookmark) else None
        total = 0
        while True:
            request_limit = min(self.DOWNLOAD_LIMIT, limit - total) if limit is not None else self.DOWNLOAD_LIMIT
            response = self.client.tool.three_d.models_classic.paginate(
                published=published, include_revision_info=True, limit=request_limit, cursor=cursor
            )
            items = [item for item in response.items if self._is_selected(item, included_models)]
            total += len(items)
            if items:
                bm: Bookmark = CursorBookmark(cursor=response.next_cursor) if response.next_cursor else NoBookmark()
                yield self.emit_registered_page(
                    Page(
                        worker_id="main",
                        items=[DataItem(tracking_id=item.name, item=item) for item in items],
                        bookmark=bm,
                    )
                )
            if response.next_cursor is None:
                break
            cursor = response.next_cursor

    def count(self, selector: ThreeDSelector) -> int | None:
        # There is no efficient way to count 3D models in CDF.
        return None

    def data_to_json_chunk(
        self, data_chunk: Page[ThreeDModelClassicResponse], selector: ThreeDSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        raise NotImplementedError("Deserializing Annotation Migrations from JSON is not supported.")

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> ThreeDMigrationRequest:
        raise NotImplementedError("Deserializing ThreeD Migrations from JSON is not supported.")

    def upload_items(
        self,
        data_chunk: Page[ThreeDMigrationRequest],
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
                items=data_chunk.items,
            )
        )
        if (
            failed_response := next((res for res in responses if isinstance(res, ItemsFailedResponse)), None)
        ) and failed_response.status_code == 400:
            raise ToolkitAPIError("3D model migration failed. You need to enable the 3D migration alpha feature flag.")

        results.extend(responses)
        success_ids = {id for res in responses if isinstance(res, ItemsSuccessResponse) for id in res.ids}
        for data in data_chunk.items:
            if data.tracking_id not in success_ids:
                continue
            revision = http_client.request_single_retries(
                message=RequestMessage(
                    endpoint_url=self.client.config.create_api_url(self.REVISION_ENDPOINT),
                    method="POST",
                    body_content={"items": [data.item.revision.dump(camel_case=True)]},
                )
            )
            results.append(revision.as_item_response(data.tracking_id))
        return results


class ThreeDAssetMappingMigrationIO(
    UploadableDataIO[ThreeDSelector, AssetMappingClassicResponse, AssetMappingDMRequestId]
):
    KIND = "3DMigrationAssetMapping"
    DOWNLOAD_LIMIT = 1000
    CHUNK_SIZE = 100
    UPLOAD_ENDPOINT = "/3d/models/{modelId}/revisions/{revisionId}/mappings"

    def __init__(self, client: ToolkitClient, object_3D_space: str, cad_node_space: str) -> None:
        super().__init__(client)
        self.object_3D_space = object_3D_space
        self.cad_node_space = cad_node_space
        # We can only migrate asset mappings for 3D models that are already migrated to data modeling.
        self._3D_io = ThreeDMigrationIO(client, data_model_type="data modeling")

    def stream_data(
        self,
        selector: ThreeDSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page[AssetMappingClassicResponse]]:
        total = 0
        for three_d_page in self._3D_io.stream_data(selector, None):
            for data_item in three_d_page.items:
                seen_mappings: set[tuple[int, int, int, int]] = set()  # (model_id, revision_id, node_id, asset_id)
                model = data_item.item
                if model.last_revision_info is None or model.last_revision_info.revision_id is None:
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
                    unique_items: list[AssetMappingClassicResponse] = []
                    skipped_entries: list[MigrationEntryV2] = []
                    for item in response.items:
                        mapping_key = (
                            item.model_id,
                            item.revision_id,
                            item.node_id,
                            item.asset_id if item.asset_id is not None else -1,
                        )
                        if mapping_key in seen_mappings:
                            skipped_entries.append(
                                MigrationEntryV2(
                                    id=f"AssetMapping_{item.model_id!s}_{item.revision_id!s}_{item.node_id!s}_{item.asset_id!s}",
                                    label="Skipped",
                                    message="Duplicate asset mapping found.",
                                    severity=Severity.skipped,
                                    source=self.KIND,
                                    destination="3D asset mappings",
                                )
                            )
                        else:
                            seen_mappings.add(mapping_key)
                            unique_items.append(item)
                    if skipped_entries:
                        self.logger.log(skipped_entries)
                    total += len(unique_items)
                    if unique_items:
                        bm: Bookmark = (
                            CursorBookmark(cursor=response.next_cursor) if response.next_cursor else NoBookmark()
                        )
                        yield self.emit_registered_page(
                            Page(
                                worker_id="main",
                                items=[
                                    DataItem(
                                        tracking_id=f"AssetMapping_{item.model_id!s}_{item.revision_id!s}_{item.node_id!s}_{item.asset_id!s}",
                                        item=item,
                                    )
                                    for item in unique_items
                                ],
                                bookmark=bm,
                            )
                        )
                    if response.next_cursor is None:
                        break
                    cursor = response.next_cursor

    def count(self, selector: ThreeDSelector) -> int | None:
        # There is no efficient way to count 3D asset mappings in CDF.
        return None

    def upload_items(
        self,
        data_chunk: Page[AssetMappingDMRequestId],
        http_client: HTTPClient,
        selector: T_Selector | None = None,
    ) -> ItemsResultList:
        """Migrate 3D asset mappings by uploading them to the migrate/asset-mappings endpoint."""
        if not data_chunk:
            return ItemsResultList()
        # Assume all items in the chunk belong to the same model and revision, they should
        # if the .stream_data method is used for downloading.
        first = data_chunk.items[0]
        model_id = first.item.model_id
        revision_id = first.item.revision_id
        endpoint = self.UPLOAD_ENDPOINT.format(modelId=model_id, revisionId=revision_id)
        return http_client.request_items_retries(
            ItemsRequest(
                endpoint_url=self.client.config.create_api_url(endpoint),
                method="POST",
                items=data_chunk.items,
                extra_body_fields={
                    "dmsContextualizationConfig": {
                        "object3DSpace": self.object_3D_space,
                        "cadNodeSpace": self.cad_node_space,
                    }
                },
            )
        )

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> AssetMappingDMRequestId:
        raise NotImplementedError("Deserializing 3D Asset Mappings from JSON is not supported.")

    def data_to_json_chunk(
        self, data_chunk: Page[AssetMappingClassicResponse], selector: ThreeDSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        raise NotImplementedError("Serializing 3D Asset Mappings to JSON is not supported.")
