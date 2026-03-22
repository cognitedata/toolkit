import json
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from typing import Any, ClassVar, Generic, Literal, cast
from uuid import uuid4

from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteException
from pydantic import JsonValue, TypeAdapter, ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, EdgeTypeId, InstanceId
from cognite_toolkit._cdf_tk.client.resource_classes.canvas import (
    ContainerReferenceItem,
    FdmInstanceContainerReferenceItem,
    IndustrialCanvasRequest,
    IndustrialCanvasResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.chart import ChartRequest, ChartResponse
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import (
    ChartCoreTimeseries,
    ChartSource,
    ChartTimeseries,
)
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    DirectNodeRelation,
    EdgeRequest,
    EdgeResponse,
    InstanceRequest,
    InstanceResponse,
    InstanceSource,
    NodeId,
    NodeRequest,
    NodeResponse,
    ViewCorePropertyResponse,
    ViewId,
    ViewResponse,
    ViewResponseProperty,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import ChartsAdminAcl
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import (
    RESOURCE_VIEW_MAPPING_SPACE,
    ResourceViewMappingRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicResponse,
    AssetMappingDMRequestId,
    RevisionStatus,
    ThreeDModelClassicResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.commands._migrate.conversion import (
    ConnectionCreator,
    ConversionContext,
    CustomContainerPropertiesMapping,
    DirectRelationCache,
    EdgeOtherSide,
    asset_centric_to_dm,
    convert_container_properties,
    convert_edges,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import (
    ThreeDMigrationRequest,
    ThreeDRevisionMigrationRequest,
)
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import create_default_mappings
from cognite_toolkit._cdf_tk.commands._migrate.issues import (
    CanvasMigrationIssue,
    ChartMigrationIssue,
    ConversionIssue,
    InstanceConversionIssue,
    ThreeDModelMigrationIssue,
)
from cognite_toolkit._cdf_tk.constants import MISSING_INSTANCE_SPACE
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError, ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import T_DataRequest, T_DataResponse, T_Selector
from cognite_toolkit._cdf_tk.storageio.logger import DataLogger, NoOpLogger
from cognite_toolkit._cdf_tk.storageio.selectors import (
    CanvasSelector,
    ChartSelector,
    InstanceSelector,
    ThreeDSelector,
)
from cognite_toolkit._cdf_tk.utils import calculate_hash, humanize_collection
from cognite_toolkit._cdf_tk.utils.useful_types2 import T_AssetCentricResourceExtended

from .data_classes import AssetCentricMapping
from .selectors import AssetCentricMigrationSelector


class DataMapper(Generic[T_Selector, T_DataResponse, T_DataRequest], ABC):
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client
        self.logger: DataLogger = NoOpLogger()

    def prepare(self, source_selector: T_Selector) -> None:
        """Prepare the data mapper with the given source selector.

        Args:
            source_selector: The selector for the source data.

        """
        # Override in subclass if needed.
        pass

    @abstractmethod
    def map(self, source: Sequence[T_DataResponse]) -> Sequence[T_DataRequest | None]:
        """Map a chunk of source data to the target format.

        Args:
            source: The source data chunk to be mapped.

        Returns:
            A sequence of mapped target data.

        """
        raise NotImplementedError("Subclasses must implement this method.")


class AssetCentricMapper(
    DataMapper[AssetCentricMigrationSelector, AssetCentricMapping[T_AssetCentricResourceExtended], InstanceRequest]
):
    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._ingestion_view_by_id: dict[ViewId, ViewResponse] = {}
        self._view_mapping_by_id: dict[str, ResourceViewMappingRequest] = {}
        self._direct_relation_cache = DirectRelationCache(client)

    def prepare(self, source_selector: AssetCentricMigrationSelector) -> None:
        ingestion_view_ids = source_selector.get_ingestion_mappings()
        node_ids = NodeId.from_str_ids(ingestion_view_ids, space=RESOURCE_VIEW_MAPPING_SPACE)
        ingestion_views = self.client.migration.resource_view_mapping.retrieve(node_ids)
        defaults = {mapping.external_id: mapping for mapping in create_default_mappings()}
        # Custom mappings from CDF override the default mappings
        self._view_mapping_by_id = defaults | {view.external_id: view.as_request_resource() for view in ingestion_views}
        missing_mappings = set(ingestion_view_ids) - set(self._view_mapping_by_id.keys())
        if missing_mappings:
            raise ToolkitValueError(
                f"The following ingestion views were not found: {humanize_collection(missing_mappings)}"
            )

        view_ids = list(
            {
                ViewId(
                    space=mapping.view_id.space,
                    external_id=mapping.view_id.external_id,
                    version=mapping.view_id.version,
                )
                for mapping in self._view_mapping_by_id.values()
            }
        )
        views = self.client.tool.views.retrieve(view_ids)
        self._ingestion_view_by_id = {view.as_id(): view for view in views}
        missing_views = set(view_ids) - set(self._ingestion_view_by_id.keys())
        if missing_views:
            raise ToolkitValueError(
                f"The following ingestion views were not found in Data Modeling: {humanize_collection(missing_views)}"
            )

    def map(
        self, source: Sequence[AssetCentricMapping[T_AssetCentricResourceExtended]]
    ) -> Sequence[InstanceRequest | None]:
        """Map a chunk of asset-centric data to InstanceApplyList format."""
        # We update the direct relation cache in bulk for all resources in the chunk.
        self._direct_relation_cache.update(item.resource for item in source)
        output: list[InstanceRequest | None] = []
        issues: list[ConversionIssue] = []
        for item in source:
            instance, conversion_issue = self._map_single_item(item)
            identifier = str(item.mapping.as_asset_centric_id())

            if conversion_issue.missing_instance_space:
                self.logger.tracker.add_issue(identifier, "Missing instance space")
            if conversion_issue.failed_conversions:
                self.logger.tracker.add_issue(identifier, "Failed conversions")
            if conversion_issue.invalid_instance_property_types:
                self.logger.tracker.add_issue(identifier, "Invalid instance property types")
            if conversion_issue.missing_asset_centric_properties:
                self.logger.tracker.add_issue(identifier, "Missing asset-centric properties")
            if conversion_issue.missing_instance_properties:
                self.logger.tracker.add_issue(identifier, "Missing instance properties")
            if conversion_issue.ignored_asset_centric_properties:
                self.logger.tracker.add_issue(identifier, "Ignored asset-centric properties")

            if conversion_issue.has_issues:
                issues.append(conversion_issue)

            if instance is None:
                self.logger.tracker.finalize_item(identifier, "failure")
            output.append(instance)
        if issues:
            self.logger.log(issues)
        return output

    def _map_single_item(
        self, item: AssetCentricMapping[T_AssetCentricResourceExtended]
    ) -> tuple[NodeRequest | EdgeRequest | None, ConversionIssue]:
        mapping = item.mapping
        ingestion_view = mapping.get_ingestion_mapping()
        try:
            view_source = self._view_mapping_by_id[ingestion_view]
            view_ref = ViewId(
                space=view_source.view_id.space,
                external_id=view_source.view_id.external_id,
                version=view_source.view_id.version,
            )
            view_properties = self._ingestion_view_by_id[view_ref].properties
        except KeyError as e:
            raise RuntimeError(
                f"Failed to lookup mapping or view for ingestion view '{ingestion_view}'. Did you forget to call .prepare()?"
            ) from e
        instance, conversion_issue = asset_centric_to_dm(
            item.resource,
            instance_id=mapping.instance_id,
            view_source=view_source,
            view_properties=view_properties,
            direct_relation_cache=self._direct_relation_cache,
            preferred_consumer_view=mapping.preferred_consumer_view,
        )
        if mapping.instance_id.space == MISSING_INSTANCE_SPACE:
            conversion_issue.missing_instance_space = f"Missing instance space for dataset ID {mapping.data_set_id!r}"
        return instance, conversion_issue


class ChartMapper(DataMapper[ChartSelector, ChartResponse, ChartRequest]):
    def prepare(self, source_selector: ChartSelector) -> None:
        if missing_acl := self.client.tool.token.verify_acls(
            [ChartsAdminAcl(actions=["READ", "UPDATE"], scope=AllScope())]
        ):
            raise self.client.tool.token.create_error(missing_acl, action="migrate charts")

    def map(self, source: Sequence[ChartResponse]) -> Sequence[ChartRequest | None]:
        self._populate_cache(source)
        output: list[ChartRequest | None] = []
        issues: list[ChartMigrationIssue] = []
        for item in source:
            mapped_item, issue = self._map_single_item(item)
            identifier = item.external_id

            if issue.missing_timeseries_ids:
                self.logger.tracker.add_issue(identifier, "Missing timeseries IDs")
            if issue.missing_timeseries_external_ids:
                self.logger.tracker.add_issue(identifier, "Missing timeseries external IDs")
            if issue.missing_timeseries_identifier:
                self.logger.tracker.add_issue(identifier, "Missing timeseries identifier")

            if issue.has_issues:
                issues.append(issue)

            if mapped_item is None:
                self.logger.tracker.finalize_item(identifier, "failure")
            output.append(mapped_item)
        if issues:
            self.logger.log(issues)
        return output

    def _populate_cache(self, source: Sequence[ChartResponse]) -> None:
        """Populate the internal cache with timeseries from the source charts.

        Note that the consumption views are also cached as part of the timeseries lookup.
        """
        timeseries_ids: set[int] = set()
        timeseries_external_ids: set[str] = set()
        for chart in source:
            for item in chart.data.time_series_collection or []:
                if item.ts_id:
                    timeseries_ids.add(item.ts_id)
                if item.ts_external_id:
                    timeseries_external_ids.add(item.ts_external_id)
        if timeseries_ids:
            self.client.migration.lookup.time_series(list(timeseries_ids))
        if timeseries_external_ids:
            self.client.migration.lookup.time_series(external_id=list(timeseries_external_ids))

    def _map_single_item(self, item: ChartResponse) -> tuple[ChartRequest | None, ChartMigrationIssue]:
        issue = ChartMigrationIssue(chart_external_id=item.external_id, id=item.external_id)
        time_series_collection = item.data.time_series_collection or []
        timeseries_core_collection = self._create_timeseries_core_collection(time_series_collection, issue)
        if issue.has_issues:
            return None, issue

        updated_source_collection = self._update_source_collection(
            item.data.source_collection or [], time_series_collection, timeseries_core_collection
        )

        mapped_chart = item.as_request_resource()
        mapped_chart.data.core_timeseries_collection = timeseries_core_collection
        mapped_chart.data.time_series_collection = None
        mapped_chart.data.source_collection = updated_source_collection
        return mapped_chart, issue

    def _create_timeseries_core_collection(
        self, time_series_collection: list[ChartTimeseries], issue: ChartMigrationIssue
    ) -> list[ChartCoreTimeseries]:
        timeseries_core_collection: list[ChartCoreTimeseries] = []
        for ts_item in time_series_collection or []:
            node_id, consumer_view_id = self._get_node_id_consumer_view_id(ts_item)

            if node_id is None:
                if ts_item.ts_id is not None:
                    issue.missing_timeseries_ids.append(ts_item.ts_id)
                elif ts_item.ts_external_id is not None:
                    issue.missing_timeseries_external_ids.append(ts_item.ts_external_id)
                else:
                    issue.missing_timeseries_identifier.append(ts_item.id or "unknown")
                continue

            core_timeseries = self._create_new_timeseries_core(ts_item, node_id, consumer_view_id)
            timeseries_core_collection.append(core_timeseries)
        return timeseries_core_collection

    def _create_new_timeseries_core(
        self, ts_item: ChartTimeseries, node_id: NodeId, consumer_view_id: ViewId | None
    ) -> ChartCoreTimeseries:
        dumped = ts_item.model_dump(mode="json", by_alias=True, exclude_unset=True)
        dumped["nodeReference"] = dm.NodeId(space=node_id.space, external_id=node_id.external_id)
        dumped["viewReference"] = (
            dm.ViewId(
                space=consumer_view_id.space, external_id=consumer_view_id.external_id, version=consumer_view_id.version
            )
            if consumer_view_id
            else None
        )
        new_uuid = str(uuid4())
        dumped["id"] = new_uuid
        dumped["type"] = "coreTimeseries"
        # We ignore extra here to only include the fields that are shared between ChartTimeseries and ChartCoreTimeseries
        core_timeseries = ChartCoreTimeseries.model_validate(dumped, extra="ignore")
        return core_timeseries

    def _get_node_id_consumer_view_id(self, ts_item: ChartTimeseries) -> tuple[NodeId | None, ViewId | None]:
        """Look up the node ID and consumer view ID for a given timeseries item.

        Prioritizes lookup by internal ID, then by external ID.

        Args:
            ts_item: The ChartTimeseries item to look up.

        Returns:
            A tuple containing the consumer view ID and node ID, or None if not found.
        """
        node_id: NodeId | None = None
        consumer_view_id: ViewId | None = None
        for id_name, id_value in [("id", ts_item.ts_id), ("external_id", ts_item.ts_external_id)]:
            if id_value is None:
                continue
            arg = {id_name: id_value}
            node_id = self.client.migration.lookup.time_series(**arg)  # type: ignore[arg-type]
            consumer_view_id = self.client.migration.lookup.time_series.consumer_view(**arg)  # type: ignore[arg-type]
            if node_id is not None:
                break
        return node_id, consumer_view_id

    def _update_source_collection(
        self,
        source_collection: list[ChartSource],
        time_series_collection: list[ChartTimeseries],
        timeseries_core_collection: list[ChartCoreTimeseries],
    ) -> list[ChartSource]:
        remove_ids = {ts_item.id for ts_item in time_series_collection if ts_item.id is not None}
        updated_source_collection = [ts_item for ts_item in source_collection if ts_item.id not in remove_ids]
        for core_ts_item in timeseries_core_collection:
            # We cast there two as we set them in the _create_timeseries_core_collection method
            new_source_item = ChartSource(id=cast(str, core_ts_item.id), type=cast(str, core_ts_item.type))
            updated_source_collection.append(new_source_item)
        return updated_source_collection


class CanvasMapper(DataMapper[CanvasSelector, IndustrialCanvasResponse, IndustrialCanvasRequest]):
    # Note sequences are not supported in Canvas, so we do not include them here.
    asset_centric_resource_types = tuple(("asset", "event", "timeseries", "file"))
    DEFAULT_ASSET_VIEW = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
    DEFAULT_EVENT_VIEW = ViewId(space="cdf_cdm", external_id="CogniteActivity", version="v1")
    DEFAULT_FILE_VIEW = ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1")
    DEFAULT_TIMESERIES_VIEW = ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1")

    def __init__(self, client: ToolkitClient, dry_run: bool, skip_on_missing_ref: bool = False) -> None:
        super().__init__(client)
        self.dry_run = dry_run
        self.skip_on_missing_ref = skip_on_missing_ref

    def map(self, source: Sequence[IndustrialCanvasResponse]) -> Sequence[IndustrialCanvasRequest | None]:
        file_node_ids = self._populate_cache(source)
        cognite_file_by_id = {file.as_id(): file for file in self.client.tool.cognite_files.retrieve(file_node_ids)}
        output: list[IndustrialCanvasRequest | None] = []
        issues: list[CanvasMigrationIssue] = []
        for item in source:
            mapped_item, issue = self._map_single_item(item, cognite_file_by_id)
            identifier = item.name

            if issue.missing_reference_ids:
                self.logger.tracker.add_issue(identifier, "Missing reference IDs")
            if issue.files_missing_content:
                self.logger.tracker.add_issue(identifier, "File missing content")

            if issue.has_issues:
                issues.append(issue)

            if mapped_item is None:
                self.logger.tracker.finalize_item(identifier, "failure")

            output.append(mapped_item)
        if issues:
            self.logger.log(issues)
        return output

    @property
    def lookup_methods(self) -> dict[str, Callable]:
        return {
            "asset": self.client.migration.lookup.assets,
            "event": self.client.migration.lookup.events,
            "timeseries": self.client.migration.lookup.time_series,
            "file": self.client.migration.lookup.files,
        }

    def _populate_cache(self, source: Sequence[IndustrialCanvasResponse]) -> list[NodeId]:
        """Populate the internal cache with references from the source canvases.

        Note that the consumption views are also cached as part of the timeseries lookup.

        Returns:
            list[NodeId]: A list of node IDs for file references, which are used later for
                to check whether they have content.

        """
        ids_by_type: dict[str, set[int]] = defaultdict(set)
        for canvas in source:
            for ref in canvas.container_references or []:
                if ref.container_reference_type in self.asset_centric_resource_types:
                    ids_by_type[ref.container_reference_type].add(ref.resource_id)
        file_node_ids: list[NodeId] = []
        for resource_type, lookup_method in self.lookup_methods.items():
            ids = ids_by_type.get(resource_type)
            if ids:
                results = lookup_method(list(ids))
                if resource_type == "file":
                    file_node_ids.extend(results.values())
        return file_node_ids

    def _get_node_id(self, resource_id: int, resource_type: str) -> NodeId | None:
        """Look up the node ID for a given resource ID and type."""
        try:
            return self.lookup_methods[resource_type](resource_id)
        except KeyError:
            raise ToolkitValueError(f"Unsupported resource type '{resource_type}' for container reference migration.")

    def _get_consumer_view_id(self, resource_id: int, resource_type: str) -> ViewId:
        """Look up the consumer view ID for a given resource ID and type."""
        lookup_map = {
            "asset": (self.client.migration.lookup.assets.consumer_view, self.DEFAULT_ASSET_VIEW),
            "event": (self.client.migration.lookup.events.consumer_view, self.DEFAULT_EVENT_VIEW),
            "timeseries": (self.client.migration.lookup.time_series.consumer_view, self.DEFAULT_TIMESERIES_VIEW),
            "file": (self.client.migration.lookup.files.consumer_view, self.DEFAULT_FILE_VIEW),
        }
        if lookup_tuple := lookup_map.get(resource_type):
            method, fallback = lookup_tuple
            return method(resource_id) or fallback

        raise ToolkitValueError(f"Unsupported resource type '{resource_type}' for container reference migration.")

    def _map_single_item(
        self, canvas: IndustrialCanvasResponse, cognite_file_by_id: dict[NodeId, CogniteFileResponse]
    ) -> tuple[IndustrialCanvasRequest | None, CanvasMigrationIssue]:
        update = canvas.as_write()
        issue = CanvasMigrationIssue(canvas_external_id=canvas.external_id, canvas_name=canvas.name, id=canvas.name)

        remaining_container_references: list[IndustrialCanvasRequest] = []
        new_fdm_references: list[FdmInstanceContainerReferenceItem] = []
        uuid_generator: dict[str, str] = defaultdict(lambda: str(uuid4()))
        for ref in update.container_references or []:
            if ref.container_reference_type not in self.asset_centric_resource_types:
                remaining_container_references.append(ref)
                continue
            node_id = self._get_node_id(ref.resource_id, ref.container_reference_type)
            if node_id is None:
                issue.missing_reference_ids.append(ref.as_asset_centric_id())
                continue
            if ref.container_reference_type == "file" and node_id in cognite_file_by_id:
                if not cognite_file_by_id[node_id].is_uploaded:
                    issue.files_missing_content.append(node_id)

            consumer_view = self._get_consumer_view_id(ref.resource_id, ref.container_reference_type)
            fdm_ref = self.migrate_container_reference(ref, canvas.external_id, node_id, consumer_view, uuid_generator)
            new_fdm_references.append(fdm_ref)
        if issue.missing_reference_ids and self.skip_on_missing_ref:
            return None, issue

        update.container_references = remaining_container_references
        update.fdm_instance_container_references = (update.fdm_instance_container_references or []) + new_fdm_references
        if not self.dry_run:
            backup = canvas.as_request_resource().create_backup()
            try:
                self.client.canvas.create([backup])
            except CogniteException as e:
                raise ToolkitMigrationError(f"Failed to create backup for canvas '{canvas.name}': {e!s}. ") from e

        # There might annotations or other components that reference the old IDs, so we need to update those as well.
        update = update.replace_ids(uuid_generator)

        return update, issue

    @classmethod
    def migrate_container_reference(
        cls,
        reference: ContainerReferenceItem,
        canvas_external_id: str,
        instance_id: NodeId,
        consumer_view: ViewId,
        uuid_generator: dict[str, str],
    ) -> FdmInstanceContainerReferenceItem:
        """Migrate a single container reference by replacing the asset-centric ID with the data model instance ID."""
        new_id = uuid_generator[reference.id_]
        new_external_id = f"{canvas_external_id}_{new_id}"
        return FdmInstanceContainerReferenceItem(
            external_id=new_external_id,
            id_=new_id,
            container_reference_type="fdmInstance",
            instance_space=instance_id.space,
            instance_external_id=instance_id.external_id,
            view_space=consumer_view.space,
            view_external_id=consumer_view.external_id,
            view_version=consumer_view.version,
            label=reference.label,
            properties_=reference.properties_,
            x=reference.x,
            y=reference.y,
            width=reference.width,
            height=reference.height,
            max_width=reference.max_width,
            max_height=reference.max_height,
        )


class ThreeDMapper(DataMapper[ThreeDSelector, ThreeDModelClassicResponse, ThreeDMigrationRequest]):
    def map(self, source: Sequence[ThreeDModelClassicResponse]) -> Sequence[ThreeDMigrationRequest | None]:
        self._populate_cache(source)
        output: list[ThreeDMigrationRequest | None] = []
        issues: list[ThreeDModelMigrationIssue] = []
        for item in source:
            mapped_item, issue = self._map_single_item(item)
            identifier = item.name

            if issue.error_message:
                for error in issue.error_message:
                    self.logger.tracker.add_issue(identifier, error)

            if issue.has_issues:
                issues.append(issue)

            if mapped_item is None:
                self.logger.tracker.finalize_item(identifier, "failure")
            output.append(mapped_item)
        if issues:
            self.logger.log(issues)
        return output

    def _populate_cache(self, source: Sequence[ThreeDModelClassicResponse]) -> None:
        dataset_ids: set[int] = set()
        for model in source:
            if model.data_set_id is not None:
                dataset_ids.add(model.data_set_id)
        self.client.migration.space_source.retrieve(list(dataset_ids))

    def _map_single_item(
        self, item: ThreeDModelClassicResponse
    ) -> tuple[ThreeDMigrationRequest | None, ThreeDModelMigrationIssue]:
        issue = ThreeDModelMigrationIssue(model_name=item.name, model_id=item.id, id=item.name)
        instance_space: str | None = None
        last_revision_id: int | None = None
        model_type: Literal["CAD", "PointCloud", "Image360"] | None = None
        if item.data_set_id is None:
            issue.error_message.append("3D model is not associated with any dataset.")
        else:
            space_source = self.client.migration.space_source.retrieve(item.data_set_id)
            if space_source is not None:
                instance_space = space_source.instance_space
        if instance_space is None and item.data_set_id is not None:
            issue.error_message.append(f"Missing instance space for dataset ID {item.data_set_id!r}")
        if item.last_revision_info is None:
            issue.error_message.append("3D model has no revisions.")
        else:
            model_type = self._get_type(item.last_revision_info)
            last_revision_id = item.last_revision_info.revision_id
            if last_revision_id is None:
                issue.error_message.append("3D model's last revision has no revision ID.")

        if model_type is None:
            issue.error_message.append("3D model's last revision has no recognized type.")

        if instance_space is None or last_revision_id is None or model_type is None or issue.has_issues:
            return None, issue

        mapped_request = ThreeDMigrationRequest(
            model_id=item.id,
            type=model_type,
            space=instance_space,
            revision=ThreeDRevisionMigrationRequest(
                space=instance_space,
                type=model_type,
                revision_id=last_revision_id,
                model=InstanceId(
                    instance_id=NodeId(
                        space=instance_space,
                        external_id=f"cog_3d_model_{item.id!s}",
                    )
                ),
            ),
        )
        return mapped_request, issue

    @staticmethod
    def _get_type(revision: RevisionStatus) -> Literal["CAD", "PointCloud", "Image360"] | None:
        types = revision.types or []
        if any("gltf-directory" in t for t in types):
            return "CAD"
        elif any("ept-pointcloud" in t for t in types):
            return "PointCloud"
        else:
            return None


class ThreeDAssetMapper(DataMapper[ThreeDSelector, AssetMappingClassicResponse, AssetMappingDMRequestId]):
    def map(self, source: Sequence[AssetMappingClassicResponse]) -> Sequence[AssetMappingDMRequestId | None]:
        output: list[AssetMappingDMRequestId | None] = []
        issues: list[ThreeDModelMigrationIssue] = []
        self._populate_cache(source)
        for item in source:
            mapped_item, issue = self._map_single_item(item)
            identifier = f"AssetMapping_{item.model_id!s}_{item.revision_id!s}_{item.asset_id!s}"

            if issue.error_message:
                for error in issue.error_message:
                    self.logger.tracker.add_issue(identifier, error)

            if issue.has_issues:
                issues.append(issue)

            if mapped_item is None:
                self.logger.tracker.finalize_item(identifier, "failure")

            output.append(mapped_item)
        if issues:
            self.logger.log(issues)
        return output

    def _populate_cache(self, source: Sequence[AssetMappingClassicResponse]) -> None:
        asset_ids: set[int] = set()
        for mapping in source:
            if mapping.asset_id is not None:
                asset_ids.add(mapping.asset_id)
        self.client.migration.lookup.assets(list(asset_ids))

    def _map_single_item(
        self, item: AssetMappingClassicResponse
    ) -> tuple[AssetMappingDMRequestId | None, ThreeDModelMigrationIssue]:
        issue = ThreeDModelMigrationIssue(
            model_name=f"AssetMapping_{item.model_id}", model_id=item.model_id, id=f"AssetMapping_{item.model_id}"
        )
        asset_instance_id = item.asset_instance_id
        if item.asset_id and asset_instance_id is None:
            asset_node_id = self.client.migration.lookup.assets(item.asset_id)
            if asset_node_id is None:
                issue.error_message.append(f"Missing asset instance for asset ID {item.asset_id!r}")
                return None, issue
            asset_instance_id = NodeId(space=asset_node_id.space, external_id=asset_node_id.external_id)

        if asset_instance_id is None:
            issue.error_message.append("Neither assetInstanceId nor assetId provided for mapping.")
            return None, issue
        mapped_request = AssetMappingDMRequestId(
            model_id=item.model_id,
            revision_id=item.revision_id,
            node_id=item.node_id,
            asset_instance_id=asset_instance_id,
        )
        return mapped_request, issue


class FDMtoCDMMapper(DataMapper[InstanceSelector, InstanceResponse, InstanceRequest]):
    """This mapper maps instances to instances accounting for the difference between older data models
    (back when they were called Flexible Data Models) and newer data models (backed by Cognite Core Data Model).

    This means in particular the following:

    - Supports converting edges to direct/reverse direct relations.
    - Supports converting timeseries/files references to direct relation pointing to the CogniteTimeSeries/CogniteFile
        views.

    Args:
        client: The ToolkitClient to use for lookups and caching.
        mappings: A sequence of ViewToViewMappings defining how to map source views to target views and how to convert properties and edges.
        connection_creator: A ConnectionCreator instance to handle the creation of connections
            (edges and direct/reverse direct relations) based on the provided mappings.
        custom_properties_mappings: Optional sequence of ContainerPropertiesMappings defining special cases for mapping container
            properties that cannot be handled by the general ViewToViewMappings.
        custom_instance_mappings: Optional mapping of view IDs to custom DataMappers for handling special cases of
        instance mapping that cannot be handled by the general ViewToViewMappings and ConnectionCreator.

    """

    def __init__(
        self,
        client: ToolkitClient,
        mappings: Sequence[ViewToViewMapping],
        connection_creator: ConnectionCreator,
        custom_properties_mappings: Sequence[CustomContainerPropertiesMapping] | None = None,
        custom_instance_mappings: Mapping[ViewId, DataMapper[InstanceSelector, InstanceResponse, InstanceRequest]]
        | None = None,
    ) -> None:
        super().__init__(client)
        self._connection_creator = connection_creator
        self._mappings_by_source_view: dict[ViewId, ViewToViewMapping] = {
            mapping.source_view: mapping for mapping in mappings
        }
        self._custom_properties_mapping: dict[ViewId, CustomContainerPropertiesMapping] = {
            view_id: mapping for mapping in (custom_properties_mappings or []) for view_id in mapping.VIEW_IDS
        }
        self._custom_instance_mappings = custom_instance_mappings
        # These data structures are used to validate required direct relations, i.e.,
        # direct relation properties that requires that the target has properties in a
        # given container.
        self._constrained_properties_by_view_id: dict[ViewId, set[str]] = {}
        self._is_existing_by_node_id: dict[NodeId, bool] = {}

    def prepare(self, source_selector: InstanceSelector) -> None:
        view_ids = set(mapping.source_view for mapping in self._mappings_by_source_view.values()) | set(
            mapping.destination_view for mapping in self._mappings_by_source_view.values()
        )
        views = self.client.tool.views.retrieve(list(view_ids))
        self._connection_creator.update_view_cache(views)

    def map(self, source: Sequence[InstanceResponse]) -> Sequence[InstanceRequest | None]:
        if self._custom_instance_mappings and (
            intersecting_view_ids := (self._get_view_ids(source) & set(self._custom_instance_mappings))
        ):
            if len(intersecting_view_ids) == 1:
                intersection_view_id = next(iter(intersecting_view_ids))
                return self._custom_instance_mappings[intersection_view_id].map(source)
            else:
                # This is caused by the selector used to download the instance responses not matching the expectation in
                # the mapper.
                raise NotImplementedError(
                    "Bug in Toolkit: There should be at most one intersecting view when using custom mapping of instances."
                )
        self._connection_creator.update_cache(source)
        nodes, other_side_by_edge_type_and_direction_by_source = self._as_nodes_and_edges(source)
        mapped_instances: list[InstanceRequest | None] = []
        issue_by_node_id: dict[NodeId, InstanceConversionIssue] = {}
        target_view_ids: set[ViewId] = set()
        for node in nodes:
            node_id = node.as_id()
            if node.space not in self._connection_creator.space_mapping:
                issue_by_node_id[node_id] = InstanceConversionIssue(
                    id=str(node_id), errors=[f"No target space mapping for source space '{node.space}'"]
                )
                self.logger.tracker.finalize_item(str(node.as_id()), "failure")
                continue
            mapped_node, edges, issue = self._map_single_node(
                node, other_side_by_edge_type_and_direction_by_source[node.as_id()]
            )
            if issue.has_issues:
                issue_by_node_id[node_id] = issue
            target_view_ids.update(
                source_view_id
                for source_view_id in (node.properties or {}).keys()
                if isinstance(source_view_id, ViewId)
            )
            mapped_instances.append(mapped_node)
            mapped_instances.extend(edges)

        # Todo: Post validation - check that all direct relations with constraints exists

        # We need to
        self._connection_creator.update_view_cache(view_ids=target_view_ids)
        self._update_existing_node_cache(mapped_instances)

        if issue_by_node_id:
            self.logger.log(list(issue_by_node_id.values()))
        return mapped_instances

    def _get_view_ids(self, source: Sequence[InstanceResponse]) -> set[ViewId]:
        return {
            view_or_container_id
            for item in source
            for view_or_container_id in (item.properties or {}).keys()
            if isinstance(view_or_container_id, ViewId)
        }

    def _as_nodes_and_edges(
        self, source: Sequence[NodeResponse | EdgeResponse]
    ) -> tuple[
        list[NodeResponse],
        dict[NodeId, dict[EdgeTypeId, list[EdgeOtherSide]]],
    ]:
        nodes: list[NodeResponse] = []
        other_side_by_edge_type_and_direction_by_source: dict[NodeId, dict[EdgeTypeId, list[EdgeOtherSide]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        for item in source:
            if isinstance(item, NodeResponse):
                nodes.append(item)
            elif isinstance(item, EdgeResponse):
                edge_id = item.as_id()
                other_side_by_edge_type_and_direction_by_source[item.start_node][
                    EdgeTypeId(type=item.type, direction="outwards")
                ].append(EdgeOtherSide(edge_id, item.end_node))

                other_side_by_edge_type_and_direction_by_source[item.end_node][
                    EdgeTypeId(type=item.type, direction="inwards")
                ].append(EdgeOtherSide(edge_id, item.start_node))
        return nodes, other_side_by_edge_type_and_direction_by_source

    def _update_existing_node_cache(self, mapped_instances: Sequence[InstanceRequest | EdgeRequest | None]) -> None:
        to_check: list[NodeId] = []
        for mapped_node in mapped_instances:
            if mapped_node is None or isinstance(mapped_node, EdgeRequest):
                # We assume edges do not have direct relations.
                continue
            for source in mapped_node.sources or []:
                if source.properties is None or not isinstance(source.source, ViewId):
                    continue
                if source.source not in self._constrained_properties_by_view_id:
                    view = self._connection_creator.view_by_id[source.source]
                    self._constrained_properties_by_view_id[source.source] = {
                        prop_id
                        for prop_id, prop in view.properties.items()
                        if isinstance(prop, ViewCorePropertyResponse)
                        and isinstance(prop.type, DirectNodeRelation)
                        and prop.container is not None
                    }
                if direct_relation_properties := (
                    self._constrained_properties_by_view_id[source.source] & set(source.properties.keys())
                ):
                    for prop_id in direct_relation_properties:
                        value = source.properties[prop_id]
                        if isinstance(value, dict):
                            try:
                                to_check.append(NodeId.model_validate(value))
                            except ValidationError:
                                continue
                        elif isinstance(value, list):
                            try:
                                to_check.extend(TypeAdapter(list[NodeId]).validate_python(value))
                            except ValidationError:
                                continue

        missing_in_cache = set(to_check) - set(self._is_existing_by_node_id.keys())
        if missing_in_cache:
            existing_node_ids = {node.as_id() for node in self.client.tool.instances.retrieve(list(missing_in_cache))}
            for node_id in missing_in_cache:
                self._is_existing_by_node_id[node_id] = node_id in existing_node_ids

    def _map_single_node(
        self,
        node: NodeResponse,
        other_side_by_edge_type_and_direction: dict[EdgeTypeId, list[EdgeOtherSide]],
    ) -> tuple[NodeRequest, list[EdgeRequest], InstanceConversionIssue]:
        new_id = self._connection_creator.map_instance(node)
        sources, new_edges, issue = self._create_instance_data(new_id, node, other_side_by_edge_type_and_direction)

        return (
            NodeRequest(space=new_id.space, external_id=new_id.external_id, sources=sources or None),
            new_edges,
            issue,
        )

    def _create_instance_data(
        self,
        new_id: NodeId,
        node: NodeResponse,
        other_side_by_edge_type_and_direction: dict[EdgeTypeId, list[EdgeOtherSide]],
    ) -> tuple[list[InstanceSource], list[EdgeRequest], InstanceConversionIssue]:
        sources: list[InstanceSource] = []
        new_edges: list[EdgeRequest] = []
        issue = InstanceConversionIssue(id=str(new_id))
        for view_or_container_id, source_properties in (node.properties or {}).items():
            if not (context := self._create_context(view_or_container_id, issue, node.as_id(), new_id)):
                continue

            source_properties.update(
                {
                    "node.createdTime": node.created_time,
                    "node.lastUpdatedTime": node.last_updated_time,
                    "node.version": node.version,
                }
            )
            special_properties: dict[str, JsonValue | NodeId | list[NodeId]] = {}
            if context.mapping.source_view in self._custom_properties_mapping:
                special_results = self._custom_properties_mapping[context.mapping.source_view].convert(
                    source_properties, context
                )
                issue.errors.extend(special_results.errors)
                new_edges.extend(special_results.edges)
                special_properties = special_results.container_properties

            container_results = convert_container_properties(source_properties, context)
            edge_results = convert_edges(other_side_by_edge_type_and_direction, context)
            issue.errors.extend(container_results.errors)
            issue.errors.extend(edge_results.errors)
            new_edges.extend(container_results.edges)
            new_edges.extend(edge_results.edges)

            if overwritten := set(container_results.container_properties) & set(edge_results.container_properties):
                issue.errors.append(
                    f"Conflicting mapping. When converting container properties and edge for view "
                    f"'{context.mapping.source_view}' on node '{node.as_id()}' there were conflicting"
                    " destination properties. The properties from the edge will be prioritized for the "
                    f"conflicting properties: {humanize_collection(overwritten)}"
                )

            created_container_properties = {
                **container_results.container_properties,
                **edge_results.container_properties,
                # We overwrite the special properties.
                **special_properties,
            }

            if created_container_properties:
                sources.append(
                    InstanceSource(source=context.mapping.destination_view, properties=created_container_properties)
                )

        return sources, new_edges, issue

    def _create_context(
        self,
        view_or_container_id: ViewId | ContainerId,
        issue: InstanceConversionIssue,
        source_id: NodeId,
        new_id: NodeId,
    ) -> ConversionContext | None:
        if not isinstance(view_or_container_id, ViewId):
            issue.errors.append(
                f"Migration of ContainerReferenced properties is not supported. Found property with non-view ID '{view_or_container_id}' on node '{source_id}'."
            )
            return None
        mapping = self._mappings_by_source_view.get(view_or_container_id)
        if mapping is None:
            issue.errors.append(f"No mapping found for view '{view_or_container_id}'")
            return None
        destination_view = self._connection_creator.view_by_id.get(mapping.destination_view)
        if destination_view is None:
            issue.errors.append(
                f"Invalid mapping {mapping.external_id}': destination view {mapping.destination_view} not found."
            )
            return None
        if view_or_container_id not in self._connection_creator.view_by_id:
            issue.errors.append(
                f"View '{view_or_container_id}' not found in source data. This likely indicates that the view is missing from the cache. Did you forget to call .prepare()?"
            )
            return None

        return ConversionContext(
            mapping=mapping,
            destination_properties=destination_view.properties,
            connection_creator=self._connection_creator,
            source_view_id=view_or_container_id,
            new_id=new_id,
        )


class InFieldLegacyToCDMScheduleMapper(DataMapper[InstanceSelector, InstanceResponse, InstanceRequest]):
    """This is a custom case for mapping InField legacy to InField on CDM.

    In legacy, the schedules are modeled with edge connections as follows:

    Template -> TemplateItem -> Schedule.

    This leads to lots of duplicated schedules as it is often just two unique schedules for each template.

    In the new CDM based model, the schedule is connected to the template and template item with direct relations
    instead of edges.

    Template <- Schedule.template
    TemplateItems <- Schedule.templateItems (many).

    This mapper assumes that the sequence of InstanceResponses are all schedules connected to a single template,
    along with all the edges between the template and template items, as well as the template items and schedules.
    Note there should be no template or template items in the source, only schedules and edges.

    The mapper will then find all duplicated schedules based on their properties and use the edges to determine
    which direct relations to add to the single unique schedule that is created for each set of duplicated schedules.

    """

    SCHEDULE_VIEW = ViewId(space="cdf_apm", external_id="Schedule", version="v4")
    TEMPLATE_VIEW = ViewId(space="cdf_apm", external_id="Template", version="v8")
    TEMPLATE_EDGE_TYPE = NodeId(space="cdf_apm", external_id="referenceTemplateItems")
    TEMPLATE_ITEM_EDGE_TYPE = NodeId(space="cdf_apm", external_id="referenceSchedules")
    UNIQUE_SCHEDULE_PROPERTIES: ClassVar[tuple[str, ...]] = (
        "until",
        "byMonth",
        "byDay",
        "interval",
        "freq",
        "exceptionDates",
        "timezone",
        "startTime",
        "endTime",
    )

    def __init__(
        self, client: ToolkitClient, connection_creator: ConnectionCreator, mapping: ViewToViewMapping
    ) -> None:
        super().__init__(client)
        self._connection_creator = connection_creator
        self._mapping = mapping
        if self._mapping.source_view != self.SCHEDULE_VIEW:
            raise ValueError(
                f"Invalid mapping for InFieldLegacyToCDMScheduleMapper. Expected source view {self.SCHEDULE_VIEW}, got {self._mapping.source_view}"
            )

    def prepare(self, source_selector: InstanceSelector) -> None:
        # We need to have the view cache ready to be able to create the direct relations.
        retrieved = self.client.tool.views.retrieve([self._mapping.destination_view])
        self._connection_creator.update_view_cache(retrieved)

    def map(self, source: Sequence[InstanceResponse]) -> Sequence[InstanceRequest | None]:
        schedules, template_edges, template_id_edges, issues = self._as_schedules_and_edges(source)
        output: list[InstanceRequest | None] = []
        for duplicated_schedules in schedules.values():
            # Sort for deterministic output.
            duplicated_schedules.sort(key=lambda item: item.external_id)
            mapped_item, issue = self._create_single_schedule(duplicated_schedules, template_edges, template_id_edges)
            if issue.has_issues:
                issues.append(issue)
            if mapped_item is None:
                self.logger.tracker.finalize_item(str(duplicated_schedules[0].as_id()), "failure")
            output.append(mapped_item)
        if issues:
            self.logger.log(issues)
        return output

    def _as_schedules_and_edges(
        self, source: Sequence[InstanceResponse]
    ) -> tuple[
        dict[str, list[NodeResponse]],
        dict[NodeId, list[EdgeOtherSide]],
        dict[NodeId, list[EdgeOtherSide]],
        list[InstanceConversionIssue],
    ]:
        schedules: dict[str, list[NodeResponse]] = defaultdict(list)
        template_edges_by_item_id: dict[NodeId, list[EdgeOtherSide]] = defaultdict(list)
        template_item_edges_by_schedule_id: dict[NodeId, list[EdgeOtherSide]] = defaultdict(list)
        issues: list[InstanceConversionIssue] = []
        for item in source:
            if isinstance(item, NodeResponse):
                item_properties = item.properties or {}
                if schedule_properties := item_properties.get(self.SCHEDULE_VIEW):
                    schedule_hash = self._calculate_schedule_hash(schedule_properties)
                    schedules[schedule_hash].append(item)
                elif self.TEMPLATE_VIEW in item_properties:
                    # The template nodes are included to do pagination correctly (one page per template),
                    # but we do not need the templates, so we can safely ignore them.
                    continue
                else:
                    issues.append(
                        InstanceConversionIssue(
                            id=str(item.as_id()),
                            errors=[
                                f"Unexpected node with ID {item.as_id()} found in source that does not have the Schedule view."
                            ],
                        )
                    )

                    self.logger.tracker.finalize_item(str(item.as_id()), "failure")
            elif isinstance(item, EdgeResponse):
                if item.type == self.TEMPLATE_EDGE_TYPE:
                    template_edges_by_item_id[item.end_node].append(
                        EdgeOtherSide(edge_id=item.as_id(), other_side=item.start_node)
                    )
                elif item.type == self.TEMPLATE_ITEM_EDGE_TYPE:
                    template_item_edges_by_schedule_id[item.end_node].append(
                        EdgeOtherSide(edge_id=item.as_id(), other_side=item.start_node)
                    )
                else:
                    issues.append(
                        InstanceConversionIssue(
                            id=str(item.as_id()),
                            errors=[
                                f"Unexpected edge with ID {item.as_id()} and type {item.type} found in source. Expected edge types are {self.TEMPLATE_EDGE_TYPE} and {self.TEMPLATE_ITEM_EDGE_TYPE}."
                            ],
                        )
                    )
                    self.logger.tracker.finalize_item(str(item.as_id()), "failure")
        return schedules, template_edges_by_item_id, template_item_edges_by_schedule_id, issues

    def _calculate_schedule_hash(self, properties: dict[str, JsonValue | NodeId | list[NodeId]]) -> str:
        relevant_properties: dict[str, Any] = {}
        for key in self.UNIQUE_SCHEDULE_PROPERTIES:
            value = properties.get(key)
            if isinstance(value, list):
                relevant_properties[key] = sorted([str(item) for item in value])
            else:
                relevant_properties[key] = str(value)
        return calculate_hash(json.dumps(relevant_properties, sort_keys=True), shorten=True)

    def _create_single_schedule(
        self,
        duplicated_schedules: list[NodeResponse],
        template_edges_by_item_id: dict[NodeId, list[EdgeOtherSide]],
        template_item_edges_by_schedule_id: dict[NodeId, list[EdgeOtherSide]],
    ) -> tuple[InstanceRequest | None, InstanceConversionIssue]:
        if not duplicated_schedules:
            raise ValueError("At least one schedule is required to create a schedule mapping.")
        first = duplicated_schedules[0]
        issue = InstanceConversionIssue(id=str(first.as_id()))
        try:
            new_id = self._connection_creator.map_instance(first)
        except KeyError as e:
            issue.errors.append(f"Failed to map schedule with ID {first.as_id()}: {e!s}")
            return None, issue
        if self._mapping.destination_view not in self._connection_creator.view_by_id:
            issue.errors.append(
                f"Destination view '{self._mapping.destination_view}' not found in view cache. This likely indicates that the view is missing from the cache. Did you forget to call .prepare()?"
            )
            return None, issue
        destination_view = self._connection_creator.view_by_id[self._mapping.destination_view]
        source_properties = (first.properties or {}).get(self.SCHEDULE_VIEW, {})

        context = ConversionContext(
            mapping=self._mapping,
            destination_properties=destination_view.properties,
            connection_creator=self._connection_creator,
            source_view_id=self._mapping.source_view,
            new_id=new_id,
        )
        result = convert_container_properties(source_properties, context)
        created_properties = result.container_properties

        template_edges, template_item_edges = self._find_schedule_edges(
            duplicated_schedules, template_edges_by_item_id, template_item_edges_by_schedule_id
        )
        template_and_template_item_properties = self._create_template_relations(
            template_edges, template_item_edges, destination_view.properties, issue
        )
        created_properties.update(template_and_template_item_properties)

        return NodeRequest(
            space=new_id.space,
            external_id=new_id.external_id,
            sources=[InstanceSource(source=self._mapping.destination_view, properties=created_properties)],
        ), issue

    def _find_schedule_edges(
        self,
        duplicated_schedules: list[NodeResponse],
        template_edges_by_item_id: dict[NodeId, list[EdgeOtherSide]],
        template_item_edges_by_schedule_id: dict[NodeId, list[EdgeOtherSide]],
    ) -> tuple[list[EdgeOtherSide], list[EdgeOtherSide]]:
        template_item_edges: list[EdgeOtherSide] = []
        template_edges: list[EdgeOtherSide] = []
        for schedule in duplicated_schedules:
            for template_item_edge in template_item_edges_by_schedule_id.get(schedule.as_id(), []):
                template_item_edges.append(template_item_edge)
                for template_edge in template_edges_by_item_id.get(template_item_edge.other_side, []):
                    template_edges.append(template_edge)
        return template_edges, template_item_edges

    def _create_template_relations(
        self,
        template_edges: list[EdgeOtherSide],
        template_item_edges: list[EdgeOtherSide],
        destination_properties: dict[str, ViewResponseProperty],
        issue: InstanceConversionIssue,
    ) -> dict[str, JsonValue | NodeId | list[NodeId]]:
        created_properties: dict[str, JsonValue | NodeId | list[NodeId]] = {}
        if template_relation := self._create_direct_relation(
            template_edges,
            "template",
            destination_properties,
            EdgeTypeId(type=self.TEMPLATE_EDGE_TYPE, direction="inwards"),
            issue,
        ):
            created_properties["template"] = template_relation
        if template_item_relations := self._create_direct_relation(
            template_item_edges,
            "templateItems",
            destination_properties,
            EdgeTypeId(type=self.TEMPLATE_ITEM_EDGE_TYPE, direction="inwards"),
            issue,
        ):
            created_properties["templateItems"] = template_item_relations
        return created_properties

    def _create_direct_relation(
        self,
        edges: list[EdgeOtherSide],
        prop_id: str,
        destination_properties: dict[str, ViewResponseProperty],
        source_edge_type: EdgeTypeId,
        issue: InstanceConversionIssue,
    ) -> JsonValue | NodeId | list[NodeId] | None:
        if not edges:
            return None
        if isinstance(dm_prop := destination_properties.get(prop_id), ViewCorePropertyResponse) and isinstance(
            dm_prop.type, DirectNodeRelation
        ):
            try:
                node_id, creation_issues = self._connection_creator.create_direct_relation_from_edges(
                    edges=edges,
                    dm_prop=dm_prop.type,
                    source_edge_type=source_edge_type,
                )
            except ValueError as err:
                issue.errors.append(f"Failed to create direct relation {dm_prop.type} from edge {edges}: {err!s}")
                return None
            issue.errors.extend(creation_issues)
            return node_id
        else:
            issue.errors.append(
                f"Cannot create direct relation for property '{prop_id}' as it is not a DirectNodeRelation property in the destination view."
            )
        return None
