from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Sequence
from typing import Generic, Literal, cast
from uuid import uuid4

from cognite.client.data_classes.data_modeling import (
    EdgeApply,
    InstanceApply,
    NodeApply,
    NodeId,
    View,
    ViewId,
)
from cognite.client.exceptions import CogniteException

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import (
    ChartCoreTimeseries,
    ChartSource,
    ChartTimeseries,
)
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import InstanceIdentifier, NodeReference
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.canvas import (
    ContainerReferenceApply,
    FdmInstanceContainerReferenceApply,
    IndustrialCanvas,
    IndustrialCanvasApply,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.charts import Chart, ChartWrite
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import ResourceViewMappingApply
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicResponse,
    AssetMappingDMRequest,
    RevisionStatus,
    ThreeDModelResponse,
)
from cognite_toolkit._cdf_tk.commands._migrate.conversion import DirectRelationCache, asset_centric_to_dm
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import (
    Model,
    ThreeDMigrationRequest,
    ThreeDRevisionMigrationRequest,
)
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import create_default_mappings
from cognite_toolkit._cdf_tk.commands._migrate.issues import (
    CanvasMigrationIssue,
    ChartMigrationIssue,
    ConversionIssue,
    ThreeDModelMigrationIssue,
)
from cognite_toolkit._cdf_tk.constants import MISSING_INSTANCE_SPACE
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError, ToolkitValueError
from cognite_toolkit._cdf_tk.protocols import T_ResourceRequest, T_ResourceResponse
from cognite_toolkit._cdf_tk.storageio._base import T_Selector
from cognite_toolkit._cdf_tk.storageio.logger import DataLogger, NoOpLogger
from cognite_toolkit._cdf_tk.storageio.selectors import CanvasSelector, ChartSelector, ThreeDSelector
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.useful_types2 import T_AssetCentricResourceExtended

from .data_classes import AssetCentricMapping
from .selectors import AssetCentricMigrationSelector


class DataMapper(Generic[T_Selector, T_ResourceResponse, T_ResourceRequest], ABC):
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
    def map(self, source: Sequence[T_ResourceResponse]) -> Sequence[T_ResourceRequest | None]:
        """Map a chunk of source data to the target format.

        Args:
            source: The source data chunk to be mapped.

        Returns:
            A sequence of mapped target data.

        """
        raise NotImplementedError("Subclasses must implement this method.")


class AssetCentricMapper(
    DataMapper[AssetCentricMigrationSelector, AssetCentricMapping[T_AssetCentricResourceExtended], InstanceApply]
):
    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._ingestion_view_by_id: dict[ViewId, View] = {}
        self._view_mapping_by_id: dict[str, ResourceViewMappingApply] = {}
        self._direct_relation_cache = DirectRelationCache(client)

    def prepare(self, source_selector: AssetCentricMigrationSelector) -> None:
        ingestion_view_ids = source_selector.get_ingestion_mappings()
        ingestion_views = self.client.migration.resource_view_mapping.retrieve(ingestion_view_ids)
        defaults = {mapping.external_id: mapping for mapping in create_default_mappings()}
        # Custom mappings from CDF override the default mappings
        self._view_mapping_by_id = defaults | {view.external_id: view.as_write() for view in ingestion_views}
        missing_mappings = set(ingestion_view_ids) - set(self._view_mapping_by_id.keys())
        if missing_mappings:
            raise ToolkitValueError(
                f"The following ingestion views were not found: {humanize_collection(missing_mappings)}"
            )

        view_ids = list({mapping.view_id for mapping in self._view_mapping_by_id.values()})
        views = self.client.data_modeling.views.retrieve(view_ids)
        self._ingestion_view_by_id = {view.as_id(): view for view in views}
        missing_views = set(view_ids) - set(self._ingestion_view_by_id.keys())
        if missing_views:
            raise ToolkitValueError(
                f"The following ingestion views were not found in Data Modeling: {humanize_collection(missing_views)}"
            )

    def map(
        self, source: Sequence[AssetCentricMapping[T_AssetCentricResourceExtended]]
    ) -> Sequence[InstanceApply | None]:
        """Map a chunk of asset-centric data to InstanceApplyList format."""
        # We update the direct relation cache in bulk for all resources in the chunk.
        self._direct_relation_cache.update(item.resource for item in source)
        output: list[InstanceApply | None] = []
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
    ) -> tuple[NodeApply | EdgeApply | None, ConversionIssue]:
        mapping = item.mapping
        ingestion_view = mapping.get_ingestion_view()
        try:
            view_source = self._view_mapping_by_id[ingestion_view]
            view_properties = self._ingestion_view_by_id[view_source.view_id].properties
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


class ChartMapper(DataMapper[ChartSelector, Chart, ChartWrite]):
    def map(self, source: Sequence[Chart]) -> Sequence[ChartWrite | None]:
        self._populate_cache(source)
        output: list[ChartWrite | None] = []
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

    def _populate_cache(self, source: Sequence[Chart]) -> None:
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

    def _map_single_item(self, item: Chart) -> tuple[ChartWrite | None, ChartMigrationIssue]:
        issue = ChartMigrationIssue(chart_external_id=item.external_id, id=item.external_id)
        time_series_collection = item.data.time_series_collection or []
        timeseries_core_collection = self._create_timeseries_core_collection(time_series_collection, issue)
        if issue.has_issues:
            return None, issue

        updated_source_collection = self._update_source_collection(
            item.data.source_collection or [], time_series_collection, timeseries_core_collection
        )

        mapped_chart = item.as_write()
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
        dumped["nodeReference"] = node_id
        dumped["viewReference"] = consumer_view_id
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


class CanvasMapper(DataMapper[CanvasSelector, IndustrialCanvas, IndustrialCanvasApply]):
    # Note sequences are not supported in Canvas, so we do not include them here.
    asset_centric_resource_types = tuple(("asset", "event", "timeseries", "file"))
    DEFAULT_ASSET_VIEW = ViewId("cdf_cdm", "CogniteAsset", "v1")
    DEFAULT_EVENT_VIEW = ViewId("cdf_cdm", "CogniteActivity", "v1")
    DEFAULT_FILE_VIEW = ViewId("cdf_cdm", "CogniteFile", "v1")
    DEFAULT_TIMESERIES_VIEW = ViewId("cdf_cdm", "CogniteTimeSeries", "v1")

    def __init__(self, client: ToolkitClient, dry_run: bool, skip_on_missing_ref: bool = False) -> None:
        super().__init__(client)
        self.dry_run = dry_run
        self.skip_on_missing_ref = skip_on_missing_ref

    def map(self, source: Sequence[IndustrialCanvas]) -> Sequence[IndustrialCanvasApply | None]:
        self._populate_cache(source)
        output: list[IndustrialCanvasApply | None] = []
        issues: list[CanvasMigrationIssue] = []
        for item in source:
            mapped_item, issue = self._map_single_item(item)
            identifier = item.as_id()

            if issue.missing_reference_ids:
                self.logger.tracker.add_issue(identifier, "Missing reference IDs")

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

    def _populate_cache(self, source: Sequence[IndustrialCanvas]) -> None:
        """Populate the internal cache with references from the source canvases.

        Note that the consumption views are also cached as part of the timeseries lookup.
        """
        ids_by_type: dict[str, set[int]] = defaultdict(set)
        for canvas in source:
            for ref in canvas.container_references or []:
                if ref.container_reference_type in self.asset_centric_resource_types:
                    ids_by_type[ref.container_reference_type].add(ref.resource_id)

        for resource_type, lookup_method in self.lookup_methods.items():
            ids = ids_by_type.get(resource_type)
            if ids:
                lookup_method(list(ids))

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

    def _map_single_item(self, canvas: IndustrialCanvas) -> tuple[IndustrialCanvasApply | None, CanvasMigrationIssue]:
        update = canvas.as_write()
        issue = CanvasMigrationIssue(
            canvas_external_id=canvas.canvas.external_id, canvas_name=canvas.canvas.name, id=canvas.canvas.name
        )

        remaining_container_references: list[ContainerReferenceApply] = []
        new_fdm_references: list[FdmInstanceContainerReferenceApply] = []
        uuid_generator: dict[str, str] = defaultdict(lambda: str(uuid4()))
        for ref in update.container_references or []:
            if ref.container_reference_type not in self.asset_centric_resource_types:
                remaining_container_references.append(ref)
                continue
            node_id = self._get_node_id(ref.resource_id, ref.container_reference_type)
            if node_id is None:
                issue.missing_reference_ids.append(ref.as_asset_centric_id())
            else:
                consumer_view = self._get_consumer_view_id(ref.resource_id, ref.container_reference_type)
                fdm_ref = self.migrate_container_reference(
                    ref, canvas.canvas.external_id, node_id, consumer_view, uuid_generator
                )
                new_fdm_references.append(fdm_ref)
        if issue.missing_reference_ids and self.skip_on_missing_ref:
            return None, issue

        update.container_references = remaining_container_references
        update.fdm_instance_container_references.extend(new_fdm_references)
        if not self.dry_run:
            backup = canvas.as_write().create_backup()
            try:
                self.client.canvas.industrial.create(backup)
            except CogniteException as e:
                raise ToolkitMigrationError(
                    f"Failed to create backup for canvas '{canvas.canvas.name}': {e!s}. "
                ) from e

        # There might annotations or other components that reference the old IDs, so we need to update those as well.
        update = update.replace_ids(uuid_generator)

        return update, issue

    @classmethod
    def migrate_container_reference(
        cls,
        reference: ContainerReferenceApply,
        canvas_external_id: str,
        instance_id: NodeId,
        consumer_view: ViewId,
        uuid_generator: dict[str, str],
    ) -> FdmInstanceContainerReferenceApply:
        """Migrate a single container reference by replacing the asset-centric ID with the data model instance ID."""
        new_id = uuid_generator[reference.id_]
        new_external_id = f"{canvas_external_id}_{new_id}"
        return FdmInstanceContainerReferenceApply(
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


class ThreeDMapper(DataMapper[ThreeDSelector, ThreeDModelResponse, ThreeDMigrationRequest]):
    def map(self, source: Sequence[ThreeDModelResponse]) -> Sequence[ThreeDMigrationRequest | None]:
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

    def _populate_cache(self, source: Sequence[ThreeDModelResponse]) -> None:
        dataset_ids: set[int] = set()
        for model in source:
            if model.data_set_id is not None:
                dataset_ids.add(model.data_set_id)
        self.client.migration.space_source.retrieve(list(dataset_ids))

    def _map_single_item(
        self, item: ThreeDModelResponse
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
                model=Model(
                    instance_id=InstanceIdentifier(
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


class ThreeDAssetMapper(DataMapper[ThreeDSelector, AssetMappingClassicResponse, AssetMappingDMRequest]):
    def map(self, source: Sequence[AssetMappingClassicResponse]) -> Sequence[AssetMappingDMRequest | None]:
        output: list[AssetMappingDMRequest | None] = []
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
    ) -> tuple[AssetMappingDMRequest | None, ThreeDModelMigrationIssue]:
        issue = ThreeDModelMigrationIssue(
            model_name=f"AssetMapping_{item.model_id}", model_id=item.model_id, id=f"AssetMapping_{item.model_id}"
        )
        asset_instance_id = item.asset_instance_id
        if item.asset_id and asset_instance_id is None:
            asset_node_id = self.client.migration.lookup.assets(item.asset_id)
            if asset_node_id is None:
                issue.error_message.append(f"Missing asset instance for asset ID {item.asset_id!r}")
                return None, issue
            asset_instance_id = NodeReference(space=asset_node_id.space, external_id=asset_node_id.external_id)

        if asset_instance_id is None:
            issue.error_message.append("Neither assetInstanceId nor assetId provided for mapping.")
            return None, issue
        mapped_request = AssetMappingDMRequest(
            model_id=item.model_id,
            revision_id=item.revision_id,
            node_id=item.node_id,
            asset_instance_id=asset_instance_id,
        )
        return mapped_request, issue
