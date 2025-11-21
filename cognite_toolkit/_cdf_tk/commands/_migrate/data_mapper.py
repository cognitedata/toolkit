from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Generic

from cognite.client.data_classes._base import (
    T_CogniteResource,
)
from cognite.client.data_classes.data_modeling import (
    EdgeApply,
    InstanceApply,
    NodeApply,
    NodeId,
    View,
    ViewId,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.charts import Chart, ChartWrite
from cognite_toolkit._cdf_tk.client.data_classes.charts_data import ChartCoreTimeseries
from cognite_toolkit._cdf_tk.client.data_classes.migration import ResourceViewMappingApply
from cognite_toolkit._cdf_tk.commands._migrate.conversion import DirectRelationCache, asset_centric_to_dm
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import AssetCentricMapping
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import create_default_mappings
from cognite_toolkit._cdf_tk.commands._migrate.issues import ChartMigrationIssue, ConversionIssue, MigrationIssue
from cognite_toolkit._cdf_tk.commands._migrate.selectors import AssetCentricMigrationSelector
from cognite_toolkit._cdf_tk.constants import MISSING_INSTANCE_SPACE
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio._base import T_Selector, T_WriteCogniteResource
from cognite_toolkit._cdf_tk.storageio.selectors import ChartSelector
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.useful_types import (
    T_AssetCentricResourceExtended,
)


class DataMapper(Generic[T_Selector, T_CogniteResource, T_WriteCogniteResource], ABC):
    def prepare(self, source_selector: T_Selector) -> None:
        """Prepare the data mapper with the given source selector.

        Args:
            source_selector: The selector for the source data.

        """
        # Override in subclass if needed.
        pass

    @abstractmethod
    def map(
        self, source: Sequence[T_CogniteResource]
    ) -> Sequence[tuple[T_WriteCogniteResource | None, MigrationIssue]]:
        """Map a chunk of source data to the target format.

        Args:
            source: The source data chunk to be mapped.

        Returns:
            A tuple containing the mapped data and a list of any issues encountered during mapping.

        """
        raise NotImplementedError("Subclasses must implement this method.")


class AssetCentricMapper(
    DataMapper[AssetCentricMigrationSelector, AssetCentricMapping[T_AssetCentricResourceExtended], InstanceApply]
):
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client
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
    ) -> Sequence[tuple[InstanceApply | None, ConversionIssue]]:
        """Map a chunk of asset-centric data to InstanceApplyList format."""
        # We update the direct relation cache in bulk for all resources in the chunk.
        self._direct_relation_cache.update(item.resource for item in source)
        output: list[tuple[InstanceApply | None, ConversionIssue]] = []
        for item in source:
            instance, conversion_issue = self._map_single_item(item)
            output.append((instance, conversion_issue))
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
        )
        if mapping.instance_id.space == MISSING_INSTANCE_SPACE:
            conversion_issue.missing_instance_space = f"Missing instance space for dataset ID {mapping.data_set_id!r}"
        return instance, conversion_issue


class ChartMapper(DataMapper[ChartSelector, Chart, ChartWrite]):
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    def map(self, source: Sequence[Chart]) -> Sequence[tuple[ChartWrite | None, MigrationIssue]]:
        self._populate_cache(source)
        output: list[tuple[ChartWrite | None, MigrationIssue]] = []
        for item in source:
            mapped_item, issue = self._map_single_item(item)
            if issue.has_issues:
                # Skip migrating charts if any timeseries are missing
                output.append((None, issue))
            else:
                output.append((mapped_item, issue))
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

    def _map_single_item(self, item: Chart) -> tuple[ChartWrite, ChartMigrationIssue]:
        issue = ChartMigrationIssue(chart_external_id=item.external_id)
        timeseries_core_collection: list[ChartCoreTimeseries] = []
        for ts_item in item.data.time_series_collection or []:
            node_id: NodeId | None
            consumer_view_id: ViewId | None
            for id_name, id_value in [("id", ts_item.ts_id), ("external_id", ts_item.ts_external_id)]:
                arg = {id_name: id_value}
                node_id = self.client.migration.lookup.time_series(**arg)  # type: ignore[arg-type]
                consumer_view_id = self.client.migration.lookup.time_series.consumer_view(**arg)  # type: ignore[arg-type]
                if node_id is not None:
                    break

            if node_id is None:
                if ts_item.ts_id is not None:
                    issue.missing_timeseries_ids.append(ts_item.ts_id)
                elif ts_item.ts_external_id is not None:
                    issue.missing_timeseries_external_ids.append(ts_item.ts_external_id)
                else:
                    issue.missing_timeseries_identifier.append(ts_item.id or "unknown")
                continue

            dumped = ts_item.dump(camel_case=True)
            for asset_centric_key in ["tsId", "tsExternalId", "originalUnit"]:
                dumped.pop(asset_centric_key, None)

            dumped["nodeReference"] = node_id
            dumped["viewReference"] = consumer_view_id
            core_timeseries = ChartCoreTimeseries._load(dumped)
            timeseries_core_collection.append(core_timeseries)
        mapped_chart = item.as_write()
        mapped_chart.data.core_timeseries_collection = timeseries_core_collection
        mapped_chart.data.time_series_collection = None
        return mapped_chart, issue
