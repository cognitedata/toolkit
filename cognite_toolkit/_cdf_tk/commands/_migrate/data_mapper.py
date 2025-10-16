from abc import ABC, abstractmethod
from typing import Generic

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
)
from cognite.client.data_classes.data_modeling import DirectRelationReference, View, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList
from cognite_toolkit._cdf_tk.client.data_classes.migration import ResourceViewMapping
from cognite_toolkit._cdf_tk.commands._migrate.adapter import (
    AssetCentricMappingList,
    MigrationSelector,
)
from cognite_toolkit._cdf_tk.commands._migrate.conversion import asset_centric_to_dm
from cognite_toolkit._cdf_tk.commands._migrate.issues import MigrationIssue
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio._base import T_Selector, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils import humanize_collection


class DataMapper(Generic[T_Selector, T_WritableCogniteResourceList, T_CogniteResourceList], ABC):
    def prepare(self, source_selector: T_Selector) -> None:
        """Prepare the data mapper with the given source selector.

        Args:
            source_selector: The selector for the source data.

        """
        # Override in subclass if needed.
        pass

    @abstractmethod
    def map_chunk(self, source: T_WritableCogniteResourceList) -> tuple[T_CogniteResourceList, list[MigrationIssue]]:
        """Map a chunk of source data to the target format.

        Args:
            source: The source data chunk to be mapped.

        Returns:
            A tuple containing the mapped data and a list of any issues encountered during mapping.

        """
        raise NotImplementedError("Subclasses must implement this method.")


class AssetCentricMapper(DataMapper[MigrationSelector, AssetCentricMappingList, InstanceApplyList]):
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client
        self._ingestion_view_by_id: dict[ViewId, View] = {}
        self._view_mapping_by_id: dict[str, ResourceViewMapping] = {}
        # This is used to keep track of already mapped assets, such that we can creat direct relations
        # to them from files, events, and time series.
        self._asset_mapping_by_id: dict[int, DirectRelationReference] = {}
        self._source_system_mapping_by_id: dict[str, DirectRelationReference] = {}

    def prepare(self, source_selector: MigrationSelector) -> None:
        ingestion_view_ids = source_selector.get_ingestion_views()
        ingestion_views = self.client.migration.resource_view_mapping.retrieve(ingestion_view_ids)
        self._view_mapping_by_id = {view.external_id: view for view in ingestion_views}
        missing_mappings = set(ingestion_view_ids) - set(self._view_mapping_by_id.keys())
        if missing_mappings:
            raise ToolkitValueError(
                f"The following ingestion views were not found: {humanize_collection(missing_mappings)}"
            )

        view_ids = list({view.view_id for view in ingestion_views})
        views = self.client.data_modeling.views.retrieve(view_ids)
        self._ingestion_view_by_id = {view.as_id(): view for view in views}
        missing_views = set(view_ids) - set(self._ingestion_view_by_id.keys())
        if missing_views:
            raise ToolkitValueError(
                f"The following ingestion views were not found in Data Modeling: {humanize_collection(missing_views)}"
            )
        # We just look-up all source system for now. This can be optimized to only
        # look-up the ones that are actually used in the ingestion views. However, SourceSystem is typically in
        # the order ~10 instances, so this is not a big deal for now. See task CDF-25974.
        source_systems = self.client.migration.created_source_system.list(limit=-1)
        self._source_system_mapping_by_id = {
            source_system.source: DirectRelationReference(space=source_system.space, external_id=source_system.source)
            for source_system in source_systems
        }

    def map_chunk(self, source: AssetCentricMappingList) -> tuple[InstanceApplyList, list[MigrationIssue]]:
        """Map a chunk of asset-centric data to InstanceApplyList format."""
        instances = InstanceApplyList([])
        issues: list[MigrationIssue] = []
        for item in source:
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
                asset_instance_id_by_id=self._asset_mapping_by_id,
                source_instance_id_by_external_id=self._source_system_mapping_by_id,
            )

            instances.append(instance)
            if conversion_issue.has_issues:
                issues.append(conversion_issue)

            if mapping.resource_type == "asset":
                self._asset_mapping_by_id[mapping.id] = DirectRelationReference(
                    space=mapping.instance_id.space, external_id=mapping.instance_id.external_id
                )
        return instances, issues
