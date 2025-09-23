from abc import ABC, abstractmethod
from typing import Generic

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
)
from cognite.client.data_classes.data_modeling import View, ViewId

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
            )

            instances.append(instance)
            if conversion_issue.has_issues:
                issues.append(conversion_issue)
        return instances, issues
