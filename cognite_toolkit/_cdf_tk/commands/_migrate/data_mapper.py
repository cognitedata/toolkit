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
    View,
    ViewId,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.migration import ResourceViewMapping
from cognite_toolkit._cdf_tk.commands._migrate.conversion import DirectRelationCache, asset_centric_to_dm
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import AssetCentricMapping
from cognite_toolkit._cdf_tk.commands._migrate.issues import ConversionIssue, MigrationIssue
from cognite_toolkit._cdf_tk.commands._migrate.selectors import AssetCentricMigrationSelector
from cognite_toolkit._cdf_tk.constants import MISSING_INSTANCE_SPACE
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio._base import T_Selector, T_WriteCogniteResource
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.useful_types import (
    T_AssetCentricResource,
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
    DataMapper[AssetCentricMigrationSelector, AssetCentricMapping[T_AssetCentricResource], InstanceApply]
):
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client
        self._ingestion_view_by_id: dict[ViewId, View] = {}
        self._view_mapping_by_id: dict[str, ResourceViewMapping] = {}
        self._direct_relation_cache = DirectRelationCache(client)

    def prepare(self, source_selector: AssetCentricMigrationSelector) -> None:
        ingestion_view_ids = source_selector.get_ingestion_mappings()
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

    def map(
        self, source: Sequence[AssetCentricMapping[T_AssetCentricResource]]
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
        self, item: AssetCentricMapping[T_AssetCentricResource]
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
