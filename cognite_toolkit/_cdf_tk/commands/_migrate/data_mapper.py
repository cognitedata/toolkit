from abc import ABC, abstractmethod
from typing import Generic

from cognite.client.data_classes._base import (
    T_CogniteResource,
)
from cognite.client.data_classes.data_modeling import DirectRelationReference, InstanceApply, View, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.migration import ResourceViewMapping
from cognite_toolkit._cdf_tk.commands._migrate.adapter import (
    AssetCentricMapping,
    MigrationSelector,
)
from cognite_toolkit._cdf_tk.commands._migrate.conversion import asset_centric_to_dm
from cognite_toolkit._cdf_tk.commands._migrate.issues import ConversionIssue, MigrationIssue
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio._base import T_Selector, T_WriteCogniteResource
from cognite_toolkit._cdf_tk.utils import humanize_collection


class DataMapper(Generic[T_Selector, T_CogniteResource, T_WriteCogniteResource], ABC):
    def prepare(self, source_selector: T_Selector) -> None:
        """Prepare the data mapper with the given source selector.

        Args:
            source_selector: The selector for the source data.

        """
        # Override in subclass if needed.
        pass

    @abstractmethod
    def map(self, source: T_CogniteResource) -> tuple[T_WriteCogniteResource, MigrationIssue]:
        """Map a chunk of source data to the target format.

        Args:
            source: The source data chunk to be mapped.

        Returns:
            A tuple containing the mapped data and a list of any issues encountered during mapping.

        """
        raise NotImplementedError("Subclasses must implement this method.")


class AssetCentricMapper(DataMapper[MigrationSelector, AssetCentricMapping, InstanceApply]):
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client
        self._ingestion_view_by_id: dict[ViewId, View] = {}
        self._view_mapping_by_id: dict[str, ResourceViewMapping] = {}
        # This is used to keep track of already mapped assets, such that we can creat direct relations
        # to them from files, events, and time series.
        self._asset_mapping_by_id: dict[int, DirectRelationReference] = {}
        self._source_system_mapping_by_id: dict[str, DirectRelationReference] = {}

    def prepare(self, source_selector: MigrationSelector) -> None:
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
        # We just look-up all source system for now. This can be optimized to only
        # look-up the ones that are actually used in the ingestion views. However, SourceSystem is typically in
        # the order ~10 instances, so this is not a big deal for now. See task CDF-25974.
        source_systems = self.client.migration.created_source_system.list(limit=-1)
        self._source_system_mapping_by_id = {
            source_system.source: source_system.as_direct_relation_reference() for source_system in source_systems
        }

    def map(self, source: AssetCentricMapping) -> tuple[InstanceApply, ConversionIssue]:
        """Map a chunk of asset-centric data to InstanceApplyList format."""
        mapping = source.mapping
        ingestion_view = mapping.get_ingestion_view()
        try:
            view_source = self._view_mapping_by_id[ingestion_view]
            view_properties = self._ingestion_view_by_id[view_source.view_id].properties
        except KeyError as e:
            raise RuntimeError(
                f"Failed to lookup mapping or view for ingestion view '{ingestion_view}'. Did you forget to call .prepare()?"
            ) from e
        instance, conversion_issue = asset_centric_to_dm(
            source.resource,
            instance_id=mapping.instance_id,
            view_source=view_source,
            view_properties=view_properties,
            asset_instance_id_by_id=self._asset_mapping_by_id,
            source_instance_id_by_external_id=self._source_system_mapping_by_id,
        )
        if mapping.resource_type == "asset":
            self._asset_mapping_by_id[mapping.id] = DirectRelationReference(
                space=mapping.instance_id.space, external_id=mapping.instance_id.external_id
            )
        return instance, conversion_issue
