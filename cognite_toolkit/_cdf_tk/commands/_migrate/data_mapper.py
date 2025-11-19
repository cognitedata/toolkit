from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence, Set
from typing import ClassVar, Generic, cast

from cognite.client.data_classes import Annotation, Asset, Event, FileMetadata, TimeSeries
from cognite.client.data_classes._base import (
    T_CogniteResource,
)
from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    EdgeApply,
    InstanceApply,
    NodeApply,
    NodeId,
    View,
    ViewId,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.migration import ResourceViewMapping
from cognite_toolkit._cdf_tk.commands._migrate.conversion import asset_centric_to_dm
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import AssetCentricMapping
from cognite_toolkit._cdf_tk.commands._migrate.issues import ConversionIssue, MigrationIssue
from cognite_toolkit._cdf_tk.commands._migrate.selectors import AssetCentricMigrationSelector
from cognite_toolkit._cdf_tk.constants import MISSING_INSTANCE_SPACE
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio._base import T_Selector, T_WriteCogniteResource
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.useful_types import (
    AssetCentricResourceExtended,
    AssetCentricTypeExtended,
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


class DirectRelationCache:
    """Cache for direct relation references to look up target of direct relations.

    This is used when creating direct relations from asset-centric resources to assets, files, and source systems.
    """

    class TableName:
        ASSET_ID = "assetId"
        SOURCE_NAME = "source"
        FILE_ID = "fileId"
        ASSET_EXTERNAL_ID = "assetExternalId"
        FILE_EXTERNAL_ID = "fileExternalId"

    ASSET_ID_PROPERTIES: ClassVar[Set[tuple[str, str]]] = {
        ("timeseries", "assetId"),
        ("file", "assetIds"),
        ("event", "assetIds"),
        ("sequence", "assetId"),
        ("annotation", "data.assetRef.id"),
    }
    SOURCE_NAME_PROPERTIES: ClassVar[Set[tuple[str, str]]] = {
        ("asset", "source"),
        ("event", "source"),
        ("file", "source"),
    }
    FILE_ID_PROPERTIES: ClassVar[Set[tuple[str, str]]] = {
        ("annotation", "data.fileRef.id"),
        ("annotation", "annotatedResourceId"),
    }
    ASSET_EXTERNAL_ID_PROPERTIES: ClassVar[Set[tuple[str, str]]] = {("annotation", "data.assetRef.externalId")}
    FILE_EXTERNAL_ID_PROPERTIES: ClassVar[Set[tuple[str, str]]] = {("annotation", "data.fileRef.externalId")}

    def __init__(self, client: ToolkitClient) -> None:
        self._client = client
        self._cache_map: dict[
            tuple[str, str] | str, dict[str, DirectRelationReference] | dict[int, DirectRelationReference]
        ] = {}
        # Constructing the cache map to be accessed by both table name and property id
        for table_name, properties in [
            (self.TableName.ASSET_ID, self.ASSET_ID_PROPERTIES),
            (self.TableName.SOURCE_NAME, self.SOURCE_NAME_PROPERTIES),
            (self.TableName.FILE_ID, self.FILE_ID_PROPERTIES),
            (self.TableName.ASSET_EXTERNAL_ID, self.ASSET_EXTERNAL_ID_PROPERTIES),
            (self.TableName.FILE_EXTERNAL_ID, self.FILE_EXTERNAL_ID_PROPERTIES),
        ]:
            cache: dict[str, DirectRelationReference] | dict[int, DirectRelationReference] = {}
            self._cache_map[table_name] = cache
            for key in properties:
                self._cache_map[key] = cache

    def update(self, resources: Sequence[AssetCentricResourceExtended]) -> None:
        asset_ids: set[int] = set()
        source_ids: set[str] = set()
        file_ids: set[int] = set()
        asset_external_ids: set[str] = set()
        file_external_ids: set[str] = set()
        for resource in resources:
            if isinstance(resource, Annotation):
                if resource.annotated_resource_type == "file" and resource.annotated_resource_id:
                    file_ids.add(resource.annotated_resource_id)
                if "assetRef" in resource.data:
                    asset_ref = resource.data["assetRef"]
                    if isinstance(asset_id := asset_ref.get("id"), int):
                        asset_ids.add(asset_id)
                    if isinstance(asset_external_id := asset_ref.get("externalId"), str):
                        asset_external_ids.add(asset_external_id)
                if "fileRef" in resource.data:
                    file_ref = resource.data["fileRef"]
                    if isinstance(file_id := file_ref.get("id"), int):
                        file_ids.add(file_id)
                    if isinstance(file_external_id := file_ref.get("externalId"), str):
                        file_external_ids.add(file_external_id)
            elif isinstance(resource, Asset):
                if resource.source:
                    source_ids.add(resource.source)
            elif isinstance(resource, FileMetadata):
                if resource.source:
                    source_ids.add(resource.source)
                if resource.asset_ids:
                    asset_ids.update(resource.asset_ids)
            elif isinstance(resource, Event):
                if resource.source:
                    source_ids.add(resource.source)
                if resource.asset_ids:
                    asset_ids.update(resource.asset_ids)
            elif isinstance(resource, TimeSeries):
                if resource.asset_id is not None:
                    asset_ids.add(resource.asset_id)
        if asset_ids:
            self._update_cache(self._client.migration.lookup.assets(id=list(asset_ids)), self.TableName.ASSET_ID)
        if source_ids:
            # SourceSystems are not cached in the client, so we have to handle the caching ourselves.
            cache = cast(dict[str, DirectRelationReference], self._cache_map[self.TableName.SOURCE_NAME])
            missing = set(source_ids) - set(cache.keys())
            if missing:
                source_systems = self._client.migration.created_source_system.retrieve(list(missing))
                for source_system in source_systems:
                    cache[source_system.source] = source_system.as_direct_relation_reference()
        if file_ids:
            self._update_cache(self._client.migration.lookup.files(id=list(file_ids)), self.TableName.FILE_ID)
        if asset_external_ids:
            self._update_cache(
                self._client.migration.lookup.assets(external_id=list(asset_external_ids)),
                self.TableName.ASSET_EXTERNAL_ID,
            )
        if file_external_ids:
            self._update_cache(
                self._client.migration.lookup.files(external_id=list(file_external_ids)),
                self.TableName.FILE_EXTERNAL_ID,
            )

    def _update_cache(self, instance_id_by_id: dict[int, NodeId] | dict[str, NodeId], table_name: str) -> None:
        cache = self._cache_map[table_name]
        for identifier, instance_id in instance_id_by_id.items():
            cache[identifier] = DirectRelationReference(space=instance_id.space, external_id=instance_id.external_id)  # type: ignore[index]

    def get_cache(
        self, resource_type: AssetCentricTypeExtended, property_id: str
    ) -> Mapping[str, DirectRelationReference] | Mapping[int, DirectRelationReference] | None:
        key = resource_type, property_id
        return self._cache_map.get(key)


class AssetCentricMapper(
    DataMapper[AssetCentricMigrationSelector, AssetCentricMapping[T_AssetCentricResource], InstanceApply]
):
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client
        self._ingestion_view_by_id: dict[ViewId, View] = {}
        self._view_mapping_by_id: dict[str, ResourceViewMapping] = {}
        # This is used to keep track of already mapped assets, such that we can creat direct relations
        # to them from files, events, and time series.
        self._asset_mapping_by_id: dict[int, DirectRelationReference] = {}
        self._source_system_mapping_by_id: dict[str, DirectRelationReference] = {}

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
        # We just look-up all source system for now. This can be optimized to only
        # look-up the ones that are actually used in the ingestion views. However, SourceSystem is typically in
        # the order ~10 instances, so this is not a big deal for now. See task CDF-25974.
        source_systems = self.client.migration.created_source_system.list(limit=-1)
        self._source_system_mapping_by_id = {
            source_system.source: source_system.as_direct_relation_reference() for source_system in source_systems
        }

        # We look-up all existing asset mappings to be able to create direct relations to already mapped assets.
        # This is needed in case the migration is run multiple times, or if assets are mapped
        asset_mappings = self.client.migration.instance_source.list(resource_type="asset", limit=-1)
        self._asset_mapping_by_id = {mapping.id_: mapping.as_direct_relation_reference() for mapping in asset_mappings}

    def map(
        self, source: Sequence[AssetCentricMapping[T_AssetCentricResource]]
    ) -> Sequence[tuple[InstanceApply | None, ConversionIssue]]:
        """Map a chunk of asset-centric data to InstanceApplyList format."""

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
            asset_instance_id_by_id=self._asset_mapping_by_id,
            source_instance_id_by_external_id=self._source_system_mapping_by_id,
            file_instance_id_by_id={},
        )
        if mapping.instance_id.space == MISSING_INSTANCE_SPACE:
            conversion_issue.missing_instance_space = f"Missing instance space for dataset ID {mapping.data_set_id!r}"

        if mapping.resource_type == "asset":
            self._asset_mapping_by_id[mapping.id] = DirectRelationReference(
                space=mapping.instance_id.space, external_id=mapping.instance_id.external_id
            )
        return instance, conversion_issue
