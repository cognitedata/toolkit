from collections.abc import Iterable, Mapping, Set
from typing import Any, ClassVar, cast

from cognite.client.data_classes import Annotation, Asset, Event, FileMetadata, TimeSeries
from cognite.client.data_classes.data_modeling import (
    DirectRelation,
    DirectRelationReference,
    EdgeId,
    MappedProperty,
    NodeApply,
    NodeId,
)
from cognite.client.data_classes.data_modeling.instances import EdgeApply, NodeOrEdgeData, PropertyValueWrite
from cognite.client.data_classes.data_modeling.views import ViewProperty
from cognite.client.utils._identifier import InstanceId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId, ResourceViewMapping
from cognite_toolkit._cdf_tk.utils.collection import flatten_dict_json_path
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    asset_centric_convert_to_primary_property,
    convert_to_primary_property,
)
from cognite_toolkit._cdf_tk.utils.useful_types import (
    AssetCentricResourceExtended,
    AssetCentricTypeExtended,
)

from .data_model import INSTANCE_SOURCE_VIEW_ID
from .issues import ConversionIssue, FailedConversion, InvalidPropertyDataType


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
        ("asset", "parentId"),
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

    def update(self, resources: Iterable[AssetCentricResourceExtended]) -> None:
        """Update the cache with direct relation references for the given asset-centric resources.

        This is used to bulk update the cache for a chunk of resources before converting them to data model instances.
        """
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
                if resource.parent_id is not None:
                    asset_ids.add(resource.parent_id)
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
    ) -> Mapping[str | int, DirectRelationReference] | None:
        """Get the cache for the given resource type and property ID."""
        return self._cache_map.get((resource_type, property_id))  # type: ignore[return-value]


def asset_centric_to_dm(
    resource: AssetCentricResourceExtended,
    instance_id: InstanceId,
    view_source: ResourceViewMapping,
    view_properties: dict[str, ViewProperty],
    direct_relation_cache: DirectRelationCache,
) -> tuple[NodeApply | EdgeApply | None, ConversionIssue]:
    """Convert an asset-centric resource to a data model instance.

    Args:
        resource (CogniteResource): The asset-centric resource to convert.
        instance_id (NodeId | EdgeApply): The ID of the instance to create or update.
        view_source (ResourceViewMapping): The view source defining how to map the resource to the data model.
        view_properties (dict[str, ViewProperty]): The defined properties referenced in the view source mapping.
        direct_relation_cache (DirectRelationCache): Cache for direct relation references.

    Returns:
        tuple[NodeApply | EdgeApply, ConversionIssue]: A tuple containing the converted NodeApply and any ConversionIssue encountered.
    """
    resource_type = _lookup_resource_type(resource)
    dumped = resource.dump()
    try:
        id_ = dumped.pop("id")
    except KeyError as e:
        raise ValueError("Resource must have an 'id' field.") from e
    if not isinstance(id_, int):
        raise TypeError(f"Resource 'id' field must be an int, got {type(id_)}.")
    data_set_id = dumped.pop("dataSetId", None)
    external_id = dumped.pop("externalId", None)

    issue = ConversionIssue(asset_centric_id=AssetCentricId(resource_type, id_=id_), instance_id=instance_id)

    properties = create_properties(
        dumped,
        view_properties,
        view_source.property_mapping,
        resource_type,
        issue=issue,
        direct_relation_cache=direct_relation_cache,
    )
    sources: list[NodeOrEdgeData] = []
    if properties:
        sources.append(NodeOrEdgeData(source=view_source.view_id, properties=properties))

    if resource_type != "annotation":
        instance_source_properties = {
            "resourceType": resource_type,
            "id": id_,
            "dataSetId": data_set_id,
            "classicExternalId": external_id,
        }
        sources.append(NodeOrEdgeData(source=INSTANCE_SOURCE_VIEW_ID, properties=instance_source_properties))

    instance: NodeApply | EdgeApply
    if isinstance(instance_id, EdgeId):
        edge_properties = create_edge_properties(
            dumped, view_source.property_mapping, resource_type, issue, direct_relation_cache, instance_id.space
        )
        if any(key not in edge_properties for key in ("start_node", "end_node", "type")):
            # Failed conversion of edge properties
            return None, issue
        instance = EdgeApply(
            space=instance_id.space,
            external_id=instance_id.external_id,
            sources=sources,
            **edge_properties,  # type: ignore[arg-type]
        )
    elif isinstance(instance_id, NodeId):
        instance = NodeApply(space=instance_id.space, external_id=instance_id.external_id, sources=sources)
    else:
        raise RuntimeError(f"Unexpected instance_id type {type(instance_id)}")

    return instance, issue


def _lookup_resource_type(resource_type: AssetCentricResourceExtended) -> AssetCentricTypeExtended:
    if isinstance(resource_type, Asset):
        return "asset"
    elif isinstance(resource_type, FileMetadata):
        return "file"
    elif isinstance(resource_type, Event):
        return "event"
    elif isinstance(resource_type, TimeSeries):
        return "timeseries"
    elif isinstance(resource_type, Annotation):
        if resource_type.annotated_resource_type == "file" and resource_type.annotation_type in (
            "diagrams.AssetLink",
            "diagrams.FileLink",
        ):
            return "annotation"
    raise ValueError(f"Unsupported resource type: {resource_type}")


def create_properties(
    dumped: dict[str, Any],
    view_properties: dict[str, ViewProperty],
    property_mapping: dict[str, str],
    resource_type: AssetCentricTypeExtended,
    issue: ConversionIssue,
    direct_relation_cache: DirectRelationCache,
) -> dict[str, PropertyValueWrite]:
    """
    Create properties for a data model instance from an asset-centric resource.

    Args:
        dumped: Dict representation of the asset-centric resource.
        view_properties: Defined properties referenced in the view source mapping.
        property_mapping:  Mapping from asset-centric property IDs to data model property IDs.
        resource_type: The type of the asset-centric resource (e.g., "asset", "timeseries").
        issue: ConversionIssue object to log any issues encountered during conversion.
        direct_relation_cache: Cache for direct relation references to look up target of direct relations.

    Returns:
        Dict of property IDs to PropertyValueWrite objects.

    """
    flatten_dump = flatten_dict_json_path(dumped, keep_structured=set(property_mapping.keys()))
    properties: dict[str, PropertyValueWrite] = {}
    ignored_asset_centric_properties: set[str] = set()
    for prop_json_path, prop_id in property_mapping.items():
        if prop_json_path not in flatten_dump:
            continue
        if prop_id not in view_properties:
            continue
        if prop_id in properties:
            ignored_asset_centric_properties.add(prop_json_path)
            continue
        dm_prop = view_properties[prop_id]
        if not isinstance(dm_prop, MappedProperty):
            issue.invalid_instance_property_types.append(
                InvalidPropertyDataType(property_id=prop_id, expected_type=MappedProperty.__name__)
            )
            continue
        try:
            value = asset_centric_convert_to_primary_property(
                flatten_dump[prop_json_path],
                dm_prop.type,
                dm_prop.nullable,
                destination_container_property=(dm_prop.container, dm_prop.container_property_identifier),
                source_property=(resource_type, prop_json_path),
                direct_relation_lookup=direct_relation_cache.get_cache(resource_type, prop_json_path),
            )
        except (ValueError, TypeError, NotImplementedError) as e:
            issue.failed_conversions.append(
                FailedConversion(property_id=prop_json_path, value=flatten_dump[prop_json_path], error=str(e))
            )
            continue
        properties[prop_id] = value

    issue.ignored_asset_centric_properties = sorted(
        (set(flatten_dump.keys()) - set(property_mapping.keys())) | ignored_asset_centric_properties
    )
    issue.missing_asset_centric_properties = sorted(set(property_mapping.keys()) - set(flatten_dump.keys()))
    # Node and edge properties are handled separately
    issue.missing_instance_properties = sorted(
        {
            prop_id
            for prop_id in property_mapping.values()
            if not (prop_id.startswith("edge.") or prop_id.startswith("node."))
        }
        - set(view_properties.keys())
    )
    return properties


def create_edge_properties(
    dumped: dict[str, Any],
    property_mapping: dict[str, str],
    resource_type: AssetCentricTypeExtended,
    issue: ConversionIssue,
    direct_relation_cache: DirectRelationCache,
    default_instance_space: str,
) -> dict[str, DirectRelationReference]:
    flatten_dump = flatten_dict_json_path(dumped)
    edge_properties: dict[str, DirectRelationReference] = {}
    for prop_json_path, prop_id in property_mapping.items():
        if not prop_id.startswith("edge."):
            continue
        if prop_json_path not in flatten_dump:
            continue
        edge_prop_id = prop_id.removeprefix("edge.")
        if edge_prop_id in ("startNode", "endNode", "type"):
            # DirectRelation lookup.
            try:
                value = convert_to_primary_property(
                    flatten_dump[prop_json_path],
                    DirectRelation(),
                    False,
                    direct_relation_lookup=direct_relation_cache.get_cache(resource_type, prop_json_path),
                )
            except (ValueError, TypeError, NotImplementedError) as e:
                issue.failed_conversions.append(
                    FailedConversion(property_id=prop_json_path, value=flatten_dump[prop_json_path], error=str(e))
                )
                continue
        elif edge_prop_id.endswith(".externalId"):
            # Just an external ID string.
            edge_prop_id = edge_prop_id.removesuffix(".externalId")
            value = DirectRelationReference(default_instance_space, str(flatten_dump[prop_json_path]))
        else:
            issue.invalid_instance_property_types.append(
                InvalidPropertyDataType(property_id=prop_id, expected_type="EdgeProperty")
            )
            continue
        # We know that value is DirectRelationReference here
        edge_properties[edge_prop_id.replace("Node", "_node")] = value  # type: ignore[assignment]

    return edge_properties
