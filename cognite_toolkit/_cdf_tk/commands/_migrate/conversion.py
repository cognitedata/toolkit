from collections.abc import Iterable, Mapping, Set
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import cached_property
from typing import Any, ClassVar, Literal, cast

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationResponse, AssetLinkData, FileLinkData
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    DirectNodeRelation,
    EdgeId,
    EdgeProperty,
    EdgeRequest,
    EdgeResponse,
    FileCDFExternalIdReference,
    InstanceSource,
    NodeId,
    NodeRequest,
    TimeseriesCDFExternalIdReference,
    ViewCorePropertyResponse,
    ViewId,
    ViewResponseProperty,
)
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import AssetCentricId
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import ResourceViewMappingRequest
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.utils.collection import flatten_dict_json_path
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    asset_centric_convert_to_primary_property,
    convert_to_primary_property,
)
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricTypeExtended
from cognite_toolkit._cdf_tk.utils.useful_types2 import AssetCentricResourceExtended

from .data_model import COGNITE_MIGRATION_SPACE_ID, INSTANCE_SOURCE_VIEW_ID
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
        self._cache_map: dict[tuple[str, str] | str, dict[str, NodeId] | dict[int, NodeId]] = {}
        # Constructing the cache map to be accessed by both table name and property id
        for table_name, properties in [
            (self.TableName.ASSET_ID, self.ASSET_ID_PROPERTIES),
            (self.TableName.SOURCE_NAME, self.SOURCE_NAME_PROPERTIES),
            (self.TableName.FILE_ID, self.FILE_ID_PROPERTIES),
            (self.TableName.ASSET_EXTERNAL_ID, self.ASSET_EXTERNAL_ID_PROPERTIES),
            (self.TableName.FILE_EXTERNAL_ID, self.FILE_EXTERNAL_ID_PROPERTIES),
        ]:
            cache: dict[str, NodeId] | dict[int, NodeId] = {}
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
            if isinstance(resource, AnnotationResponse):
                if resource.annotated_resource_type == "file" and resource.annotated_resource_id:
                    file_ids.add(resource.annotated_resource_id)
                self._extract_annotation_refs(resource.data, asset_ids, asset_external_ids, file_ids, file_external_ids)
            elif isinstance(resource, AssetResponse):
                if resource.source:
                    source_ids.add(resource.source)
                if resource.parent_id is not None:
                    asset_ids.add(resource.parent_id)
            elif isinstance(resource, FileMetadataResponse):
                if resource.source:
                    source_ids.add(resource.source)
                if resource.asset_ids:
                    asset_ids.update(resource.asset_ids)
            elif isinstance(resource, EventResponse):
                if resource.source:
                    source_ids.add(resource.source)
                if resource.asset_ids:
                    asset_ids.update(resource.asset_ids)
            elif isinstance(resource, TimeSeriesResponse):
                if resource.asset_id is not None:
                    asset_ids.add(resource.asset_id)
        if asset_ids:
            self._update_cache(self._client.migration.lookup.assets(id=list(asset_ids)), self.TableName.ASSET_ID)
        if source_ids:
            # SourceSystems are not cached in the client, so we have to handle the caching ourselves.
            cache = cast(dict[str, NodeId], self._cache_map[self.TableName.SOURCE_NAME])
            missing: dict[str, str] = {}
            for source_id in source_ids:
                if source_id.casefold() not in cache:
                    missing[source_id.casefold()] = source_id
                elif source_id not in cache:
                    missing[source_id] = source_id
            if missing:
                source_systems = self._client.migration.created_source_system.retrieve(list(missing))
                for source_system in source_systems:
                    source_reference = source_system.as_direct_relation_reference()
                    cache[source_system.source] = source_reference
                    if original_str := missing.get(source_system.source):
                        cache[original_str] = source_reference
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

    @staticmethod
    def _extract_annotation_refs(
        data: AssetLinkData | FileLinkData | dict[str, Any],
        asset_ids: set[int],
        asset_external_ids: set[str],
        file_ids: set[int],
        file_external_ids: set[str],
    ) -> None:
        if isinstance(data, AssetLinkData):
            if isinstance(data.asset_ref, InternalId):
                asset_ids.add(data.asset_ref.id)
            elif isinstance(data.asset_ref, ExternalId):
                asset_external_ids.add(data.asset_ref.external_id)
        elif isinstance(data, FileLinkData):
            if isinstance(data.file_ref, InternalId):
                file_ids.add(data.file_ref.id)
            elif isinstance(data.file_ref, ExternalId):
                file_external_ids.add(data.file_ref.external_id)

    def _update_cache(self, instance_id_by_id: dict[int, NodeId] | dict[str, NodeId], table_name: str) -> None:
        cache = self._cache_map[table_name]
        for identifier, instance_id in instance_id_by_id.items():
            cache[identifier] = NodeId(space=instance_id.space, external_id=instance_id.external_id)  # type: ignore[index]

    def get_cache(self, resource_type: AssetCentricTypeExtended, property_id: str) -> Mapping[str | int, NodeId] | None:
        """Get the cache for the given resource type and property ID."""
        return self._cache_map.get((resource_type, property_id))  # type: ignore[return-value]


def asset_centric_to_dm(
    resource: AssetCentricResourceExtended,
    instance_id: NodeId | EdgeId,
    view_source: ResourceViewMappingRequest,
    view_properties: dict[str, ViewResponseProperty],
    direct_relation_cache: DirectRelationCache,
    preferred_consumer_view: ViewId | None = None,
) -> tuple[NodeRequest | EdgeRequest | None, ConversionIssue]:
    """Convert an asset-centric resource to a data model instance.

    Args:
        resource (CogniteResource): The asset-centric resource to convert.
        instance_id (NodeId | EdgeApply): The ID of the instance to create or update.
        view_source (ResourceViewMappingRequest): The view source defining how to map the resource to the data model.
        view_properties (dict[str, ViewProperty]): The defined properties referenced in the view source mapping.
        direct_relation_cache (DirectRelationCache): Cache for direct relation references.
        preferred_consumer_view (ViewId | None): The preferred consumer view for the instance.

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

    issue = ConversionIssue(
        id=str(AssetCentricId(resource_type, id_=id_)),
        asset_centric_id=AssetCentricId(resource_type, id_=id_),
        instance_id=NodeId(space=instance_id.space, external_id=instance_id.external_id),
    )

    properties = create_properties(
        dumped,
        view_properties,
        view_source.property_mapping,
        resource_type,
        issue=issue,
        direct_relation_cache=direct_relation_cache,
    )
    sources: list[InstanceSource] = []
    if properties:
        sources.append(
            InstanceSource(
                source=ViewId(
                    space=view_source.view_id.space,
                    external_id=view_source.view_id.external_id,
                    version=view_source.view_id.version,
                ),
                properties=properties,
            )
        )

    if resource_type != "annotation":
        instance_source_properties = {
            "resourceType": resource_type,
            "id": id_,
            "dataSetId": data_set_id,
            "classicExternalId": external_id,
            "resourceViewMapping": {"space": COGNITE_MIGRATION_SPACE_ID, "externalId": view_source.external_id},
        }
        if preferred_consumer_view:
            instance_source_properties["preferredConsumerViewId"] = {
                **preferred_consumer_view.dump(),
                "type": "view",
            }
        sources.append(
            InstanceSource(
                source=ViewId(
                    space=INSTANCE_SOURCE_VIEW_ID.space,
                    external_id=INSTANCE_SOURCE_VIEW_ID.external_id,
                    version=view_source.view_id.version,
                ),
                properties=instance_source_properties,
            )
        )

    instance: NodeRequest | EdgeRequest
    if isinstance(instance_id, EdgeId):
        edge_properties = create_edge_properties(
            dumped, view_source.property_mapping, resource_type, issue, direct_relation_cache, instance_id.space
        )
        if any(key not in edge_properties for key in ("start_node", "end_node", "type")):
            # Failed conversion of edge properties
            return None, issue
        instance = EdgeRequest(
            space=instance_id.space,
            external_id=instance_id.external_id,
            sources=sources,
            **edge_properties,  # type: ignore[arg-type]
        )
    elif isinstance(instance_id, NodeId):
        instance = NodeRequest(space=instance_id.space, external_id=instance_id.external_id, sources=sources)
    else:
        raise RuntimeError(f"Unexpected instance_id type {type(instance_id)}")

    return instance, issue


def _lookup_resource_type(resource_type: AssetCentricResourceExtended) -> AssetCentricTypeExtended:
    if isinstance(resource_type, AssetResponse):
        return "asset"
    elif isinstance(resource_type, FileMetadataResponse):
        return "file"
    elif isinstance(resource_type, EventResponse):
        return "event"
    elif isinstance(resource_type, TimeSeriesResponse):
        return "timeseries"
    elif isinstance(resource_type, AnnotationResponse):
        if resource_type.annotated_resource_type == "file" and resource_type.annotation_type in (
            "diagrams.AssetLink",
            "diagrams.FileLink",
        ):
            return "annotation"
    raise ValueError(f"Unsupported resource type: {resource_type}")


def create_properties(
    dumped: dict[str, Any],
    view_properties: dict[str, ViewResponseProperty],
    property_mapping: dict[str, str],
    resource_type: AssetCentricTypeExtended,
    issue: ConversionIssue,
    direct_relation_cache: DirectRelationCache,
) -> dict[str, JsonValue]:
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
    properties: dict[str, JsonValue] = {}
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
        if not isinstance(dm_prop, ViewCorePropertyResponse):
            issue.invalid_instance_property_types.append(
                InvalidPropertyDataType(property_id=prop_id, expected_type=ViewCorePropertyResponse.__name__)
            )
            continue
        try:
            value = asset_centric_convert_to_primary_property(
                flatten_dump[prop_json_path],
                dm_prop.type,
                dm_prop.nullable or False,
                destination_container_property=(dm_prop.container, dm_prop.container_property_identifier),
                source_property=(resource_type, prop_json_path),
                direct_relation_lookup=direct_relation_cache.get_cache(resource_type, prop_json_path),
            )
        except (ValueError, TypeError, NotImplementedError) as e:
            issue.failed_conversions.append(
                FailedConversion(property_id=prop_json_path, value=flatten_dump[prop_json_path], error=str(e))
            )
            continue
        if isinstance(value, date):
            # Convert date to ISO format string, as the data model expects dates as strings in ISO format.
            properties[prop_id] = value.isoformat()
        elif isinstance(value, datetime):
            # Convert datetime to ISO format string, as the data model expects datetimes as strings in ISO format.
            properties[prop_id] = value.isoformat(timespec="milliseconds")
        else:
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
) -> dict[str, NodeId]:
    flatten_dump = flatten_dict_json_path(dumped)
    edge_properties: dict[str, NodeId] = {}
    for prop_json_path, prop_id in property_mapping.items():
        if not prop_id.startswith("edge."):
            continue
        if prop_json_path not in flatten_dump:
            continue
        edge_prop_id = prop_id.removeprefix("edge.")
        if edge_prop_id in ("startNode", "endNode", "type"):
            value: NodeId | Any
            # DirectRelation lookup.
            try:
                value = convert_to_primary_property(
                    flatten_dump[prop_json_path],
                    DirectNodeRelation(),
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
            value = NodeId(space=default_instance_space, external_id=str(flatten_dump[prop_json_path]))
        else:
            issue.invalid_instance_property_types.append(
                InvalidPropertyDataType(property_id=prop_id, expected_type="EdgeProperty")
            )
            continue
        edge_properties[edge_prop_id.replace("Node", "_node")] = value  # type: ignore[assignment]

    return edge_properties


class TimeSeriesFilesReferenceCache:
    """Cache for looking up timeseries/files reference in classic to find the matching instance ID"""

    def __init__(self, client: ToolkitClient) -> None:
        self._client = client
        self._cache: dict[Literal["timeseries", "file"], dict[str, NodeId]] = {
            "timeseries": {},
            "file": {},
        }

    def update(self, resource_type: Literal["timeseries", "file"], resource_ids: list[str]) -> list[str]:
        resources: list[TimeSeriesResponse] | list[FileMetadataResponse]
        ids = ExternalId.from_external_ids(resource_ids)
        if resource_type == "timeseries":
            resources = self._client.tool.timeseries.retrieve(ids, ignore_unknown_ids=True)
        elif resource_type == "file":
            resources = self._client.tool.filemetadata.retrieve(ids, ignore_unknown_ids=True)
        else:
            raise ValueError(f"Unsupported resource type: {resource_type}")
        missing_instance_ids: list[str] = []
        for resource in resources:
            # We do the lookup on ExternalID, so we know that it will always be set.
            external_id = cast(str, resource.external_id)
            if resource.instance_id is None:
                missing_instance_ids.append(external_id)
            else:
                self._cache[resource_type][external_id] = resource.instance_id
        return missing_instance_ids

    def get_cache(self, resource_type: Literal["timeseries", "file"]) -> dict[str, NodeId]:
        return self._cache.get(resource_type, {})


class ConnectionCreator:
    def __init__(self, client: ToolkitClient) -> None:
        self._client = client

    def create_edges(
        self, value: Any, dm_prop: EdgeProperty, source_prop_id: str, source_view_id: ViewId
    ) -> list[EdgeRequest]:
        raise NotImplementedError("Edge creation logic is not implemented yet.")

    def create_direct_relation(
        self, value: Any, dm_prop: DirectNodeRelation, source_prop_id: str, source_view_id: ViewId
    ) -> NodeId | list[NodeId]:
        raise NotImplementedError("Direct relation creation logic is not implemented yet.")

    def create_direct_relation_from_edges(self, edges: list[EdgeRequest]) -> NodeId | list[NodeId]:
        raise NotImplementedError("Direct relation creation from edges logic is not implemented yet.")

    def create_edges_from_edges(self, edges: list[EdgeRequest]) -> list[EdgeRequest]:
        raise NotImplementedError("Edge creation from edges logic is not implemented yet.")


class ConversionSourceView:
    """Represents a source view for node-to-node conversion."""

    def __init__(self, view_properties: dict[str, ViewResponseProperty]) -> None:
        self._view_properties = view_properties

    @cached_property
    def timeseries_reference_property_ids(self) -> Set[str]:
        return {
            prop_id
            for prop_id, prop in self._view_properties.items()
            if isinstance(prop, ViewCorePropertyResponse) and isinstance(prop.type, TimeseriesCDFExternalIdReference)
        }

    @cached_property
    def file_reference_property_ids(self) -> Set[str]:
        """All property IDs in the source view that are file reference properties."""
        return {
            prop_id
            for prop_id, prop in self._view_properties.items()
            if isinstance(prop, ViewCorePropertyResponse) and isinstance(prop.type, FileCDFExternalIdReference)
        }

    @cached_property
    def edges(self) -> dict[str, EdgeProperty]:
        """All edge properties in the source view."""
        return {prop_id: prop for prop_id, prop in self._view_properties.items() if isinstance(prop, EdgeProperty)}


@dataclass
class ConversionResult:
    container_properties: dict[str, JsonValue]
    errors: list[str] = field(default_factory=list)
    edges: list[EdgeRequest] = field(default_factory=list)


def convert_container_properties(
    source_properties: dict[str, JsonValue],
    mapping: ViewToViewMapping,
    destination_properties: dict[str, ViewResponseProperty],
    connection_creator: ConnectionCreator,
    source_view_id: ViewId,
) -> ConversionResult:
    """
    Create properties for a data model instance from another instance's properties.

    This is used for node-to-node conversion when the source and destination views have different defined properties.

    Args:
        source_properties: Dict of source property IDs to values.
        mapping: The ViewToViewMapping defining how to map properties from the source view to the destination view.
        destination_properties: Dict of defined properties in the destination view.
        connection_creator: Helper object to create connections (edges and direct relations) based on property values.
        source_view_id: The ID of the source view, used for error messages.
    """
    created_properties: dict[str, JsonValue] = {}
    edges: list[EdgeRequest] = []
    errors: list[str] = []
    for source_prop_id, value in source_properties.items():
        dest_prop_id = mapping.get_destination_property(source_prop_id)
        if not dest_prop_id or (
            dest_prop_id not in destination_properties and dest_prop_id not in mapping.property_mapping
        ):
            # We do not warn about the node properties, as they are typically ignored.
            if not source_prop_id.startswith("node."):
                errors.append(f"Source instance property {source_prop_id!r} is not mapped to any destination property.")
            continue
        if dest_prop_id not in destination_properties:
            errors.append(f"Destination instance is missing property {dest_prop_id!r}.")
            continue

        dm_prop = destination_properties[dest_prop_id]
        if isinstance(dm_prop, EdgeProperty):
            try:
                created_edges = connection_creator.create_edges(
                    value,
                    dm_prop,
                    source_prop_id,
                    source_view_id,
                )
            except ValueError as e:
                errors.append(f"Failed to create edges for property {source_prop_id!r} with value {value!r}: {e!s}")
                continue
            edges.extend(created_edges)
        elif isinstance(dm_prop, ViewCorePropertyResponse) and isinstance(dm_prop.type, DirectNodeRelation):
            try:
                created_connection = connection_creator.create_direct_relation(
                    value,
                    dm_prop.type,
                    source_prop_id,
                    source_view_id,
                )
            except ValueError as e:
                errors.append(
                    f"Failed to create direct relation for property {source_prop_id!r} with value {value!r}: {e!s}"
                )
                continue
            if isinstance(created_connection, list):
                created_properties[dest_prop_id] = [
                    conn.dump(include_instance_type=False) for conn in created_connection
                ]
            else:
                created_properties[dest_prop_id] = created_connection.dump(include_instance_type=False)
        elif isinstance(dm_prop, ViewCorePropertyResponse):
            try:
                created_value = convert_to_primary_property(
                    value,
                    dm_prop.type,
                    dm_prop.nullable if dm_prop.nullable is not None else True,
                )
                if isinstance(created_value, date):
                    created_properties[dest_prop_id] = created_value.isoformat()
                elif isinstance(created_value, datetime):
                    created_properties[dest_prop_id] = created_value.isoformat(timespec="milliseconds")
                else:
                    created_properties[dest_prop_id] = created_value
            except (ValueError, TypeError, NotImplementedError) as e:
                errors.append(f"Failed to convert property {source_prop_id!r} with value {value!r}: {e!s}")
        # Else reverse direct relation, which we assume is handled in the other direction and thus ignore here.

    return ConversionResult(container_properties=created_properties, edges=edges, errors=errors)


def convert_edges(
    edge_targets_by_type_and_direction: dict[tuple[NodeId, Literal["outwards", "inwards"]], list[EdgeResponse]],
    mapping: ViewToViewMapping,
    destination_properties: dict[str, ViewResponseProperty],
    source_edges: dict[str, EdgeProperty],
    source_id: NodeId,
    connection_creator: Any,
    source_view_id: ViewId,
) -> ConversionResult:
    created_container_properties: dict[str, JsonValue] = {}
    created_edges: list[EdgeRequest] = []
    errors: list[str] = []
    for prop_id, source_edge_def in source_edges.items():
        edge_targets = edge_targets_by_type_and_direction.get((source_edge_def.type, source_edge_def.direction), [])
        if not edge_targets:
            continue

        dest_prop_id = mapping.get_destination_property(prop_id)
        if not dest_prop_id or dest_prop_id not in destination_properties:
            # Already captured as missing instance property in 'conver_container_properties', so we can just ignore it here.
            continue

        dm_prop = destination_properties[dest_prop_id]

        if isinstance(dm_prop, ViewCorePropertyResponse) and isinstance(dm_prop.type, DirectNodeRelation):
            try:
                created_connection = connection_creator.create_direct_relation_from_edges(
                    edge_targets,
                    dm_prop.type,
                    prop_id,
                    source_view_id,
                )
            except ValueError as e:
                errors.append(
                    f"Failed to create direct relation for edge property {prop_id!r} with targets {[target.dump() for target in edge_targets]!r}: {e!s}"
                )
                continue
            if isinstance(created_connection, list):
                created_container_properties[dest_prop_id] = [
                    conn.dump(include_instance_type=False) for conn in created_connection
                ]
            else:
                created_container_properties[dest_prop_id] = created_connection.dump(include_instance_type=False)
        elif isinstance(dm_prop, ViewCorePropertyResponse):
            # Todo: If json or text we can potentially convert to a string representation of the edge targets, but for now we just log an error.
            errors.append(f"Cannot map edge property {prop_id!r} to non-connection property {dm_prop.type.type!s}.")
        elif isinstance(dm_prop, EdgeProperty):
            try:
                created_edges = connection_creator.create_edges_from_edges(
                    edge_targets,
                    dm_prop,
                    prop_id,
                    source_id,
                    source_view_id,
                )
            except ValueError as e:
                errors.append(
                    f"Failed to create edges for edge property {prop_id!r} with targets {[target.dump() for target in edge_targets]!r}: {e!s}"
                )
                continue
            created_edges.extend(created_edges)
        # else reverse direct relation, which we assume is handled in the other direction and thus ignore here.

    return ConversionResult(container_properties=created_container_properties, edges=created_edges, errors=errors)
