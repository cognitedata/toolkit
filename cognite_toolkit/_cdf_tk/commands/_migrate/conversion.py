from collections.abc import Mapping, Set
from dataclasses import dataclass
from typing import Any, ClassVar, overload

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

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId, ResourceViewMapping
from cognite_toolkit._cdf_tk.utils.collection import flatten_dict_json_path
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    asset_centric_convert_to_primary_property,
    convert_to_primary_property,
)
from cognite_toolkit._cdf_tk.utils.useful_types import (
    AssetCentricResourceExtended,
    AssetCentricType,
)

from .data_model import INSTANCE_SOURCE_VIEW_ID
from .issues import ConversionIssue, FailedConversion, InvalidPropertyDataType


@dataclass
class DirectRelationCache:
    """Cache for direct relation references to look up target of direct relations.

    This is used when creating direct relations from asset-centric resources to CogniteAsset and CogniteSourceSystem.

    Attributes:
        asset: Mapping[int, DirectRelationReference]
            A mapping from asset IDs to their corresponding DirectRelationReference in the data model.
        source: Mapping[str, DirectRelationReference]
            A mapping from source strings to their corresponding DirectRelationReference in the data model.

    Methods:
        get(resource_type: AssetCentric, property_id: str) -> Mapping[str | int, DirectRelationReference]:
            Get the appropriate mapping based on the resource type and property ID.

    Note:
        The ASSET_REFERENCE_PROPERTIES and SOURCE_REFERENCE_PROPERTIES class variables define which properties
        in asset-centric resources reference assets and sources, respectively.

    """

    ASSET_REFERENCE_PROPERTIES: ClassVar[Set[tuple[AssetCentricType, str]]] = {
        ("timeseries", "assetId"),
        ("file", "assetIds"),
        ("event", "assetIds"),
        ("sequence", "assetId"),
        ("asset", "parentId"),
        ("fileAnnotation", "data.assetRef.id"),
    }
    SOURCE_REFERENCE_PROPERTIES: ClassVar[Set[tuple[AssetCentricType, str]]] = {
        ("asset", "source"),
        ("event", "source"),
        ("file", "source"),
    }
    FILE_REFERENCE_PROPERTIES: ClassVar[Set[tuple[AssetCentricType, str]]] = {
        ("fileAnnotation", "data.fileRef.id"),
        ("fileAnnotation", "annotatedResourceId"),
    }

    asset: Mapping[int, DirectRelationReference]
    source: Mapping[str, DirectRelationReference]
    file: Mapping[int, DirectRelationReference]

    def get(self, resource_type: AssetCentricType, property_id: str) -> Mapping[str | int, DirectRelationReference]:
        key = resource_type, property_id
        if key in self.ASSET_REFERENCE_PROPERTIES:
            return self.asset  # type: ignore[return-value]
        if key in self.SOURCE_REFERENCE_PROPERTIES:
            return self.source  # type: ignore[return-value]
        if key in self.FILE_REFERENCE_PROPERTIES:
            return self.file  # type: ignore[return-value]
        return {}


@overload
def asset_centric_to_dm(
    resource: AssetCentricResourceExtended,
    instance_id: NodeId,
    view_source: ResourceViewMapping,
    view_properties: dict[str, ViewProperty],
    asset_instance_id_by_id: Mapping[int, DirectRelationReference],
    source_instance_id_by_external_id: Mapping[str, DirectRelationReference],
    file_instance_id_by_id: Mapping[int, DirectRelationReference],
) -> tuple[NodeApply | None, ConversionIssue]: ...


@overload
def asset_centric_to_dm(
    resource: AssetCentricResourceExtended,
    instance_id: EdgeId,
    view_source: ResourceViewMapping,
    view_properties: dict[str, ViewProperty],
    asset_instance_id_by_id: Mapping[int, DirectRelationReference],
    source_instance_id_by_external_id: Mapping[str, DirectRelationReference],
    file_instance_id_by_id: Mapping[int, DirectRelationReference],
) -> tuple[EdgeApply | None, ConversionIssue]: ...


def asset_centric_to_dm(
    resource: AssetCentricResourceExtended,
    instance_id: NodeId | EdgeId,
    view_source: ResourceViewMapping,
    view_properties: dict[str, ViewProperty],
    asset_instance_id_by_id: Mapping[int, DirectRelationReference],
    source_instance_id_by_external_id: Mapping[str, DirectRelationReference],
    file_instance_id_by_id: Mapping[int, DirectRelationReference],
) -> tuple[NodeApply | EdgeApply | None, ConversionIssue]:
    """Convert an asset-centric resource to a data model instance.

    Args:
        resource (CogniteResource): The asset-centric resource to convert.
        instance_id (NodeId | EdgeApply): The ID of the instance to create or update.
        view_source (ResourceViewMapping): The view source defining how to map the resource to the data model.
        view_properties (dict[str, ViewProperty]): The defined properties referenced in the view source mapping.
        asset_instance_id_by_id (dict[int, DirectRelationReference]): A mapping from asset IDs to their corresponding
            DirectRelationReference in the data model. This is used to create direct relations for resources that
            reference assets.
        source_instance_id_by_external_id (dict[str, DirectRelationReference]): A mapping from source strings to their
            corresponding DirectRelationReference in the data model. This is used to create direct relations for resources
            that reference sources.
        file_instance_id_by_id (dict[int, DirectRelationReference]): A mapping from file IDs to their corresponding
            DirectRelationReference in the data model. This is used to create direct relations for resources that
            reference files.

    Returns:
        tuple[NodeApply | EdgeApply, ConversionIssue]: A tuple containing the converted NodeApply and any ConversionIssue encountered.
    """
    cache = DirectRelationCache(
        asset=asset_instance_id_by_id, source=source_instance_id_by_external_id, file=file_instance_id_by_id
    )
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
        direct_relation_cache=cache,
    )
    sources: list[NodeOrEdgeData] = []
    if properties:
        sources.append(NodeOrEdgeData(source=view_source.view_id, properties=properties))

    if resource_type != "fileAnnotation":
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
            dumped, view_source.property_mapping, resource_type, issue, cache, instance_id.space
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


def _lookup_resource_type(resource_type: AssetCentricResourceExtended) -> AssetCentricType:
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
            return "fileAnnotation"
    raise ValueError(f"Unsupported resource type: {resource_type}")


def create_properties(
    dumped: dict[str, Any],
    view_properties: dict[str, ViewProperty],
    property_mapping: dict[str, str],
    resource_type: AssetCentricType,
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
                direct_relation_lookup=direct_relation_cache.get(resource_type, prop_json_path),
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
    resource_type: AssetCentricType,
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
                    direct_relation_lookup=direct_relation_cache.get(resource_type, prop_json_path),
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
