from typing import Any

from cognite.client.data_classes import Asset, Event, FileMetadata, Sequence, TimeSeries
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling import MappedProperty, NodeApply, NodeId
from cognite.client.data_classes.data_modeling.instances import NodeOrEdgeData, PropertyValueWrite
from cognite.client.data_classes.data_modeling.views import ViewProperty

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId, ResourceViewMapping
from cognite_toolkit._cdf_tk.utils.collection import flatten_dict_json_path
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    asset_centric_convert_to_primary_property,
)
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentric

from .data_model import INSTANCE_SOURCE_VIEW_ID
from .issues import ConversionIssue, FailedConversion, InvalidPropertyDataType


def asset_centric_to_dm(
    resource: CogniteResource,
    instance_id: NodeId,
    view_source: ResourceViewMapping,
    view_properties: dict[str, ViewProperty],
) -> tuple[NodeApply, ConversionIssue]:
    """Convert an asset-centric resource to a data model instance.

    Args:
        resource (CogniteResource): The asset-centric resource to convert.
        instance_id (NodeId): The ID of the instance to create or update.
        view_source (ResourceViewMapping): The view source defining how to map the resource to the data model.
        view_properties (dict[str, ViewProperty]): The defined properties referenced in the view source mapping.

    Returns:
        tuple[NodeApply, ConversionIssue]: A tuple containing the converted NodeApply and any ConversionIssue encountered.
    """
    resource_type = _lookup_resource_type(type(resource))
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
    )
    sources: list[NodeOrEdgeData] = []
    if properties:
        sources.append(NodeOrEdgeData(source=view_source.view_id, properties=properties))
    instance_source_properties = {
        "resourceType": resource_type,
        "id": id_,
        "dataSetId": data_set_id,
        "classicExternalId": external_id,
    }
    sources.append(NodeOrEdgeData(source=INSTANCE_SOURCE_VIEW_ID, properties=instance_source_properties))

    node = NodeApply(
        space=instance_id.space,
        external_id=instance_id.external_id,
        sources=sources,
    )

    return node, issue


def _lookup_resource_type(resource_type: type[CogniteResource]) -> AssetCentric:
    resource_type_map: dict[type[CogniteResource], AssetCentric] = {
        Asset: "asset",
        FileMetadata: "file",
        Event: "event",
        TimeSeries: "timeseries",
        Sequence: "sequence",
    }
    try:
        return resource_type_map[resource_type]
    except KeyError as e:
        raise ValueError(f"Unsupported resource type: {resource_type}") from e


def create_properties(
    dumped: dict[str, Any],
    view_properties: dict[str, ViewProperty],
    property_mapping: dict[str, str],
    resource_type: AssetCentric,
    issue: ConversionIssue,
) -> dict[str, PropertyValueWrite]:
    """
    Create properties for a data model instance from an asset-centric resource.

    Args:
        dumped: Dict representation of the asset-centric resource.
        view_properties: Defined properties referenced in the view source mapping.
        property_mapping:  Mapping from asset-centric property IDs to data model property IDs.
        resource_type: The type of the asset-centric resource (e.g., "asset", "timeseries").
        issue: ConversionIssue object to log any issues encountered during conversion.

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
    issue.missing_instance_properties = sorted(set(property_mapping.values()) - set(view_properties.keys()))
    return properties
