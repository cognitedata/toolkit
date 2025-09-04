from collections.abc import Callable
from typing import Any

from cognite.client.data_classes import Asset, Event, FileMetadata, Sequence, TimeSeries
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling import MappedProperty, NodeApply, NodeId
from cognite.client.data_classes.data_modeling.instances import NodeOrEdgeData, PropertyValueWrite
from cognite.client.data_classes.data_modeling.views import ViewProperty

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId, ViewSource
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    asset_centric_convert_to_primary_property,
    convert_to_primary_property,
)
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentric

from .base import T_AssetCentricResource
from .data_model import INSTANCE_SOURCE_VIEW_ID
from .issues import ConversionIssue, FailedConversion, InvalidPropertyDataType

# These properties are not expected to be mapped to the instance.
_RESERVED_ASSET_CENTRIC_PROPERTIES = frozenset({"metadata", "externalId", "dataSetId", "parentId", "id"})


def asset_centric_to_dm(
    resource: T_AssetCentricResource,
    instance_id: NodeId,
    view_source: ViewSource,
    view_properties: dict[str, ViewProperty],
) -> tuple[NodeApply, ConversionIssue]:
    """Convert an asset-centric resource to a data model instance.

    Args:
        resource (T_AssetCentricResource): The asset-centric resource to convert.
        instance_id (NodeId): The ID of the instance to create or update.
        view_source (ViewSource): The view source defining how to map the resource to the data model.
        view_properties (dict[str, ViewProperty]): The defined properties referenced in the view source mapping.

    Returns:
        tuple[NodeApply, ConversionIssue]: A tuple containing the converted NodeApply and any ConversionIssue encountered.
    """
    resource_type = _lookup_resource_type(type(resource))
    dumped = resource.dump()
    available_properties = (
        set(dumped.keys()) | {f"metadata.{key}" for key in resource.metadata or {}}
    ) - _RESERVED_ASSET_CENTRIC_PROPERTIES
    expected_properties = set(view_source.mapping.to_property_id.keys()) | {
        f"metadata.{key}" for key in (view_source.mapping.metadata_to_property_id or {}).keys()
    }

    issue = ConversionIssue(
        asset_centric_id=AssetCentricId(resource_type, id_=resource.id),
        instance_id=instance_id,
        ignored_asset_centric_properties=sorted(available_properties - expected_properties),
    )

    properties = _create_properties(
        dumped,
        issue,
        conversion=lambda value, prop_id, dm_prop: asset_centric_convert_to_primary_property(
            value,
            dm_prop.type,
            dm_prop.nullable,
            destination_container_property=(dm_prop.container, dm_prop.container_property_identifier),
            source_property=(resource_type, prop_id),
        ),
        view_properties=view_properties,
        asset_centric_to_instance=view_source.mapping.to_property_id,
    )
    if view_source.mapping.metadata_to_property_id is not None:
        metadata_properties = _create_properties(
            resource.metadata or {},
            issue,
            conversion=lambda value, prop_id, dm_prop: convert_to_primary_property(
                value,
                dm_prop.type,
                dm_prop.nullable,
            ),
            view_properties=view_properties,
            asset_centric_to_instance=view_source.mapping.metadata_to_property_id,
            source_prefix="metadata.",
        )
        for key, value in metadata_properties.items():
            if key not in properties:
                properties[key] = value
            else:
                issue.ignored_asset_centric_properties.append(f"metadata.{key}")

    sources: list[NodeOrEdgeData] = []
    if properties:
        sources.append(NodeOrEdgeData(source=view_source.view_id, properties=properties))
    sources.append(
        NodeOrEdgeData(
            source=INSTANCE_SOURCE_VIEW_ID,
            properties={
                "resourceType": resource_type,
                "id": resource.id,
                "dataSetId": resource.data_set_id,
                "classicExternalId": resource.external_id,
            },
        )
    )

    node = NodeApply(
        space=instance_id.space,
        external_id=instance_id.external_id,
        sources=sources,
    )

    return node, issue


def _lookup_resource_type(resource_type: type[T_AssetCentricResource]) -> AssetCentric:
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


def _create_properties(
    dumped: dict[str, Any],
    issue: ConversionIssue,
    conversion: Callable[[Any, str, MappedProperty], PropertyValueWrite],
    view_properties: dict[str, ViewProperty],
    asset_centric_to_instance: dict[str, str],
    source_prefix: str = "",
) -> dict[str, PropertyValueWrite]:
    properties: dict[str, PropertyValueWrite] = {}
    for prop_id, dm_prop_id in asset_centric_to_instance.items():
        if prop_id not in dumped:
            issue.missing_asset_centric_properties.append(f"{source_prefix}{prop_id}")
            continue
        if dm_prop_id not in view_properties:
            issue.missing_instance_properties.append(dm_prop_id)
            continue
        dm_prop = view_properties[dm_prop_id]
        if not isinstance(dm_prop, MappedProperty):
            issue.invalid_instance_property_types.append(
                InvalidPropertyDataType(property_id=dm_prop_id, expected_type=MappedProperty.__name__)
            )
            continue
        try:
            value = conversion(dumped[prop_id], prop_id, dm_prop)
        except (ValueError, TypeError) as e:
            issue.failed_conversions.append(FailedConversion(property_id=prop_id, value=dumped[prop_id], error=str(e)))
            continue
        properties[dm_prop_id] = value
    return properties
