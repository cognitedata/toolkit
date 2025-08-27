from cognite.client.data_classes.data_modeling import MappedProperty, NodeApply, NodeId, View
from cognite.client.data_classes.data_modeling.instances import NodeOrEdgeData, PropertyValueWrite

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId, ViewSource
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    asset_centric_convert_to_primary_property,
    convert_to_primary_property,
)

from .base import T_AssetCentricResource
from .data_model import INSTANCE_SOURCE_VIEW_ID
from .issues import ConversionIssue, FailedConversion, InvalidPropertyDataType


def asset_centric_to_dm(
    resource: T_AssetCentricResource, instance_id: NodeId, view_source: ViewSource, view: View
) -> tuple[NodeApply, ConversionIssue]:
    """Convert an asset-centric resource to a data model instance.

    Args:
        resource (T_AssetCentricResource): The asset-centric resource to convert.
        instance_id (NodeId): The ID of the instance to create or update.
        view_source (ViewSource): The view source defining how to map the resource to the data model.
        view (View): The view defining the data model.

    Returns:
        tuple[NodeApply, ConversionIssue]: A tuple containing the converted NodeApply and any ConversionIssue encountered.
    """
    resource_type: Literal = "asset"
    issue = ConversionIssue(asset_centric_id=AssetCentricId(resource_type, id_=resource.id), instance_id=instance_id)
    dumped = resource.dump()
    properties: dict[str, PropertyValueWrite] = {}
    for prop_id, dm_prop_id in view_source.mapping.to_property_id.items():
        if prop_id not in dumped:
            issue.missing_asset_centric_properties.append(prop_id)
            continue
        if dm_prop_id not in view.properties:
            issue.missing_instance_properties.append(prop_id)
            continue
        dm_prop = view.properties[dm_prop_id]
        if not isinstance(dm_prop, MappedProperty):
            issue.invalid_instance_property_types.append(
                InvalidPropertyDataType(property_id=type(dm_prop_id).__name__, expected_type="MappedProperty")
            )
            continue
        try:
            value = asset_centric_convert_to_primary_property(
                dumped[prop_id],
                dm_prop.type,
                dm_prop.nullable,
                (dm_prop.container, dm_prop.container_property_identifier),
                (resource_type, prop_id),
            )
        except ValueError as e:
            issue.failed_conversions.append(FailedConversion(property_id=prop_id, value=dumped[prop_id], error=str(e)))
            continue
        properties[dm_prop_id] = value

    metadata = resource.metadata or {}
    for key, dm_prop_id in view_source.mapping.metadata_to_property_id.items():
        if key not in metadata:
            issue.missing_asset_centric_properties.append(f"metadata.{key}")
            continue
        if dm_prop_id not in view.properties:
            issue.missing_instance_properties.append(key)
            continue
        dm_prop = view.properties[dm_prop_id]
        if not isinstance(dm_prop, MappedProperty):
            issue.invalid_instance_property_types.append(
                InvalidPropertyDataType(property_id=type(dm_prop_id).__name__, expected_type="MappedProperty")
            )
            continue
        try:
            value = convert_to_primary_property(metadata[key], dm_prop.type, dm_prop.nullable)
        except ValueError as e:
            issue.failed_conversions.append(FailedConversion(property_id=key, value=metadata[key], error=str(e)))
            continue
        properties[dm_prop_id] = value

    node = NodeApply(
        space=instance_id.space,
        external_id=instance_id.external_id,
        sources=[
            NodeOrEdgeData(
                source=view_source.view_id,
                properties=properties,
            ),
            NodeOrEdgeData(
                source=INSTANCE_SOURCE_VIEW_ID,
                properties={
                    "resourceType": resource_type,
                    "id": resource.id,
                    "dataSetId": resource.data_set_id,
                    "classicExternalId": resource.external_id,
                },
            ),
        ],
    )

    return node, issue
