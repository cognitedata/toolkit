"""Migration of InFieldLocationConfig fields from old APM Config format.

This module orchestrates the migration of all InFieldLocationConfig fields.
Individual field migrations are handled in separate modules for better organization.
"""

from typing import Any

from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.exceptions import CogniteAPIError

from .app_instance_space import migrate_app_instance_space
from .feature_toggles import migrate_feature_toggles
from .root_asset import migrate_root_asset


def apply_location_config_fields(location_dict: dict[str, Any], client: Any = None) -> dict[str, Any]:
    """Apply migrated fields to InFieldLocationConfig properties.

    This function applies all migrated fields from the old configuration
    to the InFieldLocationConfig properties. Fields are migrated incrementally
    as features are implemented.

    Currently migrated fields:
    - featureToggles: From featureToggles in old configuration
    - rootAsset: Direct relation from sourceDataInstanceSpace and assetExternalId (only if asset exists)
    - appInstanceSpace: From appDataInstanceSpace in old configuration

    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))
        client: Optional client to verify asset existence before including rootAsset

    Returns:
        Dictionary of properties to add to InFieldLocationConfig
    """
    props: dict[str, Any] = {}

    # Migrate featureToggles
    feature_toggles = migrate_feature_toggles(location_dict)
    if feature_toggles is not None:
        props["featureToggles"] = feature_toggles

    # Migrate rootAsset - only include if asset exists (to avoid auto-create errors)
    # TODO: Re-enable when asset data is ready in the project
    # root_asset = migrate_root_asset(location_dict)
    # if root_asset is not None and _asset_exists(root_asset, client):
    #     props["rootAsset"] = root_asset

    # Migrate appInstanceSpace
    app_instance_space = migrate_app_instance_space(location_dict)
    if app_instance_space is not None:
        props["appInstanceSpace"] = app_instance_space

    # TODO: Add more field migrations here as they are implemented
    # - threeDConfiguration
    # - dataFilters
    # - observations
    # etc.

    return props


def _asset_exists(root_asset: DirectRelationReference, client: Any | None) -> bool:
    """Check if the asset node exists in CDF.
    
    Args:
        root_asset: DirectRelationReference to the asset
        client: ToolkitClient or None
        
    Returns:
        True if asset exists, False otherwise (including if client is None)
    """
    if client is None:
        # If no client provided, we can't verify - return False to skip it
        # (to avoid auto-create errors)
        return False
    
    try:
        # Try to retrieve the node to verify it exists
        from cognite.client.data_classes.data_modeling import NodeId
        node_id = NodeId(space=root_asset.space, external_id=root_asset.external_id)
        result = client.data_modeling.instances.retrieve(nodes=[node_id])
        # Check if we actually got a node back (retrieve may return empty list if node doesn't exist)
        return len(result.nodes) > 0
    except CogniteAPIError:
        # Asset doesn't exist or we can't access it
        return False

