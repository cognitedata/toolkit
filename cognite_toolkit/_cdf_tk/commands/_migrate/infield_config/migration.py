"""Main migration functions for InField V2 config migration."""

from dataclasses import dataclass
from typing import Any

from cognite.client.data_classes.data_modeling import NodeApply, NodeApplyList, NodeOrEdgeData

from cognite_toolkit._cdf_tk.client.data_classes.location_filters import (
    LocationFilterWriteList,
)

from .constants import LOCATION_CONFIG_VIEW_ID, TARGET_SPACE
from .location_config.fields import apply_location_config_fields
from .location_filter.fields import apply_location_filter_fields
from .types_new import InFieldLocationConfigProperties
from .types_old import RootLocationConfiguration
from .utils import (
    get_location_config_external_id,
    get_location_filter_external_id,
)


@dataclass
class InfieldV2MigrationResult:
    """Result of migrating an APMConfig to InField V2 format."""
    
    location_filters: LocationFilterWriteList
    infield_location_config_nodes: NodeApplyList
    data_explorer_config_nodes: NodeApplyList
    
    def all_nodes(self) -> NodeApplyList:
        """Return all InFieldLocationConfig nodes (LocationFilters are handled separately)."""
        nodes = NodeApplyList([])
        nodes.extend(self.infield_location_config_nodes)
        nodes.extend(self.data_explorer_config_nodes)
        return nodes
    
    def all_location_filters(self) -> LocationFilterWriteList:
        """Return all LocationFilter resources."""
        return self.location_filters


def create_infield_v2_config(
    root_location_configs: list[RootLocationConfiguration | Any],
    feature_configuration: dict[str, Any] | None = None,
    client: Any | None = None,
) -> InfieldV2MigrationResult:
    """Migrate root location configurations to the new InField V2 format.

    For now, only migrates basic fields: externalId, name, and description.
    Returns structured output with separate lists for different node types.

    Args:
        root_location_configs: List of root location configurations from the old format

    Returns:
        InfieldV2MigrationResult containing separate lists of nodes
    """
    from cognite_toolkit._cdf_tk.client.data_classes.location_filters import LocationFilterWriteList

    location_filter_nodes = NodeApplyList([])
    infield_location_config_nodes = NodeApplyList([])
    data_explorer_config_nodes = NodeApplyList([])

    if not root_location_configs:
        return InfieldV2MigrationResult(
            location_filters=LocationFilterWriteList([]),
            infield_location_config_nodes=infield_location_config_nodes,
            data_explorer_config_nodes=data_explorer_config_nodes,
        )

    # Create location filters (using Location Filters API)
    location_filters = create_location_filters(root_location_configs)

    # Create infield location config nodes (using Data Modeling Instance API)
    infield_location_config_nodes = create_infield_location_config_nodes(
        root_location_configs, feature_configuration=feature_configuration, client=client
    )

    return InfieldV2MigrationResult(
        location_filters=location_filters,
        infield_location_config_nodes=infield_location_config_nodes,
        data_explorer_config_nodes=data_explorer_config_nodes,
    )


def create_location_filters(root_location_configs: list[RootLocationConfiguration | Any]) -> LocationFilterWriteList:
    """Create LocationFilter resources for each root location configuration.

    These will be upserted using the Location Filters API (storage/config/locationfilters).

    Args:
        root_location_configs: List of root location configurations from the old format

    Returns:
        LocationFilterWriteList containing LocationFilter resources
    """
    from cognite_toolkit._cdf_tk.client.data_classes.location_filters import (
        LocationFilterWrite,
        LocationFilterWriteList,
    )

    location_filters = LocationFilterWriteList([])

    for old_location in root_location_configs:
        location_dict = old_location.dump(camel_case=True) if hasattr(old_location, "dump") else {}

        # Generate location filter external ID with prefix
        location_filter_external_id = get_location_filter_external_id(location_dict)

        # Get name from displayName or assetExternalId
        name = location_dict.get("displayName") or location_dict.get("assetExternalId") or location_filter_external_id

        # Create LocationFilterWrite with basic fields
        location_filter = LocationFilterWrite._load(
            {
                "externalId": location_filter_external_id,
                "name": name,
                "description": "InField location, migrated from old location configuration",
            }
        )

        # Apply additional migrated fields (instanceSpaces, etc.)
        location_filter = apply_location_filter_fields(location_filter, location_dict)

        location_filters.append(location_filter)

    return location_filters


def create_infield_location_config_nodes(
    root_location_configs: list[RootLocationConfiguration | Any],
    feature_configuration: dict[str, Any] | None = None,
    client: Any | None = None,
) -> NodeApplyList:
    """Create InFieldLocationConfig nodes for each root location configuration.

    If the old config does not have externalId but only assetExternalId, adds a postfix with the index
    to ensure uniqueness. Each InFieldLocationConfig references its corresponding LocationFilterDTO
    via rootLocationExternalId.

    Args:
        root_location_configs: List of root location configurations from the old format

    Returns:
        NodeApplyList containing InFieldLocationConfig nodes
    """
    nodes = NodeApplyList([])

    for index, old_location in enumerate(root_location_configs):
        location_dict = old_location.dump(camel_case=True) if hasattr(old_location, "dump") else {}

        # Generate location filter external ID (must match the one created in create_location_filters)
        location_filter_external_id = get_location_filter_external_id(location_dict)

        # Generate location config external ID
        location_external_id = get_location_config_external_id(location_dict, index)

        # Create location config with rootLocationExternalId reference
        location_props: InFieldLocationConfigProperties = {
            "rootLocationExternalId": location_filter_external_id,
        }

        # Apply additional migrated fields (featureToggles, etc.)
        additional_props = apply_location_config_fields(
            location_dict, feature_configuration=feature_configuration, client=client
        )
        location_props.update(additional_props)

        nodes.append(
            NodeApply(
                space=TARGET_SPACE,
                external_id=location_external_id,
                sources=[NodeOrEdgeData(source=LOCATION_CONFIG_VIEW_ID, properties=location_props)],
            )
        )

    return nodes

