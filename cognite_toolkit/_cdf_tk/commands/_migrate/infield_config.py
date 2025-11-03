import uuid
from dataclasses import dataclass
from typing import Any, Literal, TypedDict

from cognite.client.data_classes.data_modeling import NodeApply, NodeApplyList, NodeOrEdgeData, ViewId

from cognite_toolkit._cdf_tk.client.data_classes.location_filters import (
    LocationFilterWrite,
    LocationFilterWriteList,
)

# Type definitions for old APM Config (camelCase matching dump() output)
class ViewReference(TypedDict, total=False):
    """Reference to a view."""
    externalId: str
    space: str
    type: Literal["view"]
    version: str


NamedView = Literal["activity", "asset", "operation", "notification"]


class ViewMappings(TypedDict, total=False):
    """View mappings for different named views."""
    activity: ViewReference | None
    asset: ViewReference | None
    operation: ViewReference | None
    notification: ViewReference | None


class ThreeDModelIdentifier(TypedDict, total=False):
    """3D model identifier."""
    revisionId: int
    modelId: int
    name: str


class ThreeDConfiguration(TypedDict, total=False):
    """3D configuration."""
    fullWeightModels: list[ThreeDModelIdentifier]
    lightWeightModels: list[ThreeDModelIdentifier]


class ResourceFilters(TypedDict, total=False):
    """Resource filters."""
    datasetIds: list[int] | None
    assetSubtreeExternalIds: list[str] | None
    rootAssetExternalIds: list[str] | None
    externalIdPrefix: str | None
    spaces: list[str] | None


class RootLocationDataFilters(TypedDict, total=False):
    """Data filters for root location."""
    general: ResourceFilters | None
    assets: ResourceFilters | None
    files: ResourceFilters | None
    timeseries: ResourceFilters | None


class ObservationFeatureToggles(TypedDict, total=False):
    """Feature toggles for observations."""
    isEnabled: bool
    isWriteBackEnabled: bool
    notificationsEndpointExternalId: str
    attachmentsEndpointExternalId: str


class RootLocationFeatureToggles(TypedDict, total=False):
    """Feature toggles for root location."""
    threeD: bool
    trends: bool
    documents: bool
    workorders: bool
    notifications: bool
    media: bool
    templateChecklistFlow: bool
    workorderChecklistFlow: bool
    observations: ObservationFeatureToggles


class ObservationConfigFieldProperty(TypedDict, total=False):
    """Observation config field property."""
    displayTitle: str
    displayDescription: str
    isRequired: bool


class ObservationConfigDropdownPropertyOption(TypedDict, total=False):
    """Option for dropdown property."""
    id: str
    value: str
    label: str


class ObservationConfigDropdownProperty(ObservationConfigFieldProperty, total=False):
    """Observation config dropdown property."""
    options: list[ObservationConfigDropdownPropertyOption]


class ObservationsConfig(TypedDict, total=False):
    """Observations configuration."""
    files: ObservationConfigFieldProperty
    description: ObservationConfigFieldProperty
    asset: ObservationConfigFieldProperty
    troubleshooting: ObservationConfigFieldProperty
    type: ObservationConfigDropdownProperty
    priority: ObservationConfigDropdownProperty


class RootLocationConfiguration(TypedDict, total=False):
    """Root location configuration from old APM Config."""
    externalId: str
    assetExternalId: str
    displayName: str
    threeDConfiguration: ThreeDConfiguration
    dataSetId: int
    templateAdmins: list[str]  # list of CDF group names
    checklistAdmins: list[str]  # list of CDF group names
    appDataInstanceSpace: str
    sourceDataInstanceSpace: str
    dataFilters: RootLocationDataFilters
    featureToggles: RootLocationFeatureToggles
    observations: ObservationsConfig


class Discipline(TypedDict, total=False):
    """Discipline definition."""
    externalId: str
    name: str


class PropertyConfiguration(TypedDict, total=False):
    """Property configuration."""
    highlightedProperties: list[str]
    linkableAssetKeys: list[str]


class AssetPageConfiguration(TypedDict, total=False):
    """Asset page configuration."""
    propertyCard: PropertyConfiguration


class DocumentConfiguration(TypedDict, total=False):
    """Document configuration."""
    title: str
    description: str
    type: str


class ActivityOverviewCardFilter(TypedDict, total=False):
    """Activity overview card filter."""
    externalId: str
    value: str


class ActivitiesConfiguration(TypedDict, total=False):
    """Activities configuration."""
    overviewCard: dict[str, list[ActivityOverviewCardFilter]]


class NotificationOverviewCardFilter(TypedDict, total=False):
    """Notification overview card filter."""
    externalId: str
    value: str


class NotificationsConfiguration(TypedDict, total=False):
    """Notifications configuration."""
    overviewCard: dict[str, list[NotificationOverviewCardFilter]]


class ObservationsConfigFeature(TypedDict, total=False):
    """Observations feature config."""
    enabled: bool
    sapWriteBack: dict[str, bool]
    optionalMediaField: dict[str, bool]


class CopilotConfig(TypedDict, total=False):
    """Copilot configuration."""
    enabled: bool


class FeatureConfiguration(TypedDict, total=False):
    """Feature configuration from old APM Config."""
    # Shared
    rootLocationConfigurations: list[RootLocationConfiguration]
    
    # InField
    viewMappings: ViewMappings
    assetPageConfiguration: AssetPageConfiguration
    documents: DocumentConfiguration
    activities: ActivitiesConfiguration
    notifications: NotificationsConfiguration
    disciplines: list[Discipline]
    copilot: CopilotConfig
    observations: ObservationsConfigFeature


class AppConfig(TypedDict, total=False):
    """Main APM Config (old format)."""
    name: str
    externalId: str
    appDataSpaceId: str
    appDataSpaceVersion: str
    customerDataSpaceId: str
    customerDataSpaceVersion: str
    viewMappings: ViewMappings
    featureConfiguration: FeatureConfiguration


# Type definitions for InField V2 configuration nodes
class LocationFilterDTOProperties(TypedDict, total=False):
    """Properties for LocationFilterDTO node.
    
    Currently migrated fields:
    - name: The name of the location filter
    - description: Description indicating this was migrated from old location
    """
    externalId: str
    name: str
    description: str


class InFieldLocationConfigProperties(TypedDict, total=False):
    """Properties for InFieldLocationConfig node.
    
    Currently migrated fields:
    - rootLocationExternalId: Reference to the LocationFilterDTO external ID
    """
    rootLocationExternalId: str


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


# View IDs for the new format
LOCATION_CONFIG_VIEW_ID = ViewId("infield_cdm_source_desc_sche_asset_file_ts", "InFieldLocationConfig", "v1")
TARGET_SPACE = "APM_Config"


# Utility functions for external ID generation
def _get_original_external_id(location_dict: dict[str, Any]) -> str:
    """Extract the original external ID from a location configuration dict.
    
    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))
        
    Returns:
        Original external ID (externalId, assetExternalId, or generated UUID)
    """
    return (
        location_dict.get("externalId")
        or location_dict.get("assetExternalId")
        or str(uuid.uuid4())
    )


def _get_location_filter_external_id(location_dict: dict[str, Any]) -> str:
    """Generate the LocationFilterDTO external ID with prefix.
    
    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))
        
    Returns:
        Location filter external ID with "location_filter_" prefix
    """
    original_external_id = _get_original_external_id(location_dict)
    return f"location_filter_{original_external_id}"


def _get_location_config_external_id(location_dict: dict[str, Any], index: int) -> str:
    """Generate the InFieldLocationConfig external ID.
    
    If externalId exists, use it directly. If only assetExternalId exists,
    add index postfix to ensure uniqueness. Otherwise, generate a UUID.
    
    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))
        index: Index of the location in the list (for uniqueness when only assetExternalId exists)
        
    Returns:
        Location config external ID
    """
    if location_dict.get("externalId"):
        return location_dict["externalId"]
    elif location_dict.get("assetExternalId"):
        # Add index postfix to ensure uniqueness when only assetExternalId is available
        return f"{location_dict['assetExternalId']}_{index}"
    else:
        return f"infield_location_{str(uuid.uuid4())}"


def create_infield_v2_config(root_location_configs: list[RootLocationConfiguration | Any]) -> InfieldV2MigrationResult:
    """Migrate root location configurations to the new InField V2 format.

    For now, only migrates basic fields: externalId, name, and description.
    Returns structured output with separate lists for different node types.

    Args:
        root_location_configs: List of root location configurations from the old format

    Returns:
        InfieldV2MigrationResult containing separate lists of nodes
    """
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
    infield_location_config_nodes = create_infield_location_config_nodes(root_location_configs)

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
    location_filters = LocationFilterWriteList([])

    for old_location in root_location_configs:
        location_dict = old_location.dump(camel_case=True) if hasattr(old_location, "dump") else {}

        # Generate location filter external ID with prefix
        location_filter_external_id = _get_location_filter_external_id(location_dict)

        # Get name from displayName or assetExternalId
        name = location_dict.get("displayName") or location_dict.get("assetExternalId") or location_filter_external_id

        # Create LocationFilterWrite with minimal fields for now
        location_filter = LocationFilterWrite._load(
            {
                "externalId": location_filter_external_id,
                "name": name,
                "description": "InField location, migrated from old location configuration",
            }
        )
        location_filters.append(location_filter)

    return location_filters


def create_infield_location_config_nodes(root_location_configs: list[RootLocationConfiguration | Any]) -> NodeApplyList:
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
        location_filter_external_id = _get_location_filter_external_id(location_dict)

        # Generate location config external ID
        location_external_id = _get_location_config_external_id(location_dict, index)

        # Create location config with rootLocationExternalId reference
        location_props: InFieldLocationConfigProperties = {
            "rootLocationExternalId": location_filter_external_id,
        }

        nodes.append(
            NodeApply(
                space=TARGET_SPACE,
                external_id=location_external_id,
                sources=[NodeOrEdgeData(source=LOCATION_CONFIG_VIEW_ID, properties=location_props)],
            )
        )

    return nodes

