"""Type definitions for old APM Config format.

This module contains TypedDict definitions for the legacy APM Config format
that is being migrated to InField V2 configuration format.
"""

from typing import Literal, TypedDict


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

