from typing import Any

from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import DataModelId

from .base import BaseModelResource, ToolkitResource


class LocationFilterDTOProperties(BaseModelResource):
    """Properties for LocationFilterDTO node.

    Currently migrated fields:
    - name: The name of the location filter
    - description: Description indicating this was migrated from old location
    - instanceSpaces: List of instance spaces from sourceDataInstanceSpace and appDataInstanceSpace
    - dataModels: List of DataModelId references to data models
    """

    externalId: str
    name: str
    description: str
    instanceSpaces: list[str]
    dataModels: list[DataModelId]


class ObservationFeatureToggles(BaseModelResource):
    """Feature toggles for observations."""

    isEnabled: bool
    isWriteBackEnabled: bool
    notificationsEndpointExternalId: str
    attachmentsEndpointExternalId: str


class FeatureToggles(BaseModelResource):
    """Feature toggles for InField location configuration."""

    threeD: bool
    trends: bool
    documents: bool
    workorders: bool
    notifications: bool
    media: bool
    templateChecklistFlow: bool
    workorderChecklistFlow: bool
    observations: ObservationFeatureToggles


class AccessManagement(BaseModelResource):
    """Access management configuration."""

    templateAdmins: list[str]  # list of CDF group external IDs
    checklistAdmins: list[str]  # list of CDF group external IDs


class Discipline(BaseModelResource):
    """Discipline definition."""

    externalId: str
    name: str


class ResourceFilters(BaseModelResource):
    """Resource filters."""

    datasetIds: list[int] | None
    assetSubtreeExternalIds: list[str] | None
    rootAssetExternalIds: list[str] | None
    externalIdPrefix: str | None
    spaces: list[str] | None


class RootLocationDataFilters(BaseModelResource):
    """Data filters for root location."""

    general: ResourceFilters | None
    assets: ResourceFilters | None
    files: ResourceFilters | None
    timeseries: ResourceFilters | None


class DataExplorationConfigProperties(BaseModelResource):
    """Properties for DataExplorationConfig node.

    Contains configuration for data exploration features:
    - observations: Observations feature configuration
    - activities: Activities configuration
    - documents: Document configuration
    - notifications: Notifications configuration
    - assets: Asset page configuration
    """

    observations: dict[str, Any]  # ObservationsConfigFeature
    activities: dict[str, Any]  # ActivitiesConfiguration
    documents: dict[str, Any]  # DocumentConfiguration
    notifications: dict[str, Any]  # NotificationsConfiguration
    assets: dict[str, Any]  # AssetPageConfiguration


class InfieldCDMV1YAML(ToolkitResource):
    """Properties for InFieldLocationConfig node.

    Currently migrated fields:
    - rootLocationExternalId: Reference to the LocationFilterDTO external ID
    - featureToggles: Feature toggles migrated from old configuration
    - rootAsset: Direct relation to the root asset (space and externalId)
    - appInstanceSpace: Application instance space from appDataInstanceSpace
    - accessManagement: Template and checklist admin groups (from templateAdmins and checklistAdmins)
    - disciplines: List of disciplines (from disciplines in FeatureConfiguration)
    - dataFilters: Data filters for general, assets, files, and timeseries (from dataFilters in old configuration)
    - dataExplorationConfig: Direct relation to the DataExplorationConfig node (shared across all locations)
    """

    rootLocationExternalId: str
    featureToggles: FeatureToggles
    rootAsset: DirectRelationReference
    appInstanceSpace: str
    accessManagement: AccessManagement
    disciplines: list[Discipline]
    dataFilters: RootLocationDataFilters
    dataExplorationConfig: DirectRelationReference
