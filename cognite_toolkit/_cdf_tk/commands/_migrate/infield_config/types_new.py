"""Type definitions for new InField V2 configuration format.

This module contains TypedDict definitions for the new InField V2 configuration
format that is the target of the migration from the old APM Config format.
"""

from typing import TypedDict

from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.client.data_classes.data_modeling import DirectRelationReference


class LocationFilterDTOProperties(TypedDict, total=False):
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


class ObservationFeatureToggles(TypedDict, total=False):
    """Feature toggles for observations."""
    isEnabled: bool
    isWriteBackEnabled: bool
    notificationsEndpointExternalId: str
    attachmentsEndpointExternalId: str


class FeatureToggles(TypedDict, total=False):
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


class AccessManagement(TypedDict, total=False):
    """Access management configuration."""
    templateAdmins: list[str]  # list of CDF group external IDs
    checklistAdmins: list[str]  # list of CDF group external IDs


class Discipline(TypedDict, total=False):
    """Discipline definition."""
    externalId: str
    name: str


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


class InFieldLocationConfigProperties(TypedDict, total=False):
    """Properties for InFieldLocationConfig node.
    
    Currently migrated fields:
    - rootLocationExternalId: Reference to the LocationFilterDTO external ID
    - featureToggles: Feature toggles migrated from old configuration
    - rootAsset: Direct relation to the root asset (space and externalId)
    - appInstanceSpace: Application instance space from appDataInstanceSpace
    - accessManagement: Template and checklist admin groups (from templateAdmins and checklistAdmins)
    - disciplines: List of disciplines (from disciplines in FeatureConfiguration)
    - dataFilters: Data filters for general, assets, files, and timeseries (from dataFilters in old configuration)
    """
    rootLocationExternalId: str
    featureToggles: FeatureToggles
    rootAsset: DirectRelationReference
    appInstanceSpace: str
    accessManagement: AccessManagement
    disciplines: list[Discipline]
    dataFilters: RootLocationDataFilters
    # TODO: Add the following fields:
    # dataExplorationConfig: DirectRelation to data exploration config

