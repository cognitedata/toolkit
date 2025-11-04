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


class InFieldLocationConfigProperties(TypedDict, total=False):
    """Properties for InFieldLocationConfig node.
    
    Currently migrated fields:
    - rootLocationExternalId: Reference to the LocationFilterDTO external ID
    - featureToggles: Feature toggles migrated from old configuration
    - rootAsset: Direct relation to the root asset (space and externalId)
    - appInstanceSpace: Application instance space from appDataInstanceSpace
    """
    rootLocationExternalId: str
    featureToggles: FeatureToggles
    rootAsset: DirectRelationReference
    appInstanceSpace: str
    # TODO: Add the following fields:
    # accessManagement: JSON template & checklist admin both list of strings representing CDF group external IDs
    # disciplines: List[Discipline]
    # dataFilters: RootLocationDataFilters
    # dataExplorationConfig: DirectRelation to data exploration config

