"""Type definitions for new InField V2 configuration format.

This module contains TypedDict definitions for the new InField V2 configuration
format that is the target of the migration from the old APM Config format.
"""

from typing import TypedDict

from cognite.client.data_classes.data_modeling.ids import DataModelId


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


class InFieldLocationConfigProperties(TypedDict, total=False):
    """Properties for InFieldLocationConfig node.
    
    Currently migrated fields:
    - rootLocationExternalId: Reference to the LocationFilterDTO external ID
    """
    rootLocationExternalId: str

