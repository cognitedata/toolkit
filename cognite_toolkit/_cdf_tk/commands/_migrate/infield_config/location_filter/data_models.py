"""Migration of dataModels field for LocationFilter."""

from cognite.client.data_classes.data_modeling.ids import DataModelId

from ..constants import DEFAULT_LOCATION_FILTER_DATA_MODEL


def migrate_data_models() -> list[DataModelId]:
    """Migrate dataModels field.

    This function returns a hardcoded list containing the default data model
    for LocationFilter. All LocationFilters will have this data model.

    Returns:
        List containing the default LocationFilter data model
    """
    return [DEFAULT_LOCATION_FILTER_DATA_MODEL]

