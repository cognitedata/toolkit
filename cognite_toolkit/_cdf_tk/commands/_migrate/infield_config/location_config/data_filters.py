"""Migration of dataFilters field for InFieldLocationConfig."""

from typing import Any

from ..types_new import RootLocationDataFilters


def migrate_data_filters(location_dict: dict[str, Any]) -> RootLocationDataFilters | None:
    """Migrate dataFilters from old configuration.

    Extracts the dataFilters dictionary from the old location configuration
    and returns it as a RootLocationDataFilters dict. The structure matches
    between old and new format, so it can be returned as-is.

    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))

    Returns:
        RootLocationDataFilters dict, or None if dataFilters is not present
    """
    data_filters = location_dict.get("dataFilters")
    if not data_filters:
        return None

    # Return the data filters as-is (already in the correct format)
    return data_filters

