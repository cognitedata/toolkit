"""Migration of LocationFilter fields from old APM Config format.

This module orchestrates the migration of all LocationFilter fields.
Individual field migrations are handled in separate modules for better organization.
"""

from typing import Any

from cognite_toolkit._cdf_tk.client.data_classes.location_filters import LocationFilterWrite

from .instance_spaces import migrate_instance_spaces


def apply_location_filter_fields(
    location_filter: LocationFilterWrite, location_dict: dict[str, Any]
) -> LocationFilterWrite:
    """Apply migrated fields to a LocationFilter.

    This function applies all migrated fields from the old configuration
    to the LocationFilter. Fields are migrated incrementally as features
    are implemented.

    Currently migrated fields:
    - instanceSpaces: From sourceDataInstanceSpace and appDataInstanceSpace

    Args:
        location_filter: The LocationFilter to update
        location_dict: Location configuration dict (from dump(camel_case=True))

    Returns:
        Updated LocationFilter with migrated fields applied
    """
    # Migrate instanceSpaces
    instance_spaces = migrate_instance_spaces(location_dict)
    if instance_spaces is not None:
        location_filter.instance_spaces = instance_spaces

    # TODO: Add more field migrations here as they are implemented
    # - dataModels
    # - views
    # - assetCentric
    # - scene
    # etc.

    return location_filter

