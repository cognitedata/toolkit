"""Migration of InFieldLocationConfig fields from old APM Config format.

This module orchestrates the migration of all InFieldLocationConfig fields.
Individual field migrations are handled in separate modules for better organization.
"""

from typing import Any

from .feature_toggles import migrate_feature_toggles


def apply_location_config_fields(location_dict: dict[str, Any]) -> dict[str, Any]:
    """Apply migrated fields to InFieldLocationConfig properties.

    This function applies all migrated fields from the old configuration
    to the InFieldLocationConfig properties. Fields are migrated incrementally
    as features are implemented.

    Currently migrated fields:
    - featureToggles: From featureToggles in old configuration

    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))

    Returns:
        Dictionary of properties to add to InFieldLocationConfig
    """
    props: dict[str, Any] = {}

    # Migrate featureToggles
    feature_toggles = migrate_feature_toggles(location_dict)
    if feature_toggles is not None:
        props["featureToggles"] = feature_toggles

    # TODO: Add more field migrations here as they are implemented
    # - threeDConfiguration
    # - dataFilters
    # - observations
    # etc.

    return props

