"""Migration of instanceSpaces field for LocationFilter."""

from typing import Any


def migrate_instance_spaces(location_dict: dict[str, Any]) -> list[str] | None:
    """Migrate instanceSpaces from sourceDataInstanceSpace and appDataInstanceSpace.

    Collects both sourceDataInstanceSpace and appDataInstanceSpace from the old
    configuration and returns them as a list. Empty strings are excluded.

    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))

    Returns:
        List of instance spaces, or None if no valid spaces found
    """
    instance_spaces = []
    if source_space := location_dict.get("sourceDataInstanceSpace"):
        if source_space:  # Check that it's not empty string
            instance_spaces.append(source_space)
    if app_space := location_dict.get("appDataInstanceSpace"):
        if app_space:  # Check that it's not empty string
            instance_spaces.append(app_space)

    return instance_spaces if instance_spaces else None

