"""Migration of appInstanceSpace field for InFieldLocationConfig."""

from typing import Any


def migrate_app_instance_space(location_dict: dict[str, Any]) -> str | None:
    """Migrate appInstanceSpace from appDataInstanceSpace.

    Extracts the appDataInstanceSpace from the old configuration and returns it
    as appInstanceSpace. If appDataInstanceSpace is not present or is an empty string,
    returns None.

    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))

    Returns:
        App instance space string, or None if not present or empty
    """
    app_instance_space = location_dict.get("appDataInstanceSpace")
    if not app_instance_space:  # None or empty string
        return None

    return app_instance_space

