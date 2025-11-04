"""Migration of disciplines field for InFieldLocationConfig."""

from typing import Any

from ..types_new import Discipline


def migrate_disciplines(feature_configuration: dict[str, Any] | None) -> list[Discipline] | None:
    """Migrate disciplines from old FeatureConfiguration.

    Extracts the disciplines list from the FeatureConfiguration level
    (disciplines are shared across all locations in a config).

    Args:
        feature_configuration: FeatureConfiguration dict from old APM Config

    Returns:
        List of Discipline dicts, or None if disciplines is not present or empty
    """
    if not feature_configuration:
        return None

    disciplines = feature_configuration.get("disciplines")
    if not disciplines:
        return None

    # Return disciplines as-is (already in the correct format)
    return disciplines

