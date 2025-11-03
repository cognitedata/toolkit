"""Migration of featureToggles field for InFieldLocationConfig."""

from typing import Any

from ..types_new import FeatureToggles


def migrate_feature_toggles(location_dict: dict[str, Any]) -> FeatureToggles | None:
    """Migrate featureToggles from old configuration.

    Extracts the featureToggles from the old configuration and returns it
    as a FeatureToggles dict. If featureToggles is not present, returns None.

    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))

    Returns:
        FeatureToggles dict, or None if not present in old config
    """
    feature_toggles = location_dict.get("featureToggles")
    if not feature_toggles:
        return None

    # Return the feature toggles as-is (already in the correct format)
    return feature_toggles

