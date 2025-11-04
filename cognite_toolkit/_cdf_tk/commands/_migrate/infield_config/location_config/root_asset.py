"""Migration of rootAsset field for InFieldLocationConfig."""

from typing import Any

from cognite.client.data_classes.data_modeling import DirectRelationReference


def migrate_root_asset(location_dict: dict[str, Any]) -> DirectRelationReference | None:
    """Migrate rootAsset from sourceDataInstanceSpace and assetExternalId.

    Creates a DirectRelationReference using:
    - space: sourceDataInstanceSpace from old config
    - externalId: assetExternalId from old config

    Args:
        location_dict: Location configuration dict (from dump(camel_case=True))

    Returns:
        DirectRelationReference, or None if required fields are missing
    """
    source_space = location_dict.get("sourceDataInstanceSpace")
    asset_external_id = location_dict.get("assetExternalId")

    # Both fields are required to create a DirectRelationReference
    if not source_space or not asset_external_id:
        return None

    return DirectRelationReference(space=source_space, external_id=asset_external_id)

