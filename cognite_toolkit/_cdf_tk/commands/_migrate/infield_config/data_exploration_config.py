"""Migration of DataExplorationConfig for InField V2 config migration.

This module handles the creation of a single DataExplorationConfig node per APM Config,
which is shared across all InFieldLocationConfig nodes via a direct relation.
"""

from typing import Any

from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData

from .constants import DATA_EXPLORATION_CONFIG_VIEW_ID, TARGET_SPACE
from .types_new import DataExplorationConfigProperties


def create_data_exploration_config_node(
    feature_configuration: dict[str, Any] | None,
    config_external_id: str,
) -> NodeApply | None:
    """Create a DataExplorationConfig node from FeatureConfiguration.

    Only one DataExplorationConfig is created per APM Config, shared across all locations.

    Args:
        feature_configuration: FeatureConfiguration dict from old APM Config
        config_external_id: External ID of the APM Config (used to generate DataExplorationConfig external ID)

    Returns:
        NodeApply for DataExplorationConfig, or None if feature_configuration is missing or incomplete
    """
    if not feature_configuration:
        return None

    # Extract properties from FeatureConfiguration
    props: DataExplorationConfigProperties = {}

    # Migrate observations
    if observations := feature_configuration.get("observations"):
        props["observations"] = observations

    # Migrate activities
    if activities := feature_configuration.get("activities"):
        props["activities"] = activities

    # Migrate documents
    if documents := feature_configuration.get("documents"):
        # Remove metadata. prefix from type and description if present
        migrated_documents = documents.copy()
        if "type" in migrated_documents and isinstance(migrated_documents["type"], str):
            migrated_documents["type"] = migrated_documents["type"].removeprefix("metadata.")
        if "description" in migrated_documents and isinstance(migrated_documents["description"], str):
            migrated_documents["description"] = migrated_documents["description"].removeprefix("metadata.")
        props["documents"] = migrated_documents

    # Migrate notifications
    if notifications := feature_configuration.get("notifications"):
        props["notifications"] = notifications

    # Migrate assets (from assetPageConfiguration)
    if assets := feature_configuration.get("assetPageConfiguration"):
        props["assets"] = assets

    # Only create node if at least one property is present
    if not props:
        return None

    # Generate external ID for DataExplorationConfig
    data_exploration_external_id = f"data_exploration_{config_external_id}"

    return NodeApply(
        space=TARGET_SPACE,
        external_id=data_exploration_external_id,
        sources=[NodeOrEdgeData(source=DATA_EXPLORATION_CONFIG_VIEW_ID, properties=props)],
    )


def get_data_exploration_config_external_id(config_external_id: str) -> str:
    """Generate external ID for DataExplorationConfig node.

    Args:
        config_external_id: External ID of the APM Config

    Returns:
        External ID for the DataExplorationConfig node
    """
    return f"data_exploration_{config_external_id}"

