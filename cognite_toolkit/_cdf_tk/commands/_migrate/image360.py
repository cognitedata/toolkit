from importlib import import_module

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeResponse

_360_image_mappings = import_module(".360_image_mappings", package="cognite_toolkit._cdf_tk.commands._migrate")
COGNITE_360_IMAGE_STATION_VIEW = _360_image_mappings.COGNITE_360_IMAGE_STATION_VIEW
COGNITE_360_IMAGE_VIEW = _360_image_mappings.COGNITE_360_IMAGE_VIEW
CUBEMAP_SOURCE_TO_DESTINATION_PROPERTY = _360_image_mappings.CUBEMAP_SOURCE_TO_DESTINATION_PROPERTY
LEGACY_360_IMAGE_SCHEMA_SPACE = _360_image_mappings.LEGACY_360_IMAGE_SCHEMA_SPACE
LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW = _360_image_mappings.LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW
LEGACY_IMAGE360_SOURCE_VIEW = _360_image_mappings.LEGACY_IMAGE360_SOURCE_VIEW
LEGACY_IMAGE360_STATION_SOURCE_VIEW = _360_image_mappings.LEGACY_IMAGE360_STATION_SOURCE_VIEW
create_360_image_data_mappings = _360_image_mappings.create_360_image_data_mappings
create_360_image_selector = _360_image_mappings.create_360_image_selector


def image360_collection_label(node: NodeResponse) -> str:
    """Return the human-readable name for a legacy Image360Collection node."""
    return str(
        ((node.properties or {}).get(LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW) or {}).get("label") or node.external_id
    )
