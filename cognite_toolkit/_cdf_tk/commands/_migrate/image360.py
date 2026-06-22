from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeResponse

_SOURCE_SPACE = "cdf_360_image_schema"

IMAGE360_SOURCE_VIEW = ViewId(space=_SOURCE_SPACE, external_id="Image360", version="v1")
IMAGE360_COLLECTION_SOURCE_VIEW = ViewId(space=_SOURCE_SPACE, external_id="Image360Collection", version="v1")
IMAGE360_STATION_SOURCE_VIEW = ViewId(space=_SOURCE_SPACE, external_id="Station360", version="v1")


def image360_collection_label(node: NodeResponse) -> str:
    """Return the human-readable name for a legacy Image360Collection node."""
    return str(
        ((node.properties or {}).get(IMAGE360_COLLECTION_SOURCE_VIEW) or {}).get("label") or node.external_id
    )
