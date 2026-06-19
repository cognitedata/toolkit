from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeRequest, NodeResponse
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping

_SOURCE_SPACE = "cdf_360_image_schema"
_DESTINATION_SPACE = "cdf_cdm"

IMAGE360_SOURCE_VIEW = ViewId(space=_SOURCE_SPACE, external_id="Image360", version="v1")
IMAGE360_COLLECTION_SOURCE_VIEW = ViewId(space=_SOURCE_SPACE, external_id="Image360Collection", version="v1")
IMAGE360_STATION_SOURCE_VIEW = ViewId(space=_SOURCE_SPACE, external_id="Station360", version="v1")
COGNITE360_IMAGE_VIEW = ViewId(space=_DESTINATION_SPACE, external_id="Cognite360Image", version="v1")

COGNITE360_FACE_PROPERTIES = frozenset({"front", "back", "left", "right", "top", "bottom"})

CUBEMAP_SOURCE_TO_DESTINATION_PROPERTY: dict[str, str] = {
    "cubeMapFront": "front",
    "cubeMapBack": "back",
    "cubeMapLeft": "left",
    "cubeMapRight": "right",
    "cubeMapTop": "top",
    "cubeMapBottom": "bottom",
}


def create_image360_node_mappings() -> list[ViewToViewMapping]:
    """Create ViewToViewMapping objects for migrating legacy 360-image nodes to CDM.

    Covers two view pairs handled by FDMtoCDMMapper directly:
    - Image360 → Cognite360Image
    - Station360 → Cognite360ImageStation

    Image360Collection → Cognite360ImageCollection is handled by Image360CollectionMapper registered
    via custom_instance_mappings. The Cognite360ImageModel is created separately via POST /3d/models.
    """
    return [
        ViewToViewMapping(
            external_id="Station360ToCognite360ImageStationMapping",
            source_view=IMAGE360_STATION_SOURCE_VIEW,
            destination_view=ViewId(space=_DESTINATION_SPACE, external_id="Cognite360ImageStation", version="v1"),
            map_identical_id_properties=False,
            container_mapping={"label": "name"},
        ),
        ViewToViewMapping(
            external_id="Image360ToCognite360ImageMapping",
            source_view=IMAGE360_SOURCE_VIEW,
            destination_view=COGNITE360_IMAGE_VIEW,
            map_identical_id_properties=True,
            container_mapping={
                **CUBEMAP_SOURCE_TO_DESTINATION_PROPERTY,
                "station": "station360",
                "timeTaken": "takenAt",
            },
            # Cognite360Image has no name (no CogniteDescribable), so the legacy
            # 'label' property has no destination to map to and is therefore intentionally dropped.
            ignore_source_properties={"label"},
        ),
    ]


def is_image360_node(node: NodeResponse) -> bool:
    """Return True when *node* carries legacy Image360 view properties."""
    return IMAGE360_SOURCE_VIEW in (node.properties or {})


def cognite360_image_has_all_face_files(mapped_node: NodeRequest) -> bool:
    """Return True when the mapped Cognite360Image has all six cubemap face file relations."""
    for source in mapped_node.sources or []:
        if source.source != COGNITE360_IMAGE_VIEW or source.properties is None:
            continue
        return COGNITE360_FACE_PROPERTIES.issubset(source.properties.keys())
    return False


def missing_cubemap_face_file_external_ids(
    source_node: NodeResponse,
    mapped_node: NodeRequest | None,
) -> list[str]:
    """Return classic file external IDs for cubemap faces that could not be linked on the target image."""
    source_properties = (source_node.properties or {}).get(IMAGE360_SOURCE_VIEW)
    if not isinstance(source_properties, dict):
        return []

    mapped_face_properties: set[str] = set()
    if mapped_node is not None:
        for source in mapped_node.sources or []:
            if source.source == COGNITE360_IMAGE_VIEW and source.properties is not None:
                mapped_face_properties = set(source.properties.keys())

    missing_files: list[str] = []
    for source_property, destination_property in CUBEMAP_SOURCE_TO_DESTINATION_PROPERTY.items():
        if destination_property in mapped_face_properties:
            continue
        file_external_id = source_properties.get(source_property)
        if isinstance(file_external_id, str):
            missing_files.append(file_external_id)
    return missing_files
