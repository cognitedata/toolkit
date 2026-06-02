from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping

_SOURCE_SPACE = "cdf_360_image_schema"
_DESTINATION_SPACE = "cdf_cdm"

IMAGE360_COLLECTION_SOURCE_VIEW = ViewId(space=_SOURCE_SPACE, external_id="Image360Collection", version="v1")


def create_image360_node_mappings() -> list[ViewToViewMapping]:
    """Create ViewToViewMapping objects for migrating legacy 360-image nodes to CDM.

    Covers two view pairs handled by FDMtoCDMMapper directly:
    - Image360 → Cognite360Image
    - Station360 → Cognite360ImageStation

    Image360Collection → Cognite360ImageCollection (plus the derived Cognite360ImageModel) is
    handled by Image360CollectionAndModelMapper registered via custom_instance_mappings.
    """
    return [
        ViewToViewMapping(
            external_id="Station360ToCognite360ImageStationMapping",
            source_view=ViewId(space=_SOURCE_SPACE, external_id="Station360", version="v1"),
            destination_view=ViewId(space=_DESTINATION_SPACE, external_id="Cognite360ImageStation", version="v1"),
            map_identical_id_properties=False,
            container_mapping={"label": "name"},
        ),
        ViewToViewMapping(
            external_id="Image360ToCognite360ImageMapping",
            source_view=ViewId(space=_SOURCE_SPACE, external_id="Image360", version="v1"),
            destination_view=ViewId(space=_DESTINATION_SPACE, external_id="Cognite360Image", version="v1"),
            map_identical_id_properties=True,
            container_mapping={
                "cubeMapFront": "front",
                "cubeMapBack": "back",
                "cubeMapLeft": "left",
                "cubeMapRight": "right",
                "cubeMapTop": "top",
                "cubeMapBottom": "bottom",
                "station": "station360",
                "timeTaken": "takenAt",
            },
            # Cognite360Image has no name (no CogniteDescribable), so the legacy
            # 'label' property has no destination to map to and is therefore intentionally dropped.
            ignore_source_properties={"label"},
        ),
    ]
