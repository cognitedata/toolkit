from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    QueryNodeExpression,
    QueryNodeTableExpression,
    QueryRequest,
    QuerySelect,
    QuerySelectSource,
    QuerySortSpec,
    QueryThrough,
)
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceQuerySelector

LEGACY_360_IMAGE_SCHEMA_SPACE = "cdf_360_image_schema"

LEGACY_IMAGE360_SOURCE_VIEW = ViewId(space=LEGACY_360_IMAGE_SCHEMA_SPACE, external_id="Image360", version="v1")
LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW = ViewId(
    space=LEGACY_360_IMAGE_SCHEMA_SPACE, external_id="Image360Collection", version="v1"
)
LEGACY_IMAGE360_STATION_SOURCE_VIEW = ViewId(
    space=LEGACY_360_IMAGE_SCHEMA_SPACE, external_id="Station360", version="v1"
)

COGNITE_360_IMAGE_VIEW = ViewId(space="cdf_cdm", external_id="Cognite360Image", version="v1")
COGNITE_360_IMAGE_STATION_VIEW = ViewId(space="cdf_cdm", external_id="Cognite360ImageStation", version="v1")
COGNITE_3D_REVISION_VIEW = ViewId(space="cdf_cdm", external_id="Cognite3DRevision", version="v1")

CUBEMAP_SOURCE_TO_DESTINATION_PROPERTY: dict[str, str] = {
    "cubeMapFront": "front",
    "cubeMapBack": "back",
    "cubeMapLeft": "left",
    "cubeMapRight": "right",
    "cubeMapTop": "top",
    "cubeMapBottom": "bottom",
}


def create_360_image_data_mappings() -> list[ViewToViewMapping]:
    """ViewToViewMappings for Image360 and Station360 nodes handled by FDMtoCDMMapper.

    Image360Collection → Cognite360ImageCollection is handled by Image360CollectionMapper registered
    via custom_instance_mappings, which also creates the Image360 3D model linked to the collection.
    """
    return [
        ViewToViewMapping(
            external_id="Station360ToCognite360ImageStationMapping",
            source_view=LEGACY_IMAGE360_STATION_SOURCE_VIEW,
            destination_view=COGNITE_360_IMAGE_STATION_VIEW,
            map_identical_id_properties=False,
            container_mapping={"label": "name"},
        ),
        ViewToViewMapping(
            external_id="Image360ToCognite360ImageMapping",
            source_view=LEGACY_IMAGE360_SOURCE_VIEW,
            destination_view=COGNITE_360_IMAGE_VIEW,
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


def create_360_image_selector(selected_collections: list[NodeId]) -> InstanceQuerySelector:
    """Build a query selector that fetches Image360 nodes and related collections and stations."""
    return InstanceQuerySelector(
        endpoint="query",
        query=QueryRequest(
            with_={
                "image360": QueryNodeExpression(
                    limit=10_000,
                    nodes=QueryNodeTableExpression(
                        filter={
                            "in": {
                                "property": LEGACY_IMAGE360_SOURCE_VIEW.as_property_reference("collection360"),
                                "values": [
                                    collection.dump(include_instance_type=False) for collection in selected_collections
                                ],
                            }
                        }
                    ),
                    sort=[
                        QuerySortSpec(property=["node", "space"]),
                        QuerySortSpec(property=["node", "externalId"]),
                    ],
                ),
                "image360collection": QueryNodeExpression(
                    limit=10_000,
                    nodes=QueryNodeTableExpression(
                        from_="image360",
                        direction="outwards",
                        through=QueryThrough(source=LEGACY_IMAGE360_SOURCE_VIEW, identifier="collection360"),
                    ),
                ),
                "image360station": QueryNodeExpression(
                    limit=10_000,
                    nodes=QueryNodeTableExpression(
                        from_="image360",
                        direction="outwards",
                        through=QueryThrough(source=LEGACY_IMAGE360_SOURCE_VIEW, identifier="station"),
                    ),
                ),
            },
            select={
                "image360": QuerySelect(
                    sources=[QuerySelectSource(source=LEGACY_IMAGE360_SOURCE_VIEW, properties=["*"])]
                ),
                "image360collection": QuerySelect(
                    sources=[QuerySelectSource(source=LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW, properties=["*"])]
                ),
                "image360station": QuerySelect(
                    sources=[QuerySelectSource(source=LEGACY_IMAGE360_STATION_SOURCE_VIEW, properties=["*"])]
                ),
            },
            root="image360",
        ).model_dump_json(),
        root="image360",
        subselections=tuple(["image360collection", "image360station"]),
    )
