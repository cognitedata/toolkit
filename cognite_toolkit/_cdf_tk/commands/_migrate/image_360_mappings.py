from typing import Any

from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    NodeResponse,
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

CUBEMAP_SOURCE_TO_DESTINATION_PROPERTY: dict[str, str] = {
    "cubeMapFront": "front",
    "cubeMapBack": "back",
    "cubeMapLeft": "left",
    "cubeMapRight": "right",
    "cubeMapTop": "top",
    "cubeMapBottom": "bottom",
}


def image360_collection_label(node: NodeResponse) -> str:
    """Return the human-readable name for a legacy Image360Collection node."""
    return str(
        ((node.properties or {}).get(LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW) or {}).get("label") or node.external_id
    )


def create_360_image_data_mappings() -> list[ViewToViewMapping]:
    """ViewToViewMappings for Image360 and Station360 nodes handled by FDMtoCDMMapper.

    Image360Collection → Cognite360ImageCollection is handled by Image360CollectionMapper registered
    via custom_instance_mappings. The Cognite360ImageModel is created separately via POST /3d/models.
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
                **CUBEMAP_SOURCE_TO_DESTINATION_PROPERTY,
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
    collection_filter: dict[str, Any] = {
        "in": {
            "property": LEGACY_IMAGE360_SOURCE_VIEW.as_property_reference("collection360"),
            "values": [collection.dump(include_instance_type=False) for collection in selected_collections],
        }
    }
    return InstanceQuerySelector(
        endpoint="query",
        query=QueryRequest(
            with_={
                "image360": QueryNodeExpression(
                    limit=10_000,
                    nodes=QueryNodeTableExpression(filter=collection_filter),
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
