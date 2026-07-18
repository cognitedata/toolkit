from typing import Any

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ViewId
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
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
from cognite_toolkit._cdf_tk.constants import SUBSELECTION_LIMIT_QUERY_ENDPOINT
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceQuerySelector, InstanceViewSelector, SelectedView
from cognite_toolkit._cdf_tk.utils.text import sanitize_instance_external_id

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
                **CUBEMAP_SOURCE_TO_DESTINATION_PROPERTY,
                "station": "station360",
                "timeTaken": "takenAt",
            },
            # Cognite360Image has no name (no CogniteDescribable), so the legacy
            # 'label' property has no destination to map to and is therefore intentionally dropped.
            ignore_source_properties={"label"},
        ),
    ]


def load_image360_annotation_node_data(
    client: ToolkitClient,
    collections: tuple[str, ...],
) -> dict[str, tuple[str, NodeId, NodeId]]:
    """Return a mapping from face-file external ID to (face_name, new_image360_node_id, new_collection_node_id).

    Used by both Image360AnnotationMapper (needs face_name + image360 node) and
    Image360AnnotationMigrationIO (needs file ext IDs for annotation filtering and
    collection node for upload grouping).
    """
    instance_filter = InstanceFilter(
        instance_type="node",
        source=LEGACY_IMAGE360_SOURCE_VIEW,
    )
    all_nodes = client.tool.instances.list(filter=instance_filter, limit=None)

    selected_collections = set(collections)
    face_data_by_file_ext_id: dict[str, tuple[str, NodeId, NodeId]] = {}

    for node in all_nodes:
        if not isinstance(node, NodeResponse) or node.properties is None:
            continue
        props = node.properties.get(LEGACY_IMAGE360_SOURCE_VIEW, {})

        collection_ref = props.get("collection360")
        if not isinstance(collection_ref, dict):
            continue
        collection_ext_id = collection_ref.get("externalId")
        if not collection_ext_id or collection_ext_id not in selected_collections:
            continue

        new_image360_node_id = NodeId(
            space=node.space,
            external_id=sanitize_instance_external_id(node.external_id, "_cdm"),
        )
        new_collection_node_id = NodeId(
            space=node.space,
            external_id=sanitize_instance_external_id(str(collection_ext_id), "_cdm"),
        )

        for prop_name, face_name in CUBEMAP_SOURCE_TO_DESTINATION_PROPERTY.items():
            file_ext_id = props.get(prop_name)
            if file_ext_id and isinstance(file_ext_id, str):
                face_data_by_file_ext_id[file_ext_id] = (
                    face_name,
                    new_image360_node_id,
                    new_collection_node_id,
                )

    return face_data_by_file_ext_id


def create_360_image_selectors(
    selected_collections: list[NodeId],
) -> list[InstanceViewSelector | InstanceQuerySelector]:
    """Build selectors for 360-image migration in dependency order.

    Collections are migrated first, then stations referenced by the scoped images, then the images
    themselves. Each selector returns a single view population so instance tracking stays one id
    per downloaded row.
    """
    if not selected_collections:
        raise ValueError("At least one collection must be selected for 360-image migration.")
    image360_filter: dict[str, Any] = {
        "in": {
            "property": LEGACY_IMAGE360_SOURCE_VIEW.as_property_reference("collection360"),
            "values": [collection.dump(include_instance_type=False) for collection in selected_collections],
        }
    }
    return [
        InstanceViewSelector(
            view=SelectedView(
                space=LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW.space,
                external_id=LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW.external_id,
                version=LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW.version,
            ),
            additional_filter={
                "instanceReferences": [
                    {
                        "space": collection.space,
                        "externalId": collection.external_id,
                    }
                    for collection in selected_collections
                ]
            },
            endpoint="query",
        ),
        InstanceQuerySelector(
            endpoint="query",
            query=QueryRequest(
                with_={
                    "image360": QueryNodeExpression(
                        limit=SUBSELECTION_LIMIT_QUERY_ENDPOINT,
                        nodes=QueryNodeTableExpression(filter=image360_filter),
                        sort=[
                            QuerySortSpec(property=["node", "space"]),
                            QuerySortSpec(property=["node", "externalId"]),
                        ],
                    ),
                    "image360station": QueryNodeExpression(
                        limit=SUBSELECTION_LIMIT_QUERY_ENDPOINT,
                        nodes=QueryNodeTableExpression(
                            from_="image360",
                            direction="outwards",
                            through=QueryThrough(source=LEGACY_IMAGE360_SOURCE_VIEW, identifier="station"),
                        ),
                    ),
                },
                select={
                    # 'image360' is only selected so that the query endpoint emits a cursor for it.
                    # Without this, the endpoint does not return a cursor for 'image360' and pagination
                    # silently stops after the first page. Its items are excluded from the downloaded
                    # data via 'include_root=False' below, since they are migrated separately by the
                    # 'Image360' InstanceViewSelector.
                    "image360": QuerySelect(),
                    "image360station": QuerySelect(
                        sources=[QuerySelectSource(source=LEGACY_IMAGE360_STATION_SOURCE_VIEW, properties=["*"])]
                    ),
                },
                root="image360",
            ).model_dump_json(),
            root="image360",
            subselections=("image360station",),
            include_root=False,
        ),
        InstanceViewSelector(
            view=SelectedView(
                space=LEGACY_IMAGE360_SOURCE_VIEW.space,
                external_id=LEGACY_IMAGE360_SOURCE_VIEW.external_id,
                version=LEGACY_IMAGE360_SOURCE_VIEW.version,
            ),
            additional_filter=image360_filter,
            endpoint="query",
        ),
    ]
