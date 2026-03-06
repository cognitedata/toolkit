from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.api.instances import MultiWrappedInstancesAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import InstanceDefinitionId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.canvas import (
    ANNOTATION_EDGE_TYPE_REF,
    CANVAS_ANNOTATION_VIEW_ID,
    CANVAS_VIEW_ID,
    CONTAINER_REFERENCE_EDGE_TYPE_REF,
    CONTAINER_REFERENCE_VIEW_ID,
    FDM_CONTAINER_REFERENCE_EDGE_TYPE_REF,
    FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID,
    SOLUTION_TAG_VIEW_ID,
    CanvasAnnotationItem,
    CogniteSolutionTagItem,
    ContainerReferenceItem,
    FdmInstanceContainerReferenceItem,
    IndustrialCanvasRequest,
    IndustrialCanvasResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._query import (
    QueryEdgeExpression,
    QueryEdgeTableExpression,
    QueryNodeExpression,
    QueryNodeTableExpression,
    QueryRequest,
    QueryResponseUntyped,
    QuerySelect,
    QuerySelectSource,
    QueryThrough,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._wrapped import move_properties

_QUERY_LIMIT = 1000


class IndustrialCanvasConfigAPI(MultiWrappedInstancesAPI[IndustrialCanvasRequest, IndustrialCanvasResponse]):
    _CANVAS_REF = "canvas"
    _SOLUTION_TAGS_REF = "solutionTags"
    _ANNOTATION_EDGES_REF = "annotationEdges"
    _CONTAINER_REF_EDGES_REF = "containerReferenceEdges"
    _FDM_REF_EDGES_REF = "fdmInstanceContainerReferenceEdges"
    _ANNOTATIONS_REF = "annotations"
    _CONTAINER_REFS_REF = "containerReferences"
    _FDM_REFS_REF = "fdmInstanceContainerReferences"

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, query_chunk=1)

    def _retrieve_query(self, items: Sequence[InstanceDefinitionId]) -> QueryRequest:
        canvas_vid = ViewId(
            space=CANVAS_VIEW_ID.space, external_id=CANVAS_VIEW_ID.external_id, version=CANVAS_VIEW_ID.version
        )
        annotation_vid = ViewId(
            space=CANVAS_ANNOTATION_VIEW_ID.space,
            external_id=CANVAS_ANNOTATION_VIEW_ID.external_id,
            version=CANVAS_ANNOTATION_VIEW_ID.version,
        )
        solution_tag_vid = ViewId(
            space=SOLUTION_TAG_VIEW_ID.space,
            external_id=SOLUTION_TAG_VIEW_ID.external_id,
            version=SOLUTION_TAG_VIEW_ID.version,
        )
        container_ref_vid = ViewId(
            space=CONTAINER_REFERENCE_VIEW_ID.space,
            external_id=CONTAINER_REFERENCE_VIEW_ID.external_id,
            version=CONTAINER_REFERENCE_VIEW_ID.version,
        )
        fdm_ref_vid = ViewId(
            space=FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID.space,
            external_id=FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID.external_id,
            version=FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID.version,
        )

        return QueryRequest(
            with_={
                self._CANVAS_REF: QueryNodeExpression(
                    limit=len(items),
                    nodes=QueryNodeTableExpression(
                        filter={"instanceReferences": [item.dump(include_instance_type=False) for item in items]},
                    ),
                ),
                self._SOLUTION_TAGS_REF: QueryNodeExpression(
                    limit=_QUERY_LIMIT,
                    nodes=QueryNodeTableExpression(
                        from_=self._CANVAS_REF,
                        through=QueryThrough(source=canvas_vid, identifier="solutionTags"),
                    ),
                ),
                self._ANNOTATION_EDGES_REF: QueryEdgeExpression(
                    limit=_QUERY_LIMIT,
                    edges=QueryEdgeTableExpression(
                        from_=self._CANVAS_REF,
                        direction="outwards",
                        filter={
                            "equals": {
                                "property": ["edge", "type"],
                                "value": ANNOTATION_EDGE_TYPE_REF,  # type: ignore[dict-item]
                            }
                        },
                        node_filter={"hasData": [annotation_vid.dump()]},
                    ),
                ),
                self._CONTAINER_REF_EDGES_REF: QueryEdgeExpression(
                    limit=_QUERY_LIMIT,
                    edges=QueryEdgeTableExpression(
                        from_=self._CANVAS_REF,
                        direction="outwards",
                        filter={
                            "equals": {
                                "property": ["edge", "type"],
                                "value": CONTAINER_REFERENCE_EDGE_TYPE_REF,  # type: ignore[dict-item]
                            }
                        },
                        node_filter={"hasData": [container_ref_vid.dump()]},
                    ),
                ),
                self._FDM_REF_EDGES_REF: QueryEdgeExpression(
                    limit=_QUERY_LIMIT,
                    edges=QueryEdgeTableExpression(
                        from_=self._CANVAS_REF,
                        direction="outwards",
                        filter={
                            "equals": {
                                "property": ["edge", "type"],
                                "value": FDM_CONTAINER_REFERENCE_EDGE_TYPE_REF,  # type: ignore[dict-item]
                            }
                        },
                        node_filter={"hasData": [fdm_ref_vid.dump()]},
                    ),
                ),
                self._ANNOTATIONS_REF: QueryNodeExpression(
                    limit=_QUERY_LIMIT,
                    nodes=QueryNodeTableExpression(from_=self._ANNOTATION_EDGES_REF),
                ),
                self._CONTAINER_REFS_REF: QueryNodeExpression(
                    limit=_QUERY_LIMIT,
                    nodes=QueryNodeTableExpression(from_=self._CONTAINER_REF_EDGES_REF),
                ),
                self._FDM_REFS_REF: QueryNodeExpression(
                    limit=_QUERY_LIMIT,
                    nodes=QueryNodeTableExpression(from_=self._FDM_REF_EDGES_REF),
                ),
            },
            select={
                self._CANVAS_REF: QuerySelect(
                    sources=[QuerySelectSource(source=canvas_vid, properties=["*"])],
                ),
                self._SOLUTION_TAGS_REF: QuerySelect(
                    sources=[QuerySelectSource(source=solution_tag_vid, properties=["*"])],
                ),
                self._ANNOTATIONS_REF: QuerySelect(
                    sources=[QuerySelectSource(source=annotation_vid, properties=["*"])],
                ),
                self._CONTAINER_REFS_REF: QuerySelect(
                    sources=[QuerySelectSource(source=container_ref_vid, properties=["*"])],
                ),
                self._FDM_REFS_REF: QuerySelect(
                    sources=[QuerySelectSource(source=fdm_ref_vid, properties=["*"])],
                ),
            },
        )

    def _validate_query_response(self, query_response: QueryResponseUntyped) -> list[IndustrialCanvasResponse]:
        annotations = [
            CanvasAnnotationItem.model_validate(move_properties(item, CANVAS_ANNOTATION_VIEW_ID))
            for item in query_response.items.get(self._ANNOTATIONS_REF, [])
        ]
        container_references = [
            ContainerReferenceItem.model_validate(move_properties(item, CONTAINER_REFERENCE_VIEW_ID))
            for item in query_response.items.get(self._CONTAINER_REFS_REF, [])
        ]
        fdm_refs = [
            FdmInstanceContainerReferenceItem.model_validate(
                move_properties(item, FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID)
            )
            for item in query_response.items.get(self._FDM_REFS_REF, [])
        ]
        solution_tags = [
            CogniteSolutionTagItem.model_validate(move_properties(item, SOLUTION_TAG_VIEW_ID))
            for item in query_response.items.get(self._SOLUTION_TAGS_REF, [])
        ]

        results: list[IndustrialCanvasResponse] = []
        for canvas_item in query_response.items.get(self._CANVAS_REF, []):
            canvas = IndustrialCanvasResponse.model_validate(canvas_item)
            canvas.annotations = annotations or None
            canvas.container_references = container_references or None
            canvas.fdm_instance_container_references = fdm_refs or None
            canvas.solution_tag_items = solution_tags or None
            results.append(canvas)
        return results
