from collections.abc import Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.api.instances import QUERY_ENDPOINT, MultiWrappedInstancesAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.identifiers import InstanceDefinitionId
from cognite_toolkit._cdf_tk.client.resource_classes.canvas import (
    ANNOTATION_EDGE_TYPE_REF,
    CANVAS_ANNOTATION_VIEW_ID,
    CANVAS_VIEW_ID,
    CONTAINER_REFERENCE_EDGE_TYPE_REF,
    CONTAINER_REFERENCE_VIEW_ID,
    FDM_CONTAINER_REFERENCE_EDGE_TYPE_REF,
    FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID,
    SOLUTION_TAG_VIEW_ID,
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

_QUERY_LIMIT = QUERY_ENDPOINT.item_limit


class IndustrialCanvasAPI(MultiWrappedInstancesAPI[IndustrialCanvasRequest, IndustrialCanvasResponse]):
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
                        through=QueryThrough(source=CANVAS_VIEW_ID, identifier="solutionTags"),
                        direction="outwards",
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
                                "value": ANNOTATION_EDGE_TYPE_REF.dump(include_instance_type=False),
                            }
                        },
                        node_filter={"hasData": [CANVAS_ANNOTATION_VIEW_ID.dump(include_type=True)]},
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
                                "value": CONTAINER_REFERENCE_EDGE_TYPE_REF.dump(include_instance_type=False),
                            }
                        },
                        node_filter={"hasData": [CONTAINER_REFERENCE_VIEW_ID.dump(include_type=True)]},
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
                                "value": FDM_CONTAINER_REFERENCE_EDGE_TYPE_REF.dump(include_instance_type=False),
                            }
                        },
                        node_filter={"hasData": [FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID.dump(include_type=True)]},
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
                    sources=[QuerySelectSource(source=CANVAS_VIEW_ID, properties=["*"])],
                ),
                self._SOLUTION_TAGS_REF: QuerySelect(
                    sources=[QuerySelectSource(source=SOLUTION_TAG_VIEW_ID, properties=["*"])],
                ),
                self._ANNOTATIONS_REF: QuerySelect(
                    sources=[QuerySelectSource(source=CANVAS_ANNOTATION_VIEW_ID, properties=["*"])],
                ),
                self._CONTAINER_REFS_REF: QuerySelect(
                    sources=[QuerySelectSource(source=CONTAINER_REFERENCE_VIEW_ID, properties=["*"])],
                ),
                self._FDM_REFS_REF: QuerySelect(
                    sources=[QuerySelectSource(source=FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID, properties=["*"])],
                ),
            },
        )

    def _validate_query_response(self, query_response: QueryResponseUntyped) -> list[IndustrialCanvasResponse]:
        canvas_items = query_response.items.get(self._CANVAS_REF, [])
        if len(canvas_items) > 1:
            raise ValueError(f"Expected at most one canvas item, but got {len(canvas_items)}")
        if len(canvas_items) == 0:
            return []
        canvas_item = canvas_items[0]

        # We remove the solution tag references (NodeIds) from the canvas item since they are returned as separate items
        # in the query response.
        canvas_item.pop("solutionTags", None)

        for key in [self._SOLUTION_TAGS_REF, self._ANNOTATIONS_REF, self._CONTAINER_REFS_REF, self._FDM_REFS_REF]:
            if subitems := query_response.items.get(key):
                canvas_item[key] = subitems  # type: ignore[assignment]

        canvas = IndustrialCanvasResponse.model_validate(canvas_item)
        return [canvas]

    def list(
        self, visibility: Literal["public", "private"] | None = None, limit: int | None = 100
    ) -> list[IndustrialCanvasResponse]:
        """List canvases with the given visibility.

        Note this do not retrieve the full Canvas objects, i.e., not solutions tags, annotations, container references, or fdm instance container references.
        To retrieve the full objects, use the `retrieve` method with the canvas external ids.
        """
        filter_ = self._create_filter(visibility)
        result: list[IndustrialCanvasResponse] = []
        cursor: str | None = None
        while True:
            batch_limit = min(limit - len(result), _QUERY_LIMIT) if limit is not None else _QUERY_LIMIT
            query = QueryRequest(
                with_={
                    self._CANVAS_REF: QueryNodeExpression(
                        limit=batch_limit, nodes=QueryNodeTableExpression(filter=filter_)
                    )
                },
                select={
                    self._CANVAS_REF: QuerySelect(sources=[QuerySelectSource(source=CANVAS_VIEW_ID, properties=["*"])])
                },
            )
            if cursor is not None:
                query.cursors = {self._CANVAS_REF: cursor}

            batch_response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=self._http_client.config.create_api_url(QUERY_ENDPOINT.path),
                    method=QUERY_ENDPOINT.method,
                    body_content=query.dump(),
                )
            ).get_success_or_raise()

            query_response = QueryResponseUntyped.model_validate_json(batch_response.body)
            batch_items = self._validate_query_response(query_response)
            result.extend(batch_items)
            cursor = query_response.next_cursor.get(self._CANVAS_REF)
            if not batch_items or cursor is None or (limit is not None and len(result) >= limit):
                break
        return result

    def _create_filter(self, visibility: Literal["public", "private"] | None = None) -> dict[str, Any]:
        leaf_filters: list[dict[str, Any]] = [
            {"not": {"equals": {"property": CANVAS_VIEW_ID.as_property_reference("isArchived"), "value": True}}},
            # When sourceCanvasId is not set, we get the newest version of the canvas and not
            # previous versions of the canvas
            {"not": {"exists": {"property": CANVAS_VIEW_ID.as_property_reference("sourceCanvasId")}}},
        ]
        if visibility is not None:
            leaf_filters.append(
                {"equals": {"property": CANVAS_VIEW_ID.as_property_reference("visibility"), "value": visibility}}
            )
        return {"and": leaf_filters}
