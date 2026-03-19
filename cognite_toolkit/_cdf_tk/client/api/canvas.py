from collections.abc import Sequence
from typing import Any, ClassVar, Literal

from cognite_toolkit._cdf_tk.client.api.instances import QUERY_ENDPOINT, MultiWrappedInstancesAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.identifiers import InstanceDefinitionId
from cognite_toolkit._cdf_tk.client.resource_classes.canvas import (
    ANNOTATION_EDGE_TYPE_REF,
    CANVAS_ANNOTATION_VIEW_ID,
    CANVAS_EXCLUDE_FROM_PROPERTIES,
    CANVAS_VIEW_ID,
    CONTAINER_REFERENCE_EDGE_TYPE_REF,
    CONTAINER_REFERENCE_VIEW_ID,
    FDM_CONTAINER_REFERENCE_EDGE_TYPE_REF,
    FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID,
    SOLUTION_TAG_VIEW_ID,
    CanvasProperties,
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
    _ANNOTATIONS_REF = "annotations"
    _CONTAINER_REFS_REF = "containerReferences"
    _FDM_REFS_REF = "fdmInstanceContainerReferences"
    # This is to remove the solutionTags from properties when retrieving the Canvas node. This is because in
    # the Toolkit representation of Canvas, we either want to have full solution tag objects or none.
    _CANVAS_NODE_PROPERTIES: ClassVar[list[str]] = [
        field.alias or field_id
        for field_id, field in CanvasProperties.model_fields.items()
        if field_id not in CANVAS_EXCLUDE_FROM_PROPERTIES
    ]

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, query_chunk=1)

    def _retrieve_query(self, items: Sequence[InstanceDefinitionId]) -> QueryRequest:
        with_: dict[str, QueryNodeExpression | QueryEdgeExpression] = {
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
        }
        for ref, view_id, edge_type in [
            (self._ANNOTATIONS_REF, CANVAS_ANNOTATION_VIEW_ID, ANNOTATION_EDGE_TYPE_REF),
            (self._CONTAINER_REFS_REF, CONTAINER_REFERENCE_VIEW_ID, CONTAINER_REFERENCE_EDGE_TYPE_REF),
            (self._FDM_REFS_REF, FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID, FDM_CONTAINER_REFERENCE_EDGE_TYPE_REF),
        ]:
            edge_ref = f"{ref}Edges"
            with_[edge_ref] = QueryEdgeExpression(
                limit=_QUERY_LIMIT,
                edges=QueryEdgeTableExpression(
                    from_=self._CANVAS_REF,
                    direction="outwards",
                    filter={
                        "equals": {
                            "property": ["edge", "type"],
                            "value": edge_type.dump(include_instance_type=False),
                        }
                    },
                    node_filter={"hasData": [view_id.dump(include_type=True)]},
                ),
            )
            with_[ref] = QueryNodeExpression(
                limit=_QUERY_LIMIT,
                nodes=QueryNodeTableExpression(from_=edge_ref),
            )

        return QueryRequest(
            with_=with_,
            select={
                ref: QuerySelect(sources=[QuerySelectSource(source=view_id, properties=properties)])
                for ref, view_id, properties in [
                    (self._CANVAS_REF, CANVAS_VIEW_ID, self._CANVAS_NODE_PROPERTIES),
                    (self._SOLUTION_TAGS_REF, SOLUTION_TAG_VIEW_ID, ["*"]),
                    (self._ANNOTATIONS_REF, CANVAS_ANNOTATION_VIEW_ID, ["*"]),
                    (self._CONTAINER_REFS_REF, CONTAINER_REFERENCE_VIEW_ID, ["*"]),
                    (self._FDM_REFS_REF, FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID, ["*"]),
                ]
            },
        )

    def _validate_query_response(self, query_response: QueryResponseUntyped) -> list[IndustrialCanvasResponse]:
        canvas_items = query_response.items.get(self._CANVAS_REF, [])
        if len(canvas_items) > 1:
            if len(query_response.items) > 1:
                raise RuntimeError(
                    "Bug in Toolkit. When retrieving multiple canvases, the query response should not "
                    "contain any other items than the canvases."
                )
            results: list[IndustrialCanvasResponse] = []
            for item in canvas_items:
                results.append(IndustrialCanvasResponse.model_validate(item))
            return results
        if len(canvas_items) == 0:
            return []
        canvas_item = canvas_items[0]
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
                    self._CANVAS_REF: QuerySelect(
                        sources=[QuerySelectSource(source=CANVAS_VIEW_ID, properties=self._CANVAS_NODE_PROPERTIES)]
                    )
                },
            )
            if cursor is not None:
                query.cursors = {self._CANVAS_REF: cursor}

            request = RequestMessage(
                endpoint_url=self._http_client.config.create_api_url(QUERY_ENDPOINT.path),
                method=QUERY_ENDPOINT.method,
                body_content=query.dump(),
            )
            batch_response = self._http_client.request_single_retries(request).get_success_or_raise(request)

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
            {"hasData": [CANVAS_VIEW_ID.dump(include_type=True)]},
        ]
        if visibility is not None:
            leaf_filters.append(
                {"equals": {"property": CANVAS_VIEW_ID.as_property_reference("visibility"), "value": visibility}}
            )
        return {"and": leaf_filters}
