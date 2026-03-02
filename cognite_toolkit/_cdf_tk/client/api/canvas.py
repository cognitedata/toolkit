from collections.abc import Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.api.instances import MultiWrappedInstancesAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
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
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._query import QueryResponseUntyped
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    TypedInstanceIdentifier,
    move_properties,
)

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

    def _retrieve_query(self, items: Sequence[TypedInstanceIdentifier]) -> dict[str, Any]:
        return {
            "with": {
                self._CANVAS_REF: {
                    "limit": len(items),
                    "nodes": {
                        "filter": {
                            "instanceReferences": [
                                {"space": item.space, "externalId": item.external_id} for item in items
                            ]
                        },
                    },
                },
                self._SOLUTION_TAGS_REF: {
                    "limit": _QUERY_LIMIT,
                    "nodes": {
                        "from": self._CANVAS_REF,
                        "through": {
                            "source": CANVAS_VIEW_ID.dump(),
                            "identifier": "solutionTags",
                        },
                    },
                },
                self._ANNOTATION_EDGES_REF: {
                    "limit": _QUERY_LIMIT,
                    "edges": {
                        "from": self._CANVAS_REF,
                        "direction": "outwards",
                        "filter": {
                            "equals": {
                                "property": ["edge", "type"],
                                "value": ANNOTATION_EDGE_TYPE_REF,
                            }
                        },
                        "nodeFilter": {
                            "hasData": [CANVAS_ANNOTATION_VIEW_ID.dump()],
                        },
                    },
                },
                self._CONTAINER_REF_EDGES_REF: {
                    "limit": _QUERY_LIMIT,
                    "edges": {
                        "from": self._CANVAS_REF,
                        "direction": "outwards",
                        "filter": {
                            "equals": {
                                "property": ["edge", "type"],
                                "value": CONTAINER_REFERENCE_EDGE_TYPE_REF,
                            }
                        },
                        "nodeFilter": {
                            "hasData": [CONTAINER_REFERENCE_VIEW_ID.dump()],
                        },
                    },
                },
                self._FDM_REF_EDGES_REF: {
                    "limit": _QUERY_LIMIT,
                    "edges": {
                        "from": self._CANVAS_REF,
                        "direction": "outwards",
                        "filter": {
                            "equals": {
                                "property": ["edge", "type"],
                                "value": FDM_CONTAINER_REFERENCE_EDGE_TYPE_REF,
                            }
                        },
                        "nodeFilter": {
                            "hasData": [FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID.dump()],
                        },
                    },
                },
                self._ANNOTATIONS_REF: {
                    "limit": _QUERY_LIMIT,
                    "nodes": {"from": self._ANNOTATION_EDGES_REF},
                },
                self._CONTAINER_REFS_REF: {
                    "limit": _QUERY_LIMIT,
                    "nodes": {"from": self._CONTAINER_REF_EDGES_REF},
                },
                self._FDM_REFS_REF: {
                    "limit": _QUERY_LIMIT,
                    "nodes": {"from": self._FDM_REF_EDGES_REF},
                },
            },
            "select": {
                self._CANVAS_REF: {
                    "sources": [{"source": CANVAS_VIEW_ID.dump(), "properties": ["*"]}],
                },
                self._SOLUTION_TAGS_REF: {
                    "sources": [{"source": SOLUTION_TAG_VIEW_ID.dump(), "properties": ["*"]}],
                },
                self._ANNOTATIONS_REF: {
                    "sources": [{"source": CANVAS_ANNOTATION_VIEW_ID.dump(), "properties": ["*"]}],
                },
                self._CONTAINER_REFS_REF: {
                    "sources": [{"source": CONTAINER_REFERENCE_VIEW_ID.dump(), "properties": ["*"]}],
                },
                self._FDM_REFS_REF: {
                    "sources": [{"source": FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID.dump(), "properties": ["*"]}],
                },
            },
        }

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
