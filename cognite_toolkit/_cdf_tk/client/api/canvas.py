from collections.abc import Iterable, Sequence
from typing import Any, overload

from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling import (
    EdgeId,
    InstanceSort,
    NodeApplyResultList,
    NodeId,
    NodeList,
    filters,
    query,
)
from cognite.client.data_classes.filters import Filter
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    ANNOTATION_EDGE_TYPE,
    CANVAS_INSTANCE_SPACE,
    CONTAINER_REFERENCE_EDGE_TYPE,
    FDM_CONTAINER_REFERENCE_EDGE_TYPE,
    Canvas,
    CanvasAnnotation,
    CanvasApply,
    CogniteSolutionTag,
    ContainerReference,
    FdmInstanceContainerReference,
    IndustrialCanvas,
    IndustrialCanvasApply,
)
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstancesApplyResultList

from .extended_data_modeling import ExtendedInstancesAPI


class CanvasAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self._instance_api = instance_api
        self.instance_space = CANVAS_INSTANCE_SPACE
        self.industrial = IndustrialCanvasAPI(instance_api)

    def upsert(self, canvas: CanvasApply | Sequence[CanvasApply]) -> NodeApplyResultList:
        return self._instance_api.apply(canvas).nodes

    def delete(self, external_id: str | SequenceNotStr[str]) -> list[str]:
        external_ids = [external_id] if isinstance(external_id, str) else external_id
        result = self._instance_api.delete([NodeId(self.instance_space, item) for item in external_ids])
        return [item.external_id for item in result.nodes]

    @overload
    def retrieve(self, external_id: str | None = None) -> Canvas | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> NodeList[Canvas]: ...

    def retrieve(self, external_id: str | SequenceNotStr[str] | None = None) -> Canvas | NodeList[Canvas] | None:
        if isinstance(external_id, str):
            return self._instance_api.retrieve_nodes(NodeId(self.instance_space, external_id), node_cls=Canvas)
        elif isinstance(external_id, Iterable):
            return self._instance_api.retrieve_nodes(
                [NodeId(self.instance_space, item) for item in external_id], node_cls=Canvas
            )
        else:
            raise TypeError(f"Expected str or SequenceNotStr[str], got {type(external_id)}")

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort] | InstanceSort | None = None,
        filter: Filter | dict[str, Any] | None = None,
    ) -> NodeList[Canvas]:
        return self._instance_api.list(
            instance_type=Canvas, space=self.instance_space, limit=limit, sort=sort, filter=filter
        )


class IndustrialCanvasAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self._instance_api = instance_api

    def create(self, canvas: IndustrialCanvasApply) -> InstancesApplyResultList:
        return self._instance_api.apply_fast(canvas.as_instances())

    def update(self, canvas: IndustrialCanvasApply) -> InstancesApplyResultList:
        raise NotImplementedError()

    def retrieve(self, external_id: str) -> IndustrialCanvas:
        retrieve_query = self._retrieve_query(external_id)
        result = self._instance_api.query(retrieve_query)
        return IndustrialCanvas.load(result)

    def delete(self, canvas: IndustrialCanvasApply) -> list[NodeId | EdgeId]:
        # Solution tags are used by multiple canvases, so we do not include them in the deletion.
        return self._instance_api.delete_fast(canvas.as_instance_ids(include_solution_tags=False))

    @classmethod
    def _retrieve_query(cls, external_id: str) -> query.Query:
        return query.Query(
            with_={
                "canvas": query.NodeResultSetExpression(
                    filter=filters.InstanceReferences([(CANVAS_INSTANCE_SPACE, external_id)]),
                    limit=1,
                ),
                "solutionTags": query.NodeResultSetExpression(
                    from_="canvas",
                    through=Canvas.get_source().as_property_ref("solutionTags"),
                ),
                "annotationEdges": query.EdgeResultSetExpression(
                    from_="canvas",
                    filter=filters.Equals(["edge", "type"], ANNOTATION_EDGE_TYPE.dump()),
                    node_filter=filters.HasData(views=[CanvasAnnotation.get_source()]),
                    direction="outwards",
                ),
                "containerReferenceEdges": query.EdgeResultSetExpression(
                    from_="canvas",
                    filter=filters.Equals(["edge", "type"], CONTAINER_REFERENCE_EDGE_TYPE.dump()),
                    node_filter=filters.HasData(views=[ContainerReference.get_source()]),
                    direction="outwards",
                ),
                "fdmInstanceContainerReferenceEdges": query.EdgeResultSetExpression(
                    from_="canvas",
                    filter=filters.Equals(
                        ["edge", "type"],
                        FDM_CONTAINER_REFERENCE_EDGE_TYPE.dump(),
                    ),
                    node_filter=filters.HasData(views=[FdmInstanceContainerReference.get_source()]),
                    direction="outwards",
                ),
                "annotations": query.NodeResultSetExpression(from_="annotationEdges"),
                "containerReferences": query.NodeResultSetExpression(from_="containerReferenceEdges"),
                "fdmInstanceContainerReferences": query.NodeResultSetExpression(
                    from_="fdmInstanceContainerReferenceEdges"
                ),
            },
            select={
                "canvas": query.Select([query.SourceSelector(Canvas.get_source(), properties=["*"])]),
                "solutionTags": query.Select([query.SourceSelector(CogniteSolutionTag.get_source(), properties=["*"])]),
                "annotations": query.Select([query.SourceSelector(CanvasAnnotation.get_source(), properties=["*"])]),
                "containerReferences": query.Select(
                    [query.SourceSelector(ContainerReference.get_source(), properties=["*"])]
                ),
                "fdmInstanceContainerReferences": query.Select(
                    [query.SourceSelector(FdmInstanceContainerReference.get_source(), properties=["*"])]
                ),
            },
        )
