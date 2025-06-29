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
from cognite.client.exceptions import CogniteDuplicatedError
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
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError

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
        self._APPLY_LIMIT = 1000  # Limit for applying instances in a single request

    def create(self, canvas: IndustrialCanvasApply) -> InstancesApplyResultList:
        instance_ids = canvas.as_instance_ids()
        existing = self._instance_api.retrieve(
            [node for node in instance_ids if isinstance(node, NodeId)],
            edges=[edge for edge in instance_ids if isinstance(edge, EdgeId)],
        )
        if existing.nodes or existing.edges:
            raise CogniteDuplicatedError(duplicated=existing.nodes.as_ids() + existing.edges.as_ids())
        instances = canvas.as_instances()
        self._validate_instance_count(len(instances))
        return self._instance_api.apply_fast(instances)

    def retrieve(self, external_id: str) -> IndustrialCanvas | None:
        retrieve_query = self._retrieve_query(external_id)
        result = self._instance_api.query(retrieve_query)
        if len(result["canvas"]) == 0:
            return None
        return IndustrialCanvas.load(result)

    def update(self, canvas: IndustrialCanvasApply) -> InstancesApplyResultList:
        new_instance_ids = canvas.as_instance_ids(include_solution_tags=False)
        self._validate_instance_count(len(new_instance_ids))

        existing = self.retrieve(external_id=canvas.as_id())
        if existing is None:
            raise ToolkitValueError(
                f"Cannot update canvas. Industrial canvas with external ID '{canvas.as_id()}' does not exist."
            )
        existing_instance_ids = existing.as_write().as_instance_ids(include_solution_tags=False)
        to_delete = set(existing_instance_ids) - set(new_instance_ids)
        result = self._instance_api.apply_fast(canvas.as_instances())
        if to_delete:
            # Delete components that are not in the new canvas
            self._instance_api.delete_fast(list(to_delete))
        return result

    def delete(self, canvas: IndustrialCanvasApply) -> list[NodeId | EdgeId]:
        # Solution tags are used by multiple canvases, so we do not include them in the deletion.
        return self._instance_api.delete_fast(canvas.as_instance_ids(include_solution_tags=False))

    def _validate_instance_count(self, instance_count: int) -> None:
        if instance_count > self._APPLY_LIMIT:
            # Looking at the largest Canvas users I have not seen any larger than 500 components (node + edge)
            # Creating a canvas with more than 1000 instances is more complex as it is not an atomic operation
            # thus postponing that implementation for now.
            raise ToolkitValueError(
                f"Creating/Updating an industrial canvas with more than {self._APPLY_LIMIT // 2} components is not supported. "
                "Please contact support if you need to create a larger canvas."
            )

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
