from collections.abc import Iterable, Sequence
from typing import Any, Literal, overload

from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling import InstanceSort, NodeApplyResultList, NodeId, NodeList
from cognite.client.data_classes.filters import Filter
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.canvas import CANVAS_INSTANCE_SPACE, Canvas, CanvasApply

from .extended_data_modeling import ExtendedInstancesAPI


class CanvasAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self._instance_api = instance_api
        self.instance_space = CANVAS_INSTANCE_SPACE

    def upsert(self, canvas: CanvasApply | Sequence[CanvasApply]) -> NodeApplyResultList:
        return self._instance_api.apply(canvas).nodes

    def delete(self, external_id: str | SequenceNotStr[str]) -> list[str]:
        external_ids = [external_id] if isinstance(external_id, str) else external_id
        result = self._instance_api.delete([NodeId(self.instance_space, item) for item in external_ids])
        return [item.external_id for item in result.nodes]

    @overload
    def retrieve(
        self, external_id: str | None = None, retrieve_connections: Literal["skip", "full"] = "skip"
    ) -> Canvas | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], retrieve_connections: Literal["skip", "full"] = "skip"
    ) -> NodeList[Canvas]: ...

    def retrieve(
        self,
        external_id: str | SequenceNotStr[str] | None = None,
        retrieve_connections: Literal["skip", "full"] = "skip",
    ) -> Canvas | NodeList[Canvas] | None:
        """Retrieve a canvas or a list of canvases by their external IDs.

        If the retrieve connections parameter is set to 'full', it will retrieve all connections associated with the canvases.

        Args:
            external_id (str | SequenceNotStr[str] | None): The external ID of the canvas or a sequence of external IDs.
            retrieve_connections (Literal["skip", "full"]): Whether to skip or retrieve full connections. Defaults to 'skip'.

        Returns:
            Canvas | NodeList[Canvas] | None: A single Canvas object if a single external ID is provided, a NodeList of Canvas objects if multiple external IDs are provided, or None if no external ID is provided.

        """
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
