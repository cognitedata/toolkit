from collections.abc import Sequence
from typing import Any, Literal

from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling import InstanceSort, NodeApplyResultList, NodeId, NodeList
from cognite.client.data_classes.filters import Filter
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    CANVAS_INSTANCE_SPACE,
    Canvas,
    CanvasApply,
)

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

    def retrieve(
        self, external_id: str | None = None, retrieve_connections: Literal["skip", "full"] = "skip"
    ) -> Canvas | None:
        """Retrieve a canvas with a given external ID.

        If the retrieve connections parameter is set to 'full', it will retrieve all connections associated with the Canvas will
        be returned.

        Args:
            external_id (str | SequenceNotStr[str] | None): The external ID of the canvas or a sequence of external IDs.
            retrieve_connections (Literal["skip", "full"]): Whether to skip or retrieve full connections. Defaults to 'skip'.

        Returns:
            Canvas | NodeList[Canvas] | None: A single Canvas object if a single external ID is provided.

        """
        if retrieve_connections == "skip":
            return self._instance_api.retrieve_nodes(NodeId(self.instance_space, external_id), node_cls=Canvas)
        else:
            canvas_query = self._canvas_with_connections_query(external_id)
            result = self._instance_api.query(canvas_query)
            return Canvas._load_query(result)

    def retrieve_multiple(self, external_ids: SequenceNotStr[str]) -> NodeList[Canvas]:
        """Retrieve a list of canvases by their external IDs.


        Args:
            external_ids (SequenceNotStr[str]): The external IDs of the Canvases to retrieve.

        Returns:
            NodeList[Canvas]: A list of Canvas objects corresponding to the provided external IDs.

        """
        return self._instance_api.retrieve_nodes(
            [NodeId(self.instance_space, external_id) for external_id in external_ids], node_cls=Canvas
        )

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort] | InstanceSort | None = None,
        filter: Filter | dict[str, Any] | None = None,
    ) -> NodeList[Canvas]:
        return self._instance_api.list(
            instance_type=Canvas, space=self.instance_space, limit=limit, sort=sort, filter=filter
        )
