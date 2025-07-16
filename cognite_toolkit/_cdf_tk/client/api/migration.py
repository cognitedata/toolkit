from collections.abc import Sequence
from itertools import groupby

from cognite.client.data_classes.data_modeling import NodeApplyResultList, NodeList, ViewId, filters, query
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.migration import (
    AssetCentricId,
    InstanceSource,
    ViewSource,
    ViewSourceApply,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

from .extended_data_modeling import ExtendedInstancesAPI


class InstanceSourceAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self._instance_api = instance_api
        self._RETRIEVE_LIMIT = 1000
        self._view_id = InstanceSource.get_source()

    def retrieve(self, ids: Sequence[AssetCentricId]) -> NodeList[InstanceSource]:
        """Retrieve a list of instance sources by their IDs.

        Args:
            ids (Sequence[AssetCentricId]): A sequence of AssetCentricId objects representing the IDs of the instance sources to retrieve.
        """
        results: NodeList[InstanceSource] = NodeList[InstanceSource]([])
        for chunk in chunker_sequence(ids, self._RETRIEVE_LIMIT):
            retrieve_query = query.Query(
                with_={
                    "instanceSource": query.NodeResultSetExpression(
                        filter=filters.And(filters.HasData(views=[self._view_id]), self._create_dms_filter(ids)),
                        limit=len(chunk),
                    ),
                },
                select={"instanceSource": query.Select([query.SourceSelector(self._view_id, ["*"])])},
            )
            chunk_response = self._instance_api.query(retrieve_query)
            results.extend([InstanceSource._load(item.dump()) for item in chunk_response.get("instanceSource", [])])
        return results

    @staticmethod
    def _create_dms_filter(ids: Sequence[AssetCentricId]) -> filters.Filter:
        """Create a filter that matches all the AssetCentricIds in the list."""
        if not ids:
            raise ValueError("Cannot create a filter from an empty AssetCentricIdList.")
        to_or_filters: list[filters.Filter] = []
        instance_source_view = InstanceSource.get_source()
        for resource_type, resource_ids in groupby(
            sorted(ids, key=lambda x: x.resource_type), key=lambda x: x.resource_type
        ):
            is_resource = filters.Equals(instance_source_view.as_property_ref("resourceType"), resource_type)
            is_id = filters.In(
                instance_source_view.as_property_ref("id"), [resource_id.id_ for resource_id in resource_ids]
            )
            to_or_filters.append(filters.And(is_resource, is_id))
        return filters.Or(*to_or_filters)


class ViewSourceAPI:
    """API for retrieving instance sources from the data model."""

    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self._instance_api = instance_api

    def create(self, item: ViewSourceApply | Sequence[ViewSourceApply]) -> NodeApplyResultList:
        """Create one or more view sources."""
        raise NotImplementedError()

    def retrieve(self, external_id: str | SequenceNotStr[str]) -> ViewSource | NodeList[ViewSource]:
        """Retrieve one or more view sources by their external IDs."""
        raise NotImplementedError()

    def update(self, item: ViewSourceApply | Sequence[ViewSourceApply]) -> ViewSource | NodeList[ViewSource]:
        """Update a view source or a list of view sources."""
        raise NotImplementedError()

    def delete(self, external_id: str | SequenceNotStr[str]) -> int:
        """Delete a view source or a list of view sources by their external IDs."""
        raise NotImplementedError()

    def list(
        self, resource_type: str | None = None, view: ViewId | Sequence[ViewId] | None = None, limit: int | None = 25
    ) -> NodeList[ViewSource]:
        """List view sources, optionally filtered by resource type and view."""
        raise NotImplementedError()


class MigrationAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self.instance_source = InstanceSourceAPI(instance_api)
