import warnings
from collections.abc import Sequence
from itertools import groupby
from typing import overload

from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeApplyResult,
    NodeApplyResultList,
    NodeId,
    NodeList,
    filters,
    query,
)
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.migration import (
    AssetCentricId,
    InstanceSource,
    ResourceViewMapping,
    ResourceViewMappingApply,
)
from cognite_toolkit._cdf_tk.constants import COGNITE_MIGRATION_SPACE
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
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


class ResourceViewMappingAPI:
    """API for retrieving resource view mapping from the data model."""

    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self._instance_api = instance_api
        self._view_id = ResourceViewMapping.get_source()

    @overload
    def upsert(
        self,
        item: ResourceViewMappingApply,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> NodeApplyResult: ...

    @overload
    def upsert(
        self,
        item: Sequence[ResourceViewMappingApply],
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> NodeApplyResultList: ...

    def upsert(
        self,
        item: ResourceViewMappingApply | Sequence[ResourceViewMappingApply],
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> NodeApplyResult | NodeApplyResultList:
        """Upsert one or more view sources."""
        result = self._instance_api.apply(
            item, skip_on_version_conflict=skip_on_version_conflict, replace=replace
        ).nodes
        if isinstance(item, ResourceViewMappingApply):
            return result[0]
        return NodeApplyResultList(result)

    @overload
    def retrieve(self, external_id: str) -> ResourceViewMapping | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> NodeList[ResourceViewMapping]: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str]
    ) -> ResourceViewMapping | NodeList[ResourceViewMapping] | None:
        """Retrieve one or more view sources by their external IDs."""
        if isinstance(external_id, str):
            return self._instance_api.retrieve_nodes(
                NodeId(COGNITE_MIGRATION_SPACE, external_id), node_cls=ResourceViewMapping
            )
        else:
            nodes = self._instance_api.retrieve(
                nodes=[NodeId(COGNITE_MIGRATION_SPACE, ext_id) for ext_id in external_id], sources=[self._view_id]
            ).nodes
            return self._safe_convert(nodes)

    @overload
    def delete(self, external_id: str) -> NodeId: ...

    @overload
    def delete(self, external_id: SequenceNotStr[str]) -> list[NodeId]: ...

    def delete(self, external_id: str | SequenceNotStr[str]) -> NodeId | list[NodeId]:
        """Delete a view source or a list of view sources by their external IDs."""
        if isinstance(external_id, str):
            return self._instance_api.delete(NodeId(COGNITE_MIGRATION_SPACE, external_id)).nodes[0]
        else:
            return self._instance_api.delete([NodeId(COGNITE_MIGRATION_SPACE, ext_id) for ext_id in external_id]).nodes

    def list(
        self, resource_type: str | None = None, limit: int | None = DEFAULT_LIMIT_READ
    ) -> NodeList[ResourceViewMapping]:
        """List view sources optionally filtered by resource type"""
        is_selected: filters.Filter | None = None
        if resource_type:
            is_selected = filters.Equals(self._view_id.as_property_ref("resourceType"), resource_type)

        nodes = self._instance_api.list(
            instance_type="node", filter=is_selected, limit=limit, space=COGNITE_MIGRATION_SPACE, sources=self._view_id
        )
        return self._safe_convert(nodes)

    @classmethod
    def _safe_convert(cls, nodes: NodeList[Node]) -> NodeList[ResourceViewMapping]:
        results = NodeList[ResourceViewMapping]([])
        for node in nodes:
            try:
                loaded = ResourceViewMapping._load(node.dump())
            except ValueError as e:
                warnings.warn(
                    HighSeverityWarning(
                        f"Node {node.as_id()!r} is in an invalid format. Skipping it. Error: {e!s}",
                    )
                )
                continue
            results.append(loaded)
        return results


class MigrationAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self.instance_source = InstanceSourceAPI(instance_api)
        self.resource_view_mapping = ResourceViewMappingAPI(instance_api)
