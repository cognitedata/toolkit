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
    CreatedSourceSystem,
    InstanceSource,
    ResourceViewMapping,
    ResourceViewMappingApply,
    SpaceSource,
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


class CreatedSourceSystemAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self._instance_api = instance_api
        self._RETRIEVE_LIMIT = 1000
        self._view_id = CreatedSourceSystem.get_source()

    def retrieve(self, source: SequenceNotStr[str]) -> NodeList[CreatedSourceSystem]:
        """Retrieve one or more view sources by their external IDs."""
        results: NodeList[CreatedSourceSystem] = NodeList[CreatedSourceSystem]([])
        # MyPy does not understand that SequenceNotStr is a sequence.
        for chunk in chunker_sequence(source, self._RETRIEVE_LIMIT):  # type: ignore[type-var]
            retrieve_query = query.Query(
                with_={
                    "sourceSystem": query.NodeResultSetExpression(
                        filter=filters.And(filters.HasData(views=[self._view_id]), self._create_dms_filter(chunk)),
                        limit=len(chunk),
                    ),
                },
                select={"sourceSystem": query.Select([query.SourceSelector(self._view_id, ["*"])])},
            )
            chunk_response = self._instance_api.query(retrieve_query)
            results.extend([CreatedSourceSystem._load(item.dump()) for item in chunk_response.get("sourceSystem", [])])
        return results

    def _create_dms_filter(self, source: SequenceNotStr[str]) -> filters.Filter:
        """Create a filter that matches all CreatedSourceSystem with given source in the list."""
        if not source:
            raise ValueError("Cannot create a filter from an empty source list.")
        return filters.In(self._view_id.as_property_ref("source"), list(source))

    def list(self, limit: int = -1) -> NodeList[CreatedSourceSystem]:
        """Lists all created source systems."""
        return self._instance_api.list(instance_type=CreatedSourceSystem, limit=limit)


class SpaceSourceAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self._instance_api = instance_api
        self._RETRIEVE_LIMIT = 1000
        self._view_id = SpaceSource.get_source()
        # Cache for space sources by data set ID and external ID
        self._cache_by_id: dict[int, SpaceSource] = {}
        self._cache_by_external_id: dict[str, SpaceSource] = {}

    @overload
    def retrieve(self, data_set_id: int) -> SpaceSource | None: ...

    @overload
    def retrieve(self, data_set_id: Sequence[int]) -> NodeList[SpaceSource]: ...

    @overload
    def retrieve(self, *, data_set_external_id: str) -> SpaceSource | None: ...

    @overload
    def retrieve(self, *, data_set_external_id: SequenceNotStr[str]) -> NodeList[SpaceSource]: ...

    def retrieve(
        self,
        data_set_id: int | Sequence[int] | None = None,
        data_set_external_id: str | SequenceNotStr[str] | None = None,
    ) -> SpaceSource | NodeList[SpaceSource] | None:
        """Retrieve a space source by data set ID or external ID.

        This method uses caching to avoid redundant API calls. If a space source
        has been retrieved before, it will be returned from cache.
        """
        if data_set_id is not None:
            is_single = isinstance(data_set_id, int)
            ids = [data_set_id] if is_single else list(data_set_id)

            # Check cache for all requested IDs
            cached_results: list[SpaceSource] = []
            missing_ids: list[int] = []
            for id_ in ids:
                if id_ in self._cache_by_id:
                    cached_results.append(self._cache_by_id[id_])
                else:
                    missing_ids.append(id_)

            # Fetch missing IDs from API
            if missing_ids:
                fetched = self._retrieve_by_property(
                    property_name="dataSetId",
                    value=missing_ids[0] if len(missing_ids) == 1 else missing_ids,
                    is_single=len(missing_ids) == 1,
                )
                if fetched is not None:
                    if isinstance(fetched, SpaceSource):
                        self._add_to_cache(fetched)
                        cached_results.append(fetched)
                    else:
                        for item in fetched:
                            self._add_to_cache(item)
                            cached_results.append(item)

            if is_single:
                return cached_results[0] if cached_results else None
            return NodeList[SpaceSource](cached_results)

        elif data_set_external_id is not None:
            is_single = isinstance(data_set_external_id, str)
            ext_ids = [data_set_external_id] if is_single else list(data_set_external_id)

            # Check cache for all requested external IDs
            cached_results: list[SpaceSource] = []
            missing_ext_ids: list[str] = []
            for ext_id in ext_ids:
                if ext_id in self._cache_by_external_id:
                    cached_results.append(self._cache_by_external_id[ext_id])
                else:
                    missing_ext_ids.append(ext_id)

            # Fetch missing external IDs from API
            if missing_ext_ids:
                fetched = self._retrieve_by_property(
                    property_name="classicExternalId",
                    value=missing_ext_ids[0] if len(missing_ext_ids) == 1 else missing_ext_ids,
                    is_single=len(missing_ext_ids) == 1,
                )
                if fetched is not None:
                    if isinstance(fetched, SpaceSource):
                        self._add_to_cache(fetched)
                        cached_results.append(fetched)
                    else:
                        for item in fetched:
                            self._add_to_cache(item)
                            cached_results.append(item)

            if is_single:
                return cached_results[0] if cached_results else None
            return NodeList[SpaceSource](cached_results)
        else:
            raise ValueError("One of data_set_id or data_set_external_id must be provided.")

    def _add_to_cache(self, space_source: SpaceSource) -> None:
        """Add a space source to both caches."""
        if space_source.data_set_id is not None:
            self._cache_by_id[space_source.data_set_id] = space_source
        if space_source.classic_external_id is not None:
            self._cache_by_external_id[space_source.classic_external_id] = space_source

    def _retrieve_by_property(
        self,
        property_name: str,
        value: int | str | Sequence[int] | SequenceNotStr[str],
        is_single: bool,
    ) -> SpaceSource | NodeList[SpaceSource] | None:
        """Retrieve space sources by filtering on a specific property."""
        values = [value] if is_single else list(value)  # type: ignore[arg-type]
        results: NodeList[SpaceSource] = NodeList[SpaceSource]([])
        for chunk in chunker_sequence(values, self._RETRIEVE_LIMIT):
            retrieve_query = query.Query(
                with_={
                    "spaceSource": query.NodeResultSetExpression(
                        filter=filters.And(
                            filters.HasData(views=[self._view_id]),
                            filters.In(self._view_id.as_property_ref(property_name), chunk),  # type: ignore[arg-type]
                        ),
                        limit=len(chunk),
                    ),
                },
                select={"spaceSource": query.Select([query.SourceSelector(self._view_id, ["*"])])},
            )
            chunk_response = self._instance_api.query(retrieve_query)
            items = chunk_response.get("spaceSource", [])
            results.extend([SpaceSource._load(item.dump()) for item in items])

        if is_single:
            return results[0] if results else None
        return results

    def list(self, limit: int = -1) -> NodeList[SpaceSource]:
        """Lists all space sources and populates the cache.

        Note: This method always fetches from the API and does not use the cache,
        but it will populate the cache with all retrieved space sources.
        """
        results = self._instance_api.list(instance_type=SpaceSource, limit=limit)
        # Populate cache with all retrieved space sources
        for space_source in results:
            self._add_to_cache(space_source)
        return results


class MigrationAPI:
    def __init__(self, instance_api: ExtendedInstancesAPI) -> None:
        self.instance_source = InstanceSourceAPI(instance_api)
        self.resource_view_mapping = ResourceViewMappingAPI(instance_api)
        self.created_source_system = CreatedSourceSystemAPI(instance_api)
        self.space_source = SpaceSourceAPI(instance_api)
