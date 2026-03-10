from collections.abc import Sequence
from itertools import groupby
from typing import Any, Literal, TypeVar, cast, overload

from cognite.client.utils.useful_types import SequenceNotStr
from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.api.instances import InstancesAPI, WrappedInstancesAPI
from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import AssetCentricExternalId, NodeId
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._query import (
    QueryNodeExpression,
    QueryNodeTableExpression,
    QueryRequest,
    QuerySelect,
    QuerySelectSource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.migration import (
    CREATED_SOURCE_SYSTEM_VIEW_ID,
    INSTANCE_SOURCE_VIEW_ID,
    SPACE_SOURCE_VIEW_ID,
    AssetCentricId,
    CreatedSourceSystem,
    InstanceSource,
    SpaceSource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import (
    RESOURCE_VIEW_MAPPING_SPACE,
    ResourceViewMappingRequest,
    ResourceViewMappingResponse,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricType


def _has_data_filter(view_id: ViewId) -> dict[str, Any]:
    return {"hasData": [view_id.dump(include_type=True)]}


def _equals_filter(view_id: ViewId, property_name: str, value: Any) -> dict[str, Any]:
    return {
        "equals": {
            "property": view_id.as_property_reference(property_name),
            "value": value,
        }
    }


def _in_filter(view_id: ViewId, property_name: str, values: list[Any]) -> dict[str, Any]:
    return {
        "in": {
            "property": view_id.as_property_reference(property_name),
            "values": values,
        }
    }


def _and_filter(*filters: dict[str, Any]) -> dict[str, Any]:
    return {"and": list(filters)}


def _or_filter(*filters: dict[str, Any]) -> dict[str, Any]:
    return {"or": list(filters)}


def _select_all(view_id: ViewId) -> QuerySelect:
    return QuerySelect(sources=[QuerySelectSource(source=view_id, properties=["*"])])


class InstanceSourceAPI:
    def __init__(self, instances_api: InstancesAPI) -> None:
        self._instances_api = instances_api
        self._RETRIEVE_LIMIT = 1000
        self._view_id = INSTANCE_SOURCE_VIEW_ID

    def retrieve(
        self,
        ids: Sequence[AssetCentricId] | None = None,
        *,
        external_ids: Sequence[AssetCentricExternalId] | None = None,
    ) -> list[InstanceSource]:
        """Retrieve a list of instance sources by their IDs.

        Args:
            ids: A sequence of AssetCentricId objects representing the IDs of the instance sources to retrieve.
            external_ids: A sequence of AssetCentricExternalId objects representing the external IDs.

        """
        if ids is not None and external_ids is None:
            id_property = "id"
            selected_ids: Sequence[AssetCentricId] | Sequence[AssetCentricExternalId] = ids
        elif external_ids is not None and ids is None:
            id_property = "classicExternalId"
            selected_ids = external_ids
        else:
            raise ValueError("Exactly one of 'ids' or 'external_ids' must be provided.")

        results: list[InstanceSource] = []
        for chunk in chunker_sequence(selected_ids, self._RETRIEVE_LIMIT):
            query_request = QueryRequest(
                with_={
                    "instanceSource": QueryNodeExpression(
                        nodes=QueryNodeTableExpression(
                            filter=_and_filter(
                                _has_data_filter(self._view_id),
                                self._create_dms_filter(chunk, id_property),
                            ),
                        ),
                        limit=len(chunk),
                    ),
                },
                select={"instanceSource": _select_all(self._view_id)},
            )
            response = self._instances_api.query(query_request, type_results=False)
            for item in response.items.get("instanceSource", []):
                results.append(InstanceSource.model_validate(item))
        return results

    @staticmethod
    def _create_dms_filter(
        ids: Sequence[AssetCentricId | AssetCentricExternalId], id_property: str
    ) -> dict[str, JsonValue]:
        """Create a filter that matches all the AssetCentricIds in the list."""
        to_or_filters: list[dict[str, JsonValue]] = []
        for resource_type, resource_ids in groupby(
            sorted(ids, key=lambda x: x.resource_type), key=lambda x: x.resource_type
        ):
            is_resource = _equals_filter(INSTANCE_SOURCE_VIEW_ID, "resourceType", resource_type)
            is_id = _in_filter(
                INSTANCE_SOURCE_VIEW_ID,
                id_property,
                [resource_id.id_value for resource_id in resource_ids],
            )
            to_or_filters.append(_and_filter(is_resource, is_id))
        return _or_filter(*to_or_filters)

    def list(self, resource_type: AssetCentricType, limit: int | None = 100) -> list[InstanceSource]:
        """List instance sources filtered by resource type."""
        filter_ = InstanceFilter(
            instance_type="node",
            source=self._view_id,
            filter=_equals_filter(self._view_id, "resourceType", resource_type),
        )
        nodes = self._instances_api.list(filter=filter_, limit=limit)
        return [InstanceSource.model_validate(node.dump()) for node in nodes]


class ResourceViewMappingsAPI(WrappedInstancesAPI[NodeId, ResourceViewMappingResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client, ResourceViewMappingRequest.VIEW_ID)

    def _validate_response(self, response: SuccessResponse) -> ResponseItems[NodeId]:
        return ResponseItems[NodeId].model_validate_json(response.body)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ResourceViewMappingResponse]:
        return PagedResponse[ResourceViewMappingResponse].model_validate_json(response.body)

    def list(self, resource_type: str | None = None, limit: int | None = 100) -> list[ResourceViewMappingResponse]:
        filter_: dict[str, Any] = {
            "equals": {
                "property": ["node", "space"],
                "value": RESOURCE_VIEW_MAPPING_SPACE,
            }
        }
        if resource_type:
            filter_ = {
                "and": [
                    filter_,
                    {
                        "equals": {
                            "property": ResourceViewMappingRequest.VIEW_ID.as_property_reference("resourceType"),
                            "value": resource_type,
                        }
                    },
                ]
            }
        return super()._list_instances(filter=filter_, instance_type="node", limit=limit)


class CreatedSourceSystemAPI:
    def __init__(self, instances_api: InstancesAPI) -> None:
        self._instances_api = instances_api
        self._RETRIEVE_LIMIT = 1000
        self._view_id = CREATED_SOURCE_SYSTEM_VIEW_ID

    def retrieve(self, source: SequenceNotStr[str]) -> list[CreatedSourceSystem]:
        """Retrieve one or more created source systems by their source strings."""
        results: list[CreatedSourceSystem] = []
        for chunk in chunker_sequence(source, self._RETRIEVE_LIMIT):  # type: ignore[type-var]
            query_request = QueryRequest(
                with_={
                    "sourceSystem": QueryNodeExpression(
                        nodes=QueryNodeTableExpression(
                            filter=_and_filter(
                                _has_data_filter(self._view_id),
                                self._create_dms_filter(chunk),
                            ),
                        ),
                        limit=len(chunk),
                    ),
                },
                select={"sourceSystem": _select_all(self._view_id)},
            )
            response = self._instances_api.query(query_request, type_results=False)
            for item in response.items.get("sourceSystem", []):
                results.append(CreatedSourceSystem.model_validate(item))
        return results

    def _create_dms_filter(self, source: SequenceNotStr[str]) -> dict[str, JsonValue]:
        """Create a filter that matches all CreatedSourceSystem with given source in the list."""
        if not source:
            raise ValueError("Cannot create a filter from an empty source list.")
        return _in_filter(self._view_id, "source", list(source))

    def list(self, limit: int | None = None) -> list[CreatedSourceSystem]:
        """Lists all created source systems."""
        filter_ = InstanceFilter(instance_type="node", source=self._view_id)
        nodes = self._instances_api.list(filter=filter_, limit=limit)
        return [CreatedSourceSystem.model_validate(node.dump()) for node in nodes]


_T = TypeVar("_T", bound=int | str)


class SpaceSourceAPI:
    def __init__(self, instances_api: InstancesAPI) -> None:
        self._instances_api = instances_api
        self._RETRIEVE_LIMIT = 1000
        self._view_id = SPACE_SOURCE_VIEW_ID
        self._cache_by_id: dict[int, SpaceSource] = {}
        self._cache_by_external_id: dict[str, SpaceSource] = {}

    @overload
    def retrieve(self, data_set_id: int) -> SpaceSource | None: ...

    @overload
    def retrieve(self, data_set_id: Sequence[int]) -> list[SpaceSource]: ...

    @overload
    def retrieve(self, *, data_set_external_id: str) -> SpaceSource | None: ...

    @overload
    def retrieve(self, *, data_set_external_id: SequenceNotStr[str]) -> list[SpaceSource]: ...

    def retrieve(
        self,
        data_set_id: int | Sequence[int] | None = None,
        data_set_external_id: str | SequenceNotStr[str] | None = None,
    ) -> SpaceSource | list[SpaceSource] | None:
        """Retrieve a space source by data set ID or external ID.

        This method uses caching to avoid redundant API calls.
        """
        if data_set_id is not None and data_set_external_id is None:
            return self._retrieve_with_cache(
                value=data_set_id,
                property_name="dataSetId",
                cache=self._cache_by_id,
                is_single=isinstance(data_set_id, int),
            )
        elif data_set_external_id is not None:
            return self._retrieve_with_cache(
                # MyPy is confused by SequenceNotStr
                value=data_set_external_id,  # type: ignore[arg-type]
                property_name="dataSetExternalId",
                cache=self._cache_by_external_id,
                is_single=isinstance(data_set_external_id, str),
            )
        else:
            raise ValueError("Either data_set_id or data_set_external_id must be provided, but not both.")

    def _retrieve_with_cache(
        self,
        value: _T | Sequence[_T],
        property_name: str,
        cache: dict[_T, SpaceSource],
        is_single: bool,
    ) -> SpaceSource | list[SpaceSource] | None:
        """Retrieve space sources with caching support."""
        values: list[_T] = [value] if is_single else list(value)  # type: ignore[arg-type, list-item]

        cached_results: list[SpaceSource] = []
        missing_values: list[_T] = []
        for val in values:
            if val in cache:
                cached_results.append(cache[val])
            else:
                missing_values.append(val)

        if missing_values:
            fetched = self._retrieve_by_property(property_name=property_name, values=missing_values)
            for item in fetched:
                self._add_to_cache(item)
                cached_results.append(item)

        if is_single:
            return cached_results[0] if cached_results else None
        return cached_results

    def _add_to_cache(self, space_source: SpaceSource) -> None:
        """Add a space source to both caches."""
        if space_source.data_set_id:
            self._cache_by_id[space_source.data_set_id] = space_source
        if space_source.data_set_external_id is not None:
            self._cache_by_external_id[space_source.data_set_external_id] = space_source

    def _retrieve_by_property(
        self,
        property_name: str,
        values: Sequence[_T],
    ) -> list[SpaceSource]:
        """Retrieve space sources by filtering on a specific property."""
        results: list[SpaceSource] = []
        for chunk in chunker_sequence(values, self._RETRIEVE_LIMIT):
            query_request = QueryRequest(
                with_={
                    "spaceSource": QueryNodeExpression(
                        nodes=QueryNodeTableExpression(
                            filter=_and_filter(
                                _has_data_filter(self._view_id),
                                _in_filter(self._view_id, property_name, list(chunk)),
                            ),
                        ),
                        limit=len(chunk),
                    ),
                },
                select={"spaceSource": _select_all(self._view_id)},
            )
            response = self._instances_api.query(query_request, type_results=False)
            for item in response.items.get("spaceSource", []):
                results.append(SpaceSource.model_validate(item))
        return results

    def list(self, limit: int = -1) -> list[SpaceSource]:
        """Lists all space sources and populates the cache."""
        effective_limit = None if limit == -1 else limit
        filter_ = InstanceFilter(instance_type="node", source=self._view_id)
        nodes = self._instances_api.list(filter=filter_, limit=effective_limit)
        results = [SpaceSource.model_validate(node.dump()) for node in nodes]
        for space_source in results:
            self._add_to_cache(space_source)
        return results


_T_Cached = TypeVar("_T_Cached", bound=NodeId | ViewId)


class LookupAPI:
    def __init__(self, instances_api: InstancesAPI, resource_type: AssetCentricType) -> None:
        self._instances_api = instances_api
        self._resource_type = resource_type
        self._view_id = INSTANCE_SOURCE_VIEW_ID
        self._node_id_by_id: dict[int, NodeId | None] = {}
        self._node_id_by_external_id: dict[str, NodeId | None] = {}
        self._consumer_view_id_by_id: dict[int, ViewId | None] = {}
        self._consumer_view_id_by_external_id: dict[str, ViewId | None] = {}
        self._RETRIEVE_LIMIT = 1000

    @overload
    def __call__(self, id: int, external_id: None = None) -> NodeId | None: ...

    @overload
    def __call__(self, id: SequenceNotStr[int], external_id: None = None) -> dict[int, NodeId]: ...

    @overload
    def __call__(self, *, external_id: str) -> NodeId | None: ...

    @overload
    def __call__(self, *, external_id: SequenceNotStr[str]) -> dict[str, NodeId]: ...

    def __call__(
        self, id: int | SequenceNotStr[int] | None = None, external_id: str | SequenceNotStr[str] | None = None
    ) -> dict[int, NodeId] | dict[str, NodeId] | NodeId | None:
        """Lookup NodeId by either internal ID or external ID."""
        if id is not None and external_id is None:
            return self._lookup(
                identifier=id,
                cache=self._node_id_by_id,
                property_name="id",
                return_type=NodeId,
                input_type=int,
            )
        elif external_id is not None and id is None:
            return self._lookup(
                identifier=external_id,
                cache=self._node_id_by_external_id,
                property_name="classicExternalId",
                return_type=NodeId,
                input_type=str,
            )
        else:
            raise TypeError("Either id or external_id must be provided, but not both.")

    @overload
    def consumer_view(self, id: int, external_id: None = None) -> ViewId | None: ...

    @overload
    def consumer_view(self, id: SequenceNotStr[int], external_id: None = None) -> dict[int, ViewId]: ...

    @overload
    def consumer_view(self, *, external_id: str) -> ViewId | None: ...

    @overload
    def consumer_view(self, *, external_id: SequenceNotStr[str]) -> dict[str, ViewId]: ...

    def consumer_view(
        self, id: int | SequenceNotStr[int] | None = None, external_id: str | SequenceNotStr[str] | None = None
    ) -> dict[int, ViewId] | dict[str, ViewId] | ViewId | None:
        """Lookup Consumer ViewReference by either internal ID or external ID."""
        if id is not None and external_id is None:
            return self._lookup(
                identifier=id,
                cache=self._consumer_view_id_by_id,
                property_name="id",
                return_type=ViewId,
                input_type=int,
            )
        elif external_id is not None and id is None:
            return self._lookup(
                identifier=external_id,
                cache=self._consumer_view_id_by_external_id,
                property_name="classicExternalId",
                return_type=ViewId,
                input_type=str,
            )
        else:
            raise TypeError("Either id or external_id must be provided, but not both.")

    def _lookup(
        self,
        identifier: _T | SequenceNotStr[_T],
        cache: dict[_T, _T_Cached | None],
        property_name: Literal["id", "classicExternalId"],
        return_type: type[_T_Cached],
        input_type: type[_T],
    ) -> dict[_T, _T_Cached] | _T_Cached | None:
        """Generic lookup method for both NodeId and ViewReference by id or external_id."""
        is_single = isinstance(identifier, input_type)
        # MyPy does not understand that if is_single is True, identifier is _T, else SequenceNotStr[_T].
        identifiers: list[_T] = [identifier] if is_single else list(identifier)  # type: ignore[arg-type, list-item]

        missing = [id_ for id_ in identifiers if id_ not in cache]
        if missing:
            self._fetch_and_cache(missing, by=property_name)

        if is_single:
            return cache.get(identifier)  # type: ignore[arg-type]

        return {id_: value for id_ in identifiers if isinstance(value := cache.get(id_), return_type)}

    def _fetch_and_cache(self, identifiers: Sequence[int | str], by: Literal["id", "classicExternalId"]) -> None:
        for chunk in chunker_sequence(identifiers, self._RETRIEVE_LIMIT):
            query_request = QueryRequest(
                with_={
                    "instanceSource": QueryNodeExpression(
                        nodes=QueryNodeTableExpression(
                            filter=_and_filter(
                                _has_data_filter(self._view_id),
                                _equals_filter(self._view_id, "resourceType", self._resource_type),
                                _in_filter(self._view_id, by, list(chunk)),
                            ),
                        ),
                        limit=len(chunk),
                    ),
                },
                select={"instanceSource": _select_all(self._view_id)},
            )
            response = self._instances_api.query(query_request, type_results=False)
            for item in response.items.get("instanceSource", []):
                instance_source = InstanceSource.model_validate(item)
                node_id = instance_source.as_id()
                self._node_id_by_id[instance_source.id_] = node_id
                self._consumer_view_id_by_id[instance_source.id_] = instance_source.consumer_view()
                if instance_source.classic_external_id:
                    self._node_id_by_external_id[instance_source.classic_external_id] = node_id
                    self._consumer_view_id_by_external_id[instance_source.classic_external_id] = (
                        instance_source.consumer_view()
                    )
            missing = set(chunk) - set(self._node_id_by_id.keys()) - set(self._node_id_by_external_id.keys())
            if by == "id":
                for missing_id in cast(set[int], missing):
                    if missing_id not in self._node_id_by_id:
                        self._node_id_by_id[missing_id] = None
                    if missing_id not in self._consumer_view_id_by_id:
                        self._consumer_view_id_by_id[missing_id] = None
            elif by == "classicExternalId":
                for missing_ext_id in cast(set[str], missing):
                    if missing_ext_id not in self._node_id_by_external_id:
                        self._node_id_by_external_id[missing_ext_id] = None
                    if missing_ext_id not in self._consumer_view_id_by_external_id:
                        self._consumer_view_id_by_external_id[missing_ext_id] = None


class MigrationLookupAPI:
    def __init__(self, instances_api: InstancesAPI) -> None:
        self.assets = LookupAPI(instances_api, "asset")
        self.events = LookupAPI(instances_api, "event")
        self.files = LookupAPI(instances_api, "file")
        self.time_series = LookupAPI(instances_api, "timeseries")


class MigrationAPI:
    def __init__(self, instances_api: InstancesAPI, http_client: HTTPClient) -> None:
        self.instance_source = InstanceSourceAPI(instances_api)
        self.resource_view_mapping = ResourceViewMappingsAPI(http_client)
        self.created_source_system = CreatedSourceSystemAPI(instances_api)
        self.space_source = SpaceSourceAPI(instances_api)
        self.lookup = MigrationLookupAPI(instances_api)
