from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from itertools import zip_longest
from typing import Generic, Literal, TypeAlias, TypeVar, overload

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import APIMethod, Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InstanceDefinitionId, NodeId, T_InstanceId, ViewId
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceRequest,
    InstanceResponse,
    QueryRequest,
    T_InstancesListRequest,
    T_InstancesListResponse,
    T_WrappedInstanceRequest,
    T_WrappedInstanceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._instance import InstanceSlimDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._query import (
    QueryEdgeExpression,
    QueryEdgeTableExpression,
    QueryNodeExpression,
    QueryNodeTableExpression,
    QueryResponseTyped,
    QueryResponseUntyped,
    QuerySelect,
    QuerySelectSource,
    QuerySortSpec,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

METHOD_MAP: dict[APIMethod, Endpoint] = {
    "upsert": Endpoint(method="POST", path="/models/instances", item_limit=1000),
    "retrieve": Endpoint(method="POST", path="/models/instances/byids", item_limit=1000),
    "delete": Endpoint(method="POST", path="/models/instances/delete", item_limit=1000),
    "list": Endpoint(method="POST", path="/models/instances/list", item_limit=1000),
}
QUERY_ENDPOINT = Endpoint(method="POST", path="/models/instances/query", item_limit=1000)
SYNC_ENDPOINT = Endpoint(method="POST", path="/models/instances/sync", item_limit=1000)
INSTANCE_UPSERT_ENDPOINT = METHOD_MAP["upsert"]
INSTANCE_DELETE_ENDPOINT = METHOD_MAP["delete"]

QueryEndpoint: TypeAlias = Literal["query", "sync"]

_T_QueryResponse = TypeVar("_T_QueryResponse", bound=QueryResponseTyped | QueryResponseUntyped)


class InstancesAPI(CDFResourceAPI[InstanceResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client=http_client, method_endpoint_map=METHOD_MAP)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[InstanceResponse]:
        return PagedResponse[InstanceResponse].model_validate_json(response.body)

    def _validate_response(self, response: SuccessResponse) -> ResponseItems[InstanceDefinitionId]:
        return ResponseItems[InstanceDefinitionId].model_validate_json(response.body)

    def create(self, items: Sequence[InstanceRequest]) -> list[InstanceSlimDefinition]:
        """Create instances in CDF.

        Args:
            items: List of InstanceRequest objects to create.
        Returns:
            List of created InstanceSlimDefinition objects.
        """
        response_items: list[InstanceSlimDefinition] = []
        for response in self._chunk_requests(items, "upsert", self._serialize_items):
            response_items.extend(PagedResponse[InstanceSlimDefinition].model_validate_json(response.body).items)
        return response_items

    def retrieve(self, items: Sequence[InstanceDefinitionId], source: ViewId | None = None) -> list[InstanceResponse]:
        """Retrieve instances from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            source: Optional ViewReference to specify the source view for the instances.
        Returns:
            List of retrieved InstanceResponse objects.
        """
        return self._request_item_response(
            items,
            method="retrieve",
            extra_body={"sources": [{"source": source.dump(include_type=True)}]} if source else None,
        )

    def delete(self, items: Sequence[InstanceDefinitionId]) -> list[InstanceDefinitionId]:
        """Delete instances from CDF.

        Args:
            items: List of TypedInstanceIdentifier objects to delete.
        """
        response_items: list[InstanceDefinitionId] = []
        for response in self._chunk_requests(items, "delete", self._serialize_items):
            response_items.extend(self._validate_response(response).items)
        return response_items

    def paginate(
        self,
        filter: InstanceFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
        endpoint: QueryEndpoint = "query",
    ) -> PagedResponse[InstanceResponse]:
        """Iterate over all instances in CDF.

        Args:
           filter: InstanceFilter to filter instances.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.
            endpoint: Which endpoint to use

        Returns:
            PagedResponse of InstanceResponse objects.
        """
        request = self._create_query(filter, limit, cursor)
        response = self.query(request, type_results=True, endpoint=endpoint, exhaust_sub_selections=False)
        return PagedResponse(items=response.items["root"], nextCursor=response.root_cursor)

    def _create_query(
        self, filter: InstanceFilter | None, limit: int | None, cursor: str | None = None
    ) -> QueryRequest:
        """Create a query from the instance filter"""

        # We sort by space and externalId to get a stable sort order.
        #
        #         This is also more performant than sorting by using the default sort, which will sort on
        #         internal CDF IDs. This will be slow if you have deleted a lot of instances, as they will be counted.
        #         By sorting on space and externalId, we avoid this issue.
        if filter is None:
            query = QueryRequest(
                with_={
                    "root": QueryNodeExpression(
                        limit=limit,
                        nodes=QueryNodeTableExpression(),
                        sort=[
                            QuerySortSpec(property=["node", "space"]),
                            QuerySortSpec(property=["node", "externalId"]),
                        ],
                    )
                },
                select={"root": QuerySelect()},
                root="root",
            )
            if cursor is not None:
                query.cursors = {"root": cursor}
            return query

        if filter.instance_type == "edge":
            expression: QueryNodeExpression | QueryEdgeExpression = QueryEdgeExpression(
                limit=limit,
                edges=QueryEdgeTableExpression(filter=filter.dump_filter(include_has_data=True)),
                sort=[QuerySortSpec(property=["edge", "space"]), QuerySortSpec(property=["edge", "externalId"])],
            )
        else:  # Node or none
            expression = QueryNodeExpression(
                limit=limit,
                nodes=QueryNodeTableExpression(filter=filter.dump_filter(include_has_data=True)),
                sort=[QuerySortSpec(property=["node", "space"]), QuerySortSpec(property=["node", "externalId"])],
            )
        sources: list[QuerySelectSource] = []
        if filter.source:
            sources.append(QuerySelectSource(source=filter.source, properties=["*"]))
        query = QueryRequest(with_={"root": expression}, select={"root": QuerySelect(sources=sources)}, root="root")
        if cursor:
            query.cursors = {"root": cursor}
        return query

    def iterate(
        self,
        filter: InstanceFilter | None = None,
        limit: int | None = 100,
        endpoint: QueryEndpoint = "query",
        init_cursor: str | None = None,
    ) -> Iterable[list[InstanceResponse]]:
        """Iterate over all instances in CDF.

        Args:
            filter: InstanceFilter to filter instances.
            limit: Maximum number of items to return per page.
            endpoint: Which endpoint to use
            init_cursor: Which cursor to use

        Returns:
            Iterable of lists of InstanceResponse objects.
        """
        endpoint_prop = self._get_endpoint(endpoint)
        chunk_limit = endpoint_prop.item_limit if limit is None else min(limit, endpoint_prop.item_limit)
        query = self._create_query(filter, chunk_limit, init_cursor)
        for response in self.query_iterate(
            query, type_results=True, endpoint=endpoint, exhaust_sub_selections=False, limit=limit
        ):
            yield response.items[response.root]

    def list(
        self, filter: InstanceFilter | None = None, limit: int | None = 100, endpoint: QueryEndpoint = "query"
    ) -> list[InstanceResponse]:
        """List all instances in CDF.

        Returns:
            List of InstanceResponse objects.
        """
        return [item for batch in self.iterate(filter=filter, limit=limit, endpoint=endpoint) for item in batch]

    @overload
    def query(
        self,
        query: QueryRequest,
        type_results: Literal[True] = True,
        endpoint: QueryEndpoint = "query",
        exhaust_sub_selections: bool = False,
    ) -> QueryResponseTyped: ...

    @overload
    def query(
        self,
        query: QueryRequest,
        type_results: Literal[False],
        endpoint: QueryEndpoint = "query",
        exhaust_sub_selections: bool = False,
    ) -> QueryResponseUntyped: ...

    def query(
        self,
        query: QueryRequest,
        type_results: bool = True,
        endpoint: QueryEndpoint = "query",
        exhaust_sub_selections: bool = False,
    ) -> QueryResponseTyped | QueryResponseUntyped:
        """Execute a query against the instances query endpoint.

        This uses the ``POST /models/instances/query`` endpoint which supports
        traversing the graph of nodes and edges using result set expressions.

        Args:
            query: The query request specifying what to retrieve.
                type_results: Whether to return typed results (QueryResponseTyped) or untyped results
                    (QueryResponseUntyped).
            endpoint: The endpoint to use for this query.
            exhaust_sub_selections: Whether to exhaust sub-selections, for example, if you are fetching nodes and all
                edges connected to those nodes. Setting this to true will fetch all edges.

        Returns:
            QueryResult containing matching instances grouped by result set expression name.
        """
        response_cls = QueryResponseTyped if type_results else QueryResponseUntyped
        endpoint_prop = self._get_endpoint(endpoint)
        return self._query(query, response_cls, endpoint_prop, exhaust_sub_selections, endpoint_name=endpoint)

    @overload
    def query_iterate(
        self,
        query: QueryRequest,
        type_results: Literal[True] = True,
        endpoint: QueryEndpoint = "query",
        exhaust_sub_selections: bool = False,
        limit: int | None = None,
    ) -> Iterable[QueryResponseTyped]: ...

    @overload
    def query_iterate(
        self,
        query: QueryRequest,
        type_results: Literal[False],
        endpoint: QueryEndpoint = "query",
        exhaust_sub_selections: bool = False,
        limit: int | None = None,
    ) -> Iterable[QueryResponseUntyped]: ...

    def query_iterate(
        self,
        query: QueryRequest,
        type_results: bool = True,
        endpoint: QueryEndpoint = "query",
        exhaust_sub_selections: bool = False,
        limit: int | None = None,
    ) -> Iterable[QueryResponseTyped | QueryResponseUntyped]:
        """Iterate over the results of a query against the instances query/sync endpoint."""
        endpoint_prop = self._get_endpoint(endpoint)
        response_cls = QueryResponseTyped if type_results else QueryResponseUntyped
        chunk_size = query.with_[query.root].limit or endpoint_prop.item_limit
        total = 0
        while True:
            batch = self._query(query, response_cls, endpoint_prop, exhaust_sub_selections, endpoint_name=endpoint)
            total += len(batch.items[query.root])
            next_cursor = batch.root_cursor
            yield batch
            if next_cursor is None or not batch or (limit is not None and total >= limit):
                break
            page_limit = chunk_size if limit is None else min(chunk_size, max(limit - total, 0))
            query.with_[query.root].limit = page_limit
            query.cursors = {query.root: next_cursor}

    def _query(
        self,
        query: QueryRequest,
        response_cls: type[_T_QueryResponse],
        endpoint: Endpoint,
        exhaust_sub_selections: bool,
        endpoint_name: QueryEndpoint,
    ) -> _T_QueryResponse:
        first: _T_QueryResponse | None = None
        while True:
            response = self._make_query(endpoint, query, response_cls, endpoint_name)
            if first is None:
                first = response
            else:
                for key in response.items:
                    if key != query.root and key in first.items:
                        # MyPy does not like the mix of type and untyped query responses.
                        first.items[key].extend(response.items[key])  # type: ignore[arg-type]
            if not exhaust_sub_selections:
                return first
            next_cursors: dict[str, str | None] = {}
            for select_id in query.select.keys():
                if select_id == query.root:
                    continue
                sub_cursor = response.next_cursor.get(select_id)
                if sub_cursor is not None:
                    next_cursors[select_id] = sub_cursor
            if not next_cursors or not response:
                return first
            # Keep the root cursor to iterate over all subitems.
            next_cursors[query.root] = (query.cursors or {}).get(query.root)
            query = query.model_copy(update={"cursors": next_cursors})

    def _make_query(
        self,
        endpoint: Endpoint,
        query: QueryRequest,
        response_cls: type[_T_QueryResponse],
        endpoint_name: QueryEndpoint,
    ) -> _T_QueryResponse:
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(endpoint.path),
            method=endpoint.method,
            body_content=query.dump(endpoint=endpoint_name),
        )
        response = self._http_client.request_single_retries(request)
        success = response.get_success_or_raise(request)
        # Wrong type hint in pydantic, response_cls.model_validate_json returns an instance
        # of that class no the class type.
        query_response: _T_QueryResponse = response_cls.model_validate_json(success.body)  # type: ignore[assignment]
        # We persist the root from the query. This is for convenience.
        query_response.root = query.root  #
        return query_response

    def _get_endpoint(self, endpoint: QueryEndpoint) -> Endpoint:
        if endpoint == "query":
            endpoint_prop = QUERY_ENDPOINT
        elif endpoint == "sync":
            endpoint_prop = SYNC_ENDPOINT
        else:
            raise NotImplementedError(f"Unknown endpoint {endpoint!r}")
        return endpoint_prop


class WrappedInstancesAPI(
    Generic[T_InstanceId, T_WrappedInstanceResponse], CDFResourceAPI[T_WrappedInstanceResponse], ABC
):
    """API for wrapped instances in CDF. It is intended to be subclassed for specific wrapped instance types."""

    def __init__(self, http_client: HTTPClient, view_id: ViewId) -> None:
        super().__init__(http_client=http_client, method_endpoint_map=METHOD_MAP)
        self._view_id = view_id

    @abstractmethod
    def _validate_response(self, response: SuccessResponse) -> ResponseItems[T_InstanceId]:
        raise NotImplementedError()

    def create(self, items: Sequence[T_WrappedInstanceRequest]) -> list[InstanceSlimDefinition]:
        """Create instances in CDF.

        Args:
            items: List of InstanceRequest objects to create.
        Returns:
            List of created InstanceSlimDefinition objects.
        """
        response_items: list[InstanceSlimDefinition] = []
        for response in self._chunk_requests(items, "upsert", self._serialize_items):
            response_items.extend(PagedResponse[InstanceSlimDefinition].model_validate_json(response.body).items)
        return response_items

    def retrieve(self, items: Sequence[T_InstanceId]) -> list[T_WrappedInstanceResponse]:
        """Retrieve instances from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            source: Optional ViewReference to specify the source view for the instances.
        Returns:
            List of retrieved InstanceResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"sources": [{"source": self._view_id.dump(include_type=True)}]}
        )

    def delete(self, items: Sequence[T_InstanceId]) -> list[T_InstanceId]:
        """Delete instances from CDF.

        Args:
            items: List of TypedInstanceIdentifier objects to delete.
        """
        response_items: list[T_InstanceId] = []
        for response in self._chunk_requests(items, "delete", self._serialize_items):
            response_items.extend(self._validate_response(response).items)
        return response_items

    def _list_instances(
        self,
        instance_type: Literal["node", "edge"] = "node",
        spaces: list[str] | None = None,
        filter: dict[str, JsonValue] | None = None,
        limit: int | None = 100,
    ) -> list[T_WrappedInstanceResponse]:
        """List all wrapped instances in CDF.

        Args:
            instance_type: The type of instance to list. Defaults to "node".
            spaces: Optional list of spaces to filter by.
            limit: Maximum number of items to return. If None, all items are returned.

        Returns:
            List of wrapped instance response objects.
        """
        filter_ = InstanceFilter(
            instance_type=instance_type,
            space=spaces,
            source=self._view_id,
            filter=filter,
        )
        body = {
            **filter_.dump(),
            "sort": [
                {"property": [instance_type, "space"], "direction": "ascending"},
                {"property": [instance_type, "externalId"], "direction": "ascending"},
            ],
        }
        return self._list(limit=limit, body=body)


class MultiWrappedInstancesAPI(Generic[T_InstancesListRequest, T_InstancesListResponse], ABC):
    """API for objects that wraps multiple instances in CDF.

    For example, a Canvas is a node with edges to elements in the Canvas represented as nodes. This wrapper class
    allows creating, retrieving, updating, and deleting such objects in CDF.

    """

    def __init__(self, http_client: HTTPClient, query_chunk: int) -> None:
        self._http_client = http_client
        self._method_endpoint_map = METHOD_MAP
        self._query_chunk = query_chunk

    @abstractmethod
    def _retrieve_query(self, item: Sequence[InstanceDefinitionId]) -> QueryRequest:
        raise NotImplementedError()

    @abstractmethod
    def _validate_query_response(self, query_response: QueryResponseUntyped) -> list[T_InstancesListResponse]:
        raise NotImplementedError()

    def create(self, items: Sequence[T_InstancesListRequest]) -> list[InstanceSlimDefinition]:
        """Create instances in CDF.

        Args:
            items: List of InstanceRequest objects to create.
        Returns:
            List of created InstanceSlimDefinition objects.
        """
        endpoint = self._method_endpoint_map["upsert"]
        response_items: list[InstanceSlimDefinition] = []
        for item in items:
            instance_dicts = item.dump_instances()
            item_response: list[InstanceSlimDefinition] = []
            for chunk in chunker_sequence(instance_dicts, endpoint.item_limit):
                request = RequestMessage(
                    endpoint_url=self._http_client.config.create_api_url(endpoint.path),
                    method=endpoint.method,
                    body_content={"items": chunk},  # type: ignore[dict-item]
                )
                response = self._http_client.request_single_retries(request)
                success = response.get_success_or_raise(request)
                paged_response = PagedResponse[InstanceSlimDefinition].model_validate_json(success.body)
                item_response.extend(paged_response.items)
            response_items.append(self._merge_instance_slim_definitions(item_response))
        return response_items

    def _merge_instance_slim_definitions(self, items: list[InstanceSlimDefinition]) -> InstanceSlimDefinition:
        """Merge multiple InstanceSlimDefinition objects into one.

        Args:
            items: List of InstanceSlimDefinition objects to merge.
        Returns:
            Merged InstanceSlimDefinition object.
        """
        if not items:
            raise ValueError("No items to merge.")
        # Assuming all items have the same space and external_id
        base_item = items[0]
        merged_item = InstanceSlimDefinition(
            instance_type=base_item.instance_type,
            version=base_item.version,
            was_modified=any(item.was_modified for item in items),
            space=base_item.space,
            external_id=base_item.external_id,
            created_time=min(item.created_time for item in items),
            last_updated_time=max(item.last_updated_time for item in items),
        )
        return merged_item

    def update(self, items: Sequence[T_InstancesListRequest]) -> list[InstanceSlimDefinition]:
        """Update multi-wrapped instances in CDF.

        This method automatically removes underlying instances that are part of the old object but not the new one.

        Args:
            items: A sequence of multi-wrapped instance requests to update.

        Returns:
            List of updated InstanceSlimDefinition objects, one for each item in the input sequence.
        """
        endpoint = self._method_endpoint_map["upsert"]
        updated: list[InstanceSlimDefinition] = []
        for item in items:
            identifier = item.as_id()
            retrieved = self.retrieve([identifier])
            if not retrieved:
                raise ValueError(f"Item with identifier {identifier} not found for update.")
            to_delete = [id.dump() for id in (set(retrieved[0].as_ids()) - set(item.as_ids()))]
            to_update = item.dump_instances()
            item_response: list[InstanceSlimDefinition] = []
            for upsert_chunk, delete_chunk in zip_longest(
                chunker_sequence(to_update, endpoint.item_limit),
                chunker_sequence(to_delete, endpoint.item_limit),
                fillvalue=None,
            ):
                body_content: dict[str, JsonValue] = {}
                if upsert_chunk:
                    # MyPy fails do understand that list[dict[str, JsonValue]] is a subtype of JsonValue
                    body_content["items"] = upsert_chunk  # type: ignore[assignment]
                if delete_chunk:
                    body_content["delete"] = delete_chunk  # type: ignore[assignment]

                request = RequestMessage(
                    endpoint_url=self._http_client.config.create_api_url(endpoint.path),
                    method=endpoint.method,
                    body_content=body_content,
                )

                response = self._http_client.request_single_retries(request)
                success = response.get_success_or_raise(request)
                paged_response = PagedResponse[InstanceSlimDefinition].model_validate_json(success.body)
                item_response.extend(paged_response.items)
            updated.append(self._merge_instance_slim_definitions(item_response))
        return updated

    def retrieve(self, items: Sequence[NodeId]) -> list[T_InstancesListResponse]:
        """Retrieve instances from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
        Returns:
            List of retrieved InstanceResponse objects.
        """
        retrieved: list[T_InstancesListResponse] = []
        for chunk in chunker_sequence(items, self._query_chunk):
            query = self._retrieve_query(chunk)
            request = RequestMessage(
                endpoint_url=self._http_client.config.create_api_url(QUERY_ENDPOINT.path),
                method=QUERY_ENDPOINT.method,
                body_content=query.dump(),
            )
            response = self._http_client.request_single_retries(request)
            success = response.get_success_or_raise(request)
            paged_response = QueryResponseUntyped.model_validate_json(success.body)
            retrieved.extend(self._validate_query_response(paged_response))
        return retrieved

    def delete(self, items: Sequence[NodeId]) -> list[NodeId]:
        """Delete instances from CDF.

        Args:
            items: List of TypedInstanceIdentifier objects to delete.
        """
        endpoint = self._method_endpoint_map["delete"]
        response_items: list[NodeId] = []
        for chunk in chunker_sequence(items, endpoint.item_limit):
            request = RequestMessage(
                endpoint_url=self._http_client.config.create_api_url(endpoint.path),
                method=endpoint.method,
                body_content={"items": [item.dump() for item in chunk]},
            )
            response = self._http_client.request_single_retries(request)
            success = response.get_success_or_raise(request)
            validated_response = ResponseItems[NodeId].model_validate_json(success.body)
            response_items.extend(validated_response.items)
        return response_items
