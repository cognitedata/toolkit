from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from typing import Any, Generic, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import APIMethod, Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceRequest,
    InstanceResponse,
    ViewReference,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._instance import InstanceSlimDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    T_InstancesListRequest,
    T_InstancesListResponse,
    T_TypedInstanceIdentifier,
    T_WrappedInstanceRequest,
    T_WrappedInstanceResponse,
    TypedInstanceIdentifier,
    TypedNodeIdentifier,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

METHOD_MAP: dict[APIMethod, Endpoint] = {
    "upsert": Endpoint(method="POST", path="/models/instances", item_limit=1000),
    "retrieve": Endpoint(method="POST", path="/models/instances/byids", item_limit=1000),
    "delete": Endpoint(method="POST", path="/models/instances/delete", item_limit=1000),
    "list": Endpoint(method="POST", path="/models/instances/list", item_limit=1000),
}


class InstancesAPI(CDFResourceAPI[TypedInstanceIdentifier, InstanceRequest, InstanceResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client=http_client, method_endpoint_map=METHOD_MAP)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[InstanceResponse]:
        return PagedResponse[InstanceResponse].model_validate_json(response.body)

    def _validate_response(self, response: SuccessResponse) -> ResponseItems[TypedInstanceIdentifier]:
        return ResponseItems[TypedInstanceIdentifier].model_validate_json(response.body)

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

    def retrieve(
        self, items: Sequence[TypedInstanceIdentifier], source: ViewReference | None = None
    ) -> list[InstanceResponse]:
        """Retrieve instances from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            source: Optional ViewReference to specify the source view for the instances.
        Returns:
            List of retrieved InstanceResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"sources": [{"source": source.dump()}]} if source else None
        )

    def delete(self, items: Sequence[TypedInstanceIdentifier]) -> list[TypedInstanceIdentifier]:
        """Delete instances from CDF.

        Args:
            items: List of TypedInstanceIdentifier objects to delete.
        """
        response_items: list[TypedInstanceIdentifier] = []
        for response in self._chunk_requests(items, "delete", self._serialize_items):
            response_items.extend(self._validate_response(response).items)
        return response_items

    @staticmethod
    def _create_sort_body(instance_type: Literal["node", "edge"] | None) -> list[dict]:
        """We sort by space and externalId to get a stable sort order.

        This is also more performant than sorting by using the default sort, which will sort on
        internal CDF IDs. This will be slow if you have deleted a lot of instances, as they will be counted.
        By sorting on space and externalId, we avoid this issue.
        """
        instance_type = instance_type or "node"
        return [
            {
                "property": [instance_type, "space"],
                "direction": "ascending",
            },
            {
                "property": [instance_type, "externalId"],
                "direction": "ascending",
            },
        ]

    @classmethod
    def _create_body(cls, filter: InstanceFilter | None) -> dict[str, Any]:
        return {
            **(filter.model_dump(exclude_none=True) if filter else {}),
            "sort": cls._create_sort_body(filter.instance_type if filter else "node"),
        }

    def paginate(
        self,
        filter: InstanceFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[InstanceResponse]:
        """Iterate over all instances in CDF.

        Args:
           filter: InstanceFilter to filter instances.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of InstanceResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=self._create_body(filter),
        )

    def iterate(self, filter: InstanceFilter | None = None, limit: int = 100) -> Iterable[list[InstanceResponse]]:
        """Iterate over all instances in CDF.

        Args:
            filter: InstanceFilter to filter instances.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of InstanceResponse objects.
        """
        return self._iterate(limit=limit, body=self._create_body(filter))

    def list(self, filter: InstanceFilter | None = None, limit: int | None = 100) -> list[InstanceResponse]:
        """List all instances in CDF.

        Returns:
            List of InstanceResponse objects.
        """
        return self._list(limit=limit, body=self._create_body(filter))


class WrappedInstancesAPI(
    CDFResourceAPI[T_TypedInstanceIdentifier, T_WrappedInstanceRequest, T_WrappedInstanceResponse], ABC
):
    """API for wrapped instances in CDF. It is intended to be subclassed for specific wrapped instance types."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(http_client=http_client, method_endpoint_map=METHOD_MAP)

    @abstractmethod
    def _validate_response(self, response: SuccessResponse) -> ResponseItems[T_TypedInstanceIdentifier]:
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

    def retrieve(
        self, items: Sequence[T_TypedInstanceIdentifier], source: ViewReference | None = None
    ) -> list[T_WrappedInstanceResponse]:
        """Retrieve instances from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            source: Optional ViewReference to specify the source view for the instances.
        Returns:
            List of retrieved InstanceResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"sources": [{"source": source.dump()}]} if source else None
        )

    def delete(self, items: Sequence[T_TypedInstanceIdentifier]) -> list[T_TypedInstanceIdentifier]:
        """Delete instances from CDF.

        Args:
            items: List of TypedInstanceIdentifier objects to delete.
        """
        response_items: list[T_TypedInstanceIdentifier] = []
        for response in self._chunk_requests(items, "delete", self._serialize_items):
            response_items.extend(self._validate_response(response).items)
        return response_items


class MultiWrappedInstancesAPI(Generic[T_InstancesListRequest, T_InstancesListResponse], ABC):
    """API for objects that wraps multiple instances in CDF.

    For example, a Canvas is a node with edges to elements in the Canvas represented as nodes. This wrapper class
    allows creating, retrieving, updating, and deleting such objects in CDF.

    """

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client
        self._method_endpoint_map = METHOD_MAP

    def create(self, items: Sequence[T_WrappedInstanceRequest]) -> list[InstanceSlimDefinition]:
        """Create instances in CDF.

        Args:
            items: List of InstanceRequest objects to create.
        Returns:
            List of created InstanceSlimDefinition objects.
        """
        endpoint = self._method_endpoint_map["upsert"]
        response_items: list[InstanceSlimDefinition] = []
        for item in items:
            instance_dicts = item.dump()
            item_response: list[InstanceSlimDefinition] = []
            for chunk in chunker_sequence(instance_dicts, endpoint.item_limit):
                request = RequestMessage(
                    endpoint_url=endpoint.path,
                    method=endpoint.method,
                    body_content={"items": chunk},
                )
                response = self._http_client.request_single_retries(request)
                success = response.get_success_or_raise()
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

    def update(self, items: Sequence[T_WrappedInstanceRequest]) -> list[InstanceSlimDefinition]:
        """
        # Automatically remove extra instances that are not there any more.

        Args:
            items:

        Returns:

        """
        raise NotImplementedError(f"{type(self).__name__} does not support update operation.")

    def retrieve(self, items: Sequence[TypedNodeIdentifier]) -> list[T_WrappedInstanceResponse]:
        """Retrieve instances from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
        Returns:
            List of retrieved InstanceResponse objects.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support retrieve operation.")

    def delete(self, items: Sequence[TypedNodeIdentifier]) -> list[TypedNodeIdentifier]:
        """Delete instances from CDF.

        Args:
            items: List of TypedInstanceIdentifier objects to delete.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support delete operation.")
