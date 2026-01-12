"""Base classes for resource clients.

This module provides the base infrastructure for resource-specific clients
that handle CRUD operations for CDF Data Modeling API resources.
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, Generic, Literal, TypeAlias, TypeVar

from pydantic import BaseModel, JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    RequestUpdateable,
    T_Identifier,
    T_RequestResource,
    T_ResponseResource,
)
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsRequest2,
    ItemsSuccessResponse2,
    RequestMessage2,
    SuccessResponse2,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

from .responses import PagedResponse, ResponseItems

_T_BaseModel = TypeVar("_T_BaseModel", bound=BaseModel)


@dataclass(frozen=True)
class Endpoint:
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
    path: str
    item_limit: int
    concurrency_max_workers: int = 1


APIMethod: TypeAlias = Literal["create", "retrieve", "update", "delete", "list"]


class CDFResourceAPI(Generic[T_Identifier, T_RequestResource, T_ResponseResource], ABC):
    """Generic resource API for CDF APIs

    This class provides the logic for working with CDF resources,
    including creating, retrieving, deleting, and listing resources.
    """

    def __init__(self, http_client: HTTPClient, method_endpoint_map: dict[APIMethod, Endpoint]) -> None:
        """Initialize the resource API.

        Args:
            http_client: The HTTP client to use for API requests.
            method_endpoint_map: A mapping of endpoint suffixes to their properties.
        """
        self._http_client = http_client
        self._method_endpoint_map = method_endpoint_map

    @classmethod
    def _serialize_items(cls, items: Sequence[BaseModel]) -> list[dict[str, JsonValue]]:
        """Serialize reference objects to JSON-compatible dicts."""
        return [item.model_dump(mode="json", by_alias=True) for item in items]

    @classmethod
    def _serialize_updates(
        cls, items: Sequence[RequestUpdateable], mode: Literal["patch", "replace"]
    ) -> list[dict[str, JsonValue]]:
        """Serialize updateable objects to JSON-compatible dicts."""
        return [item.as_update(mode=mode) for item in items]

    @abstractmethod
    def _page_response(self, response: SuccessResponse2 | ItemsSuccessResponse2) -> PagedResponse[T_ResponseResource]:
        """Parse a single item response."""
        raise NotImplementedError()

    @abstractmethod
    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[T_Identifier]:
        """Parse a reference response."""
        raise NotImplementedError()

    def _make_url(self, path: str = "") -> str:
        """Create the full URL for this resource endpoint."""
        return self._http_client.config.create_api_url(path)

    def _update(
        self, items: Sequence[RequestUpdateable], mode: Literal["patch", "replace"]
    ) -> list[T_ResponseResource]:
        """Update resources in chunks."""
        response_items: list[T_ResponseResource] = []
        for response in self._chunk_requests(
            items, "update", serialization=partial(self._serialize_updates, mode=mode)
        ):
            response_items.extend(self._page_response(response).items)
        return response_items

    def _request_item_response(
        self,
        items: Sequence[BaseModel],
        method: APIMethod,
        params: dict[str, Any] | None = None,
        extra_body: dict[str, Any] | None = None,
        endpoint: str | None = None,
    ) -> list[T_ResponseResource]:
        response_items: list[T_ResponseResource] = []
        for response in self._chunk_requests(items, method, self._serialize_items, params, extra_body, endpoint):
            response_items.extend(self._page_response(response).items)
        return response_items

    def _request_reference_response(
        self,
        items: Sequence[BaseModel],
        method: APIMethod,
        params: dict[str, Any] | None = None,
        extra_body: dict[str, Any] | None = None,
    ) -> list[T_Identifier]:
        all_ids: list[T_Identifier] = []
        for response in self._chunk_requests(items, method, self._serialize_items, params, extra_body):
            all_ids.extend(self._reference_response(response).items)
        return all_ids

    def _request_no_response(
        self,
        items: Sequence[BaseModel],
        method: APIMethod,
        params: dict[str, Any] | None = None,
        extra_body: dict[str, Any] | None = None,
        endpoint: str | None = None,
    ) -> None:
        list(self._chunk_requests(items, method, self._serialize_items, params, extra_body, endpoint))
        return None

    def _chunk_requests(
        self,
        items: Sequence[_T_BaseModel],
        method: APIMethod,
        serialization: Callable[[Sequence[_T_BaseModel]], list[dict[str, JsonValue]]],
        params: dict[str, Any] | None = None,
        extra_body: dict[str, Any] | None = None,
        endpoint_path: str | None = None,
    ) -> Iterable[SuccessResponse2]:
        """Send requests in chunks and yield responses.

        Args:
            items: The items to process.
            method: The API method to use. This is used ot look the up the endpoint.
            serialization: A function to serialize the items to JSON-compatible dicts.
            params: Optional query parameters for the request.
            extra_body: Optional extra body content to include in the request.
            endpoint_path: Optional override for the endpoint path.

        Yields:
            The successful responses from the API.
        """
        # Filter out None
        request_params = self._filter_out_none_values(params)
        endpoint = self._method_endpoint_map[method]

        for chunk in chunker_sequence(items, endpoint.item_limit):
            request = RequestMessage2(
                endpoint_url=f"{self._make_url(endpoint_path or endpoint.path)}",
                method=endpoint.method,
                body_content={"items": serialization(chunk), **(extra_body or {})},  # type: ignore[dict-item]
                parameters=request_params,
            )
            response = self._http_client.request_single_retries(request)
            yield response.get_success_or_raise()

    def _request_item_split_retries(
        self,
        items: Sequence[_T_BaseModel],
        method: APIMethod,
        params: dict[str, Any] | None = None,
        extra_body: dict[str, Any] | None = None,
    ) -> list[T_ResponseResource]:
        """Request items with retries, splitting on failures.

        This method handles large batches of items by chunking them according to the endpoint's item limit.
        If a single item fails, it splits the request into individual item requests to isolate the failure.

        Args:
            items: Sequence of items to request.
            method: API method to use for the request.
            params: Optional query parameters for the request.
            extra_body: Optional additional body fields for the request.
        Returns:
            List of response items.
        """
        response_items: list[T_ResponseResource] = []
        for response in self._chunk_requests_items_split_retries(items, method, params, extra_body):
            response_items.extend(self._page_response(response).items)
        return response_items

    def _chunk_requests_items_split_retries(
        self,
        items: Sequence[_T_BaseModel],
        method: APIMethod,
        params: dict[str, Any] | None = None,
        extra_body: dict[str, Any] | None = None,
    ) -> Iterable[ItemsSuccessResponse2]:
        """Request items with retries and splitting on failures.

        This method handles large batches of items by chunking them according to the endpoint's item limit.
        If a single item fails, it splits the request into individual item requests to isolate the failure.

        This can be useful to create ignore unknown IDs behavior when retrieving items for API endpoints that
        do not support it natively.

        Args:
            items: Sequence of items to request.
            method: API method to use for the request.
            params: Optional query parameters for the request.
            extra_body: Optional additional body fields for the request.
        Yields
            SuccessResponse2: Successful responses from the API. All failed items are skipped.

        """
        # Filter out None
        request_params = self._filter_out_none_values(params)
        endpoint = self._method_endpoint_map[method]

        for chunk in chunker_sequence(items, endpoint.item_limit):
            request = ItemsRequest2(
                endpoint_url=f"{self._make_url(endpoint.path)}",
                method=endpoint.method,
                parameters=request_params,
                items=chunk,
                extra_body_fields=extra_body,
            )
            responses = self._http_client.request_items_retries(request)
            for response in responses:
                if isinstance(response, ItemsSuccessResponse2):
                    yield response

    @classmethod
    def _filter_out_none_values(cls, params: dict[str, Any] | None) -> dict[str, Any] | None:
        request_params: dict[str, Any] | None = None
        if params:
            request_params = {k: v for k, v in params.items() if v is not None}
        return request_params

    @classmethod
    def _group_items_by_text_field(
        cls, items: Sequence[_T_BaseModel], field_name: str
    ) -> dict[str, list[_T_BaseModel]]:
        """Group items by a text field."""
        grouped_items: dict[str, list[_T_BaseModel]] = defaultdict(list)
        for item in items:
            key = str(getattr(item, field_name))
            grouped_items[key].append(item)
        return grouped_items

    def _iterate(
        self,
        limit: int,
        cursor: str | None = None,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        endpoint_path: str | None = None,
    ) -> PagedResponse[T_ResponseResource]:
        """Fetch a single page of resources.

        Args:
            params: Query parameters for the request. Supported parameters depend on
                the resource type but typically include:
                - cursor: Cursor for pagination
                - limit: Maximum number of items (defaults to list limit)
                - space: Filter by space
                - includeGlobal: Whether to include global resources
            body : Body content for the request, if applicable.
                limit: Maximum number of items to return in the page.
                cursor: Cursor for pagination.
            limit: Maximum number of items to return in the page.
            cursor: Cursor for pagination.

        Returns:
            A Page containing the items and the cursor for the next page.
        """
        endpoint = self._method_endpoint_map["list"]
        if not (0 < limit <= endpoint.item_limit):
            raise ValueError(f"Limit must be between 1 and {endpoint.item_limit}, got {limit}.")

        request_params = self._filter_out_none_values(params) or {}
        body = self._filter_out_none_values(body) or {}
        if endpoint.method == "GET":
            request_params["limit"] = limit
            if cursor is not None:
                request_params["cursor"] = cursor
        elif endpoint.method in {"POST", "PUT", "PATCH"}:
            body["limit"] = limit
            if cursor is not None:
                body["cursor"] = cursor
        else:
            raise NotImplementedError(f"Unsupported method {endpoint.method} for pagination.")

        request = RequestMessage2(
            endpoint_url=self._make_url(endpoint_path or endpoint.path),
            method=endpoint.method,
            parameters=request_params,
            body_content=body,
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()
        return self._page_response(response)

    def _list(
        self, limit: int | None = None, params: dict[str, Any] | None = None, endpoint_path: str | None = None
    ) -> list[T_ResponseResource]:
        """List all resources, handling pagination automatically."""
        all_items: list[T_ResponseResource] = []
        next_cursor: str | None = None
        total = 0
        endpoint = self._method_endpoint_map["list"]
        while True:
            page_limit = endpoint.item_limit if limit is None else min(limit - total, endpoint.item_limit)
            page = self._iterate(limit=page_limit, cursor=next_cursor, params=params, endpoint_path=endpoint_path)
            all_items.extend(page.items)
            total += len(page.items)
            if page.next_cursor is None or (limit is not None and total >= limit):
                break
            next_cursor = page.next_cursor
        return all_items
