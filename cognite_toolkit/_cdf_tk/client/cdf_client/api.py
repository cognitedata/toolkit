"""Base classes for resource clients.

This module provides the base infrastructure for resource-specific clients
that handle CRUD operations for CDF Data Modeling API resources.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeAlias

from pydantic import BaseModel, JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import T_Identifier, T_RequestResource, T_ResponseResource
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage2, SuccessResponse2
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

from .responses import PagedResponse, ResponseItems


@dataclass(frozen=True)
class ResourceLimits:
    """Configuration for API endpoint limits.

    These limits control the maximum number of items per request
    for each type of operation.

    Attributes:
        create: Maximum items per create request. Default is 100.
        retrieve: Maximum items per retrieve request. Default is 100.
        delete: Maximum items per delete request. Default is 100.
        list: Maximum items per list/iterate request. Default is 1000.
    """

    create: int = 100
    retrieve: int = 100
    delete: int = 100
    list: int = 1000


@dataclass(frozen=True)
class Endpoint:
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
    path: str
    item_limit: int
    concurrency_max_workers: int = 1


APIMethod: TypeAlias = Literal["create", "retrieve", "delete", "update", "list"]


class CDFResourceAPI(Generic[T_Identifier, T_RequestResource, T_ResponseResource], ABC):
    """Generic resource API for CDF APIs

    This class provides the logic for working with CDF resources,
    including creating, retrieving, deleting, and listing resources.
    """

    def __init__(
        self, http_client: HTTPClient, resource_endpoint: str, method_endpoint_map: dict[APIMethod, Endpoint]
    ) -> None:
        """Initialize the resource API.

        Args:
            http_client: The HTTP client to use for API requests.
            resource_endpoint: The API endpoint path for this resource (e.g., '/models/spaces').
            method_endpoint_map: A mapping of endpoint suffixes to their properties.
        """
        self._http_client = http_client
        self._resource_endpoint = resource_endpoint
        self._method_endpoint_map = method_endpoint_map

    @classmethod
    def _serialize_items(cls, items: Sequence[BaseModel]) -> list[dict[str, JsonValue]]:
        """Serialize reference objects to JSON-compatible dicts."""
        return [item.model_dump(mode="json", by_alias=True) for item in items]

    @abstractmethod
    def _page_response(self, response: SuccessResponse2) -> PagedResponse[T_ResponseResource]:
        """Parse a single item response."""
        raise NotImplementedError()

    @abstractmethod
    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[T_Identifier]:
        """Parse a reference response."""
        raise NotImplementedError()

    def _make_url(self, suffix: str = "") -> str:
        """Create the full URL for this resource endpoint."""
        return self._http_client.config.create_api_url(f"{self._resource_endpoint}{suffix}")

    def _request_item_response(
        self,
        items: Sequence[BaseModel],
        method: APIMethod,
        params: dict[str, Any] | None = None,
    ) -> list[T_ResponseResource]:
        response_items: list[T_ResponseResource] = []
        for response in self._chunk_requests(items, method, params):
            response_items.extend(self._page_response(response).items)
        return response_items

    def _request_reference_response(
        self,
        items: Sequence[BaseModel],
        method: APIMethod,
        params: dict[str, Any] | None = None,
    ) -> list[T_Identifier]:
        all_ids: list[T_Identifier] = []
        for response in self._chunk_requests(items, method, params):
            all_ids.extend(self._reference_response(response).items)
        return all_ids

    def _request_no_response(
        self, items: Sequence[BaseModel], method: APIMethod, params: dict[str, Any] | None = None
    ) -> None:
        list(self._chunk_requests(items, method, params))
        return None

    def _chunk_requests(
        self, items: Sequence[BaseModel], method: APIMethod, params: dict[str, Any] | None = None
    ) -> Iterable[SuccessResponse2]:
        # Filter out None
        request_params = self._filter_out_none_values(params)
        endpoint = self._method_endpoint_map[method]

        for chunk in chunker_sequence(items, endpoint.item_limit):
            request = RequestMessage2(
                endpoint_url=f"{self._make_url(endpoint.path)}",
                method=endpoint.method,
                body_content={"items": self._serialize_items(chunk)},  # type: ignore[dict-item]
                parameters=request_params,
            )
            response = self._http_client.request_single_retries(request)
            yield response.get_success_or_raise()

    @classmethod
    def _filter_out_none_values(cls, params: dict[str, Any] | None) -> dict[str, str | int | float | bool] | None:
        request_params: dict[str, str | int | float | bool] | None = None
        if params:
            request_params = {k: v for k, v in params.items() if v is not None}
        return request_params

    def _iterate(
        self, limit: int, cursor: str | None = None, params: dict[str, Any] | None = None
    ) -> PagedResponse[T_ResponseResource]:
        """Fetch a single page of resources.

        Args:
            params: Query parameters for the request. Supported parameters depend on
                the resource type but typically include:
                - cursor: Cursor for pagination
                - limit: Maximum number of items (defaults to list limit)
                - space: Filter by space
                - includeGlobal: Whether to include global resources

        Returns:
            A Page containing the items and the cursor for the next page.
        """
        endpoint = self._method_endpoint_map["list"]
        if not (0 < limit <= endpoint.item_limit):
            raise ValueError(f"Limit must be between 1 and {endpoint.item_limit}, got {limit}.")

        request_params = self._filter_out_none_values(params) or {}

        request_params["limit"] = limit
        if cursor is not None:
            request_params["cursor"] = cursor

        request = RequestMessage2(endpoint_url=self._make_url(), method=endpoint.method, parameters=request_params)
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()
        return self._page_response(response)

    def _list(self, limit: int | None = None, params: dict[str, Any] | None = None) -> list[T_ResponseResource]:
        """List all resources, handling pagination automatically."""
        all_items: list[T_ResponseResource] = []
        next_cursor: str | None = None
        total = 0
        endpoint = self._method_endpoint_map["list"]
        while True:
            page_limit = endpoint.item_limit if limit is None else min(limit - total, endpoint.item_limit)
            page = self._iterate(limit=page_limit, cursor=next_cursor, params=params)
            all_items.extend(page.items)
            total += len(page.items)
            if page.next_cursor is None or (limit is not None and total >= limit):
                break
            next_cursor = page.next_cursor
        return all_items
