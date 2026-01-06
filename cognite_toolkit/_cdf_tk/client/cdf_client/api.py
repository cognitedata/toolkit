"""Base classes for resource clients.

This module provides the base infrastructure for resource-specific clients
that handle CRUD operations for CDF Data Modeling API resources.
"""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Generic

from pydantic import JsonValue

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


class ResourceAPI(Generic[T_Identifier, T_RequestResource, T_ResponseResource], ABC):
    """Generic resource API for CDF APIs

    This class provides the logic for working with CDF resources,
    including creating, retrieving, deleting, and listing resources.
    """

    def __init__(
        self,
        http_client: HTTPClient,
        endpoint: str,
        limits: ResourceLimits,
    ) -> None:
        """Initialize the resource API.

        Args:
            http_client: The HTTP client to use for API requests.
            endpoint: The API endpoint path for this resource (e.g., '/models/spaces').
            limits: Configuration for API endpoint limits. Uses defaults if not provided.
        """
        self._http_client = http_client
        self._endpoint = endpoint
        self._limits = limits

    def _serialize_request_resource(self, items: Sequence[T_RequestResource]) -> list[dict[str, JsonValue]]:
        """Serialize request objects to JSON-compatible dicts."""
        return [item.model_dump(mode="json", by_alias=True) for item in items]

    def _serialize_reference(self, items: Sequence[T_Identifier]) -> list[dict[str, JsonValue]]:
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

    def _make_url(self) -> str:
        """Create the full URL for this resource endpoint."""
        return self._http_client.config.create_api_url(self._endpoint)

    def _create(self, items: Sequence[T_RequestResource]) -> list[T_ResponseResource]:
        """Create or update resources.

        Args:
            items: A sequence of request objects defining the resources to create/update.

        Returns:
            A list of the created/updated resource objects.
        """
        if not items:
            return []

        all_responses: list[T_ResponseResource] = []

        for chunk in chunker_sequence(items, self._limits.create):
            request = RequestMessage2(
                endpoint_url=self._make_url(),
                method="POST",
                body_content={"items": self._serialize_request_resource(chunk)},  # type: ignore[dict-item]
            )

            result = self._http_client.request_single_retries(request)
            response = result.get_success_or_raise()
            created_items = self._page_response(response).items
            all_responses.extend(created_items)

        return all_responses

    def _retrieve(
        self,
        references: Sequence[T_Identifier],
        params: dict[str, Any] | None = None,
    ) -> list[T_ResponseResource]:
        """Retrieve specific resources by their references.

        Args:
            references: A sequence of reference objects identifying the resources to retrieve.
            params: Additional query parameters to include in the request.

        Returns:
            A list of resource objects. Resources that don't exist are not included.
        """
        if not references:
            return []

        # Convert params to the expected type
        request_params: dict[str, str | int | float | bool] | None = None
        if params:
            request_params = {k: v for k, v in params.items() if v is not None}

        all_responses: list[T_ResponseResource] = []
        for chunk in chunker_sequence(references, self._limits.retrieve):
            request = RequestMessage2(
                endpoint_url=f"{self._make_url()}/byids",
                method="POST",
                body_content={"items": self._serialize_reference(chunk)},  # type: ignore[dict-item]
                parameters=request_params,
            )

            result = self._http_client.request_single_retries(request)
            response = result.get_success_or_raise()
            all_responses.extend(self._page_response(response).items)

        return all_responses

    def _delete(self, references: Sequence[T_Identifier]) -> list[T_Identifier]:
        """Delete resources by their references.

        Args:
            references: A sequence of reference objects identifying the resources to delete.

        Returns:
            A list of references to the deleted resources.
        """
        if not references:
            return []

        all_deleted: list[T_Identifier] = []
        for chunk in chunker_sequence(references, self._limits.delete):
            request = RequestMessage2(
                endpoint_url=f"{self._make_url()}/delete",
                method="POST",
                body_content={"items": self._serialize_reference(chunk)},  # type: ignore[dict-item]
            )

            result = self._http_client.request_single_retries(request)
            response = result.get_success_or_raise()
            all_deleted.extend(self._reference_response(response).items)

        return all_deleted

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
        if not (0 < limit <= self._limits.list):
            raise ValueError(f"Limit must be between 1 and {self._limits.list}, got {limit}.")

        request_params: dict[str, str | int | float | bool] = {}

        if params:
            for key, value in params.items():
                if value is not None:
                    request_params[key] = value

        request_params["limit"] = self._limits.list
        if cursor is not None:
            request_params["cursor"] = cursor

        request = RequestMessage2(
            endpoint_url=self._make_url(),
            method="GET",
            parameters=request_params,
        )

        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()

        return self._page_response(response)

    def _list(self, limit: int | None = None, params: dict[str, Any] | None = None) -> list[T_ResponseResource]:
        """List all resources, handling pagination automatically."""
        all_items: list[T_ResponseResource] = []
        next_cursor: str | None = None
        total = 0
        while True:
            page_limit = self._limits.list if limit is None else min(limit - total, self._limits.list)
            page = self._iterate(limit=page_limit, cursor=next_cursor, params=params)
            all_items.extend(page.items)
            total += len(page.items)
            if page.next_cursor is None or (limit is not None and total >= limit):
                break
            next_cursor = page.next_cursor
        return all_items
