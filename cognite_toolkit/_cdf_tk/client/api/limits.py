"""Limits API for project-level usage limits.

Based on the alpha API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Limits
"""

import builtins
from collections.abc import Iterator
from urllib.parse import quote

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.limits import LimitValue

_API_VERSION = "alpha"


class LimitsAPI(CDFResourceAPI[LimitValue]):
    """API for project limit values."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "list": Endpoint(method="GET", path="/limits/values", item_limit=1000),
                "search": Endpoint(method="POST", path="/limits/values/list", item_limit=1000),
                "retrieve": Endpoint(method="GET", path="/limits/values", item_limit=1),
            },
            api_version=_API_VERSION,
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[LimitValue]:
        return PagedResponse[LimitValue].model_validate_json(response.body)

    def _get_page(self, endpoint: Endpoint, cursor: str | None, limit: int) -> PagedResponse[LimitValue]:
        parameters: dict[str, str | int | float | bool] = {"limit": limit}
        if cursor is not None:
            parameters["cursor"] = cursor
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path),
            method="GET",
            parameters=parameters,
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return self._validate_page_response(response)

    def _search_page(self, body: dict[str, JsonValue]) -> PagedResponse[LimitValue]:
        endpoint = self._method_endpoint_map["search"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path),
            method="POST",
            body_content=body,
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return self._validate_page_response(response)

    def retrieve(self, limit_id: str) -> LimitValue:
        """Fetch a single limit value by its ID.

        Args:
            limit_id: The ID of the limit, for example ``atlas.ai_requests``.

        Returns:
            The limit value.
        """
        endpoint = self._method_endpoint_map["retrieve"]
        request = RequestMessage(
            endpoint_url=self._make_url(f"{endpoint.path}/{quote(limit_id, safe='')}"),
            method="GET",
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return LimitValue.model_validate_json(response.body)

    def paginate(self, cursor: str | None = None, limit: int = 1000) -> PagedResponse[LimitValue]:
        """Fetch a single page of limit values.

        Args:
            cursor: Cursor from a previous page, if any.
            limit: Maximum number of items to return. The API maximum is 1000.

        Returns:
            A page of limit values with the next cursor, if any.
        """
        return self._get_page(self._method_endpoint_map["list"], cursor, limit)

    def list(self) -> builtins.list[LimitValue]:
        """Retrieve all limit values in the project.

        Returns:
            All limit values, across all pages.
        """
        return builtins.list(self.iterate())

    def iterate(self, limit: int = 1000) -> Iterator[LimitValue]:
        """Iterate over all limit values, following cursors across pages.

        Args:
            limit: Page size. The API maximum is 1000.

        Yields:
            Each limit value in the project.
        """
        cursor: str | None = None
        while True:
            page = self.paginate(cursor=cursor, limit=limit)
            yield from page.items
            if not page.next_cursor:
                return
            cursor = page.next_cursor

    def search(self, prefix: str, limit: int = 1000) -> builtins.list[LimitValue]:
        """Retrieve limit values whose ID starts with the given prefix.

        Args:
            prefix: The limit ID prefix to match, for example ``atlas.``.
            limit: Page size. The API maximum is 1000.

        Returns:
            All matching limit values, across all pages.
        """
        results: builtins.list[LimitValue] = []
        cursor: str | None = None
        while True:
            body: dict[str, JsonValue] = {
                "filter": {"prefix": {"property": ["limitId"], "value": prefix}},
                "limit": limit,
            }
            if cursor is not None:
                body["cursor"] = cursor
            page = self._search_page(body)
            results.extend(page.items)
            if not page.next_cursor:
                return results
            cursor = page.next_cursor
