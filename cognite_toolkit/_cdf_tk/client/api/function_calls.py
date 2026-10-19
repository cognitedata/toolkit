"""Function calls API for executing and inspecting CDF function calls.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Function-calls
"""

import builtins
from collections.abc import Iterable, Sequence
from typing import Any

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import FunctionCallFilter
from cognite_toolkit._cdf_tk.client.resource_classes.function_call import (
    FunctionCallLogEntry,
    FunctionCallLogs,
    FunctionCallResponse,
    FunctionCallResult,
)


class FunctionCallsAPI(CDFResourceAPI[FunctionCallResponse]):
    """API for calling Cognite Functions and inspecting those calls.

    Calls are scoped to a single function. Listing uses the filter endpoint, which
    also returns every call when no filter is provided.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="POST", path="/functions/{functionId}/calls/byids", item_limit=10_000),
                "list": Endpoint(method="POST", path="/functions/{functionId}/calls/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[FunctionCallResponse]:
        return PagedResponse[FunctionCallResponse].model_validate_json(response.body)

    def _calls_path(self, function_id: int, suffix: str = "") -> str:
        return f"/functions/{function_id}/calls{suffix}"

    def _list_body(self, filter: FunctionCallFilter | None) -> dict[str, Any] | None:
        if filter is None:
            return None
        dumped = filter.dump()
        if not dumped:
            return None
        return {"filter": dumped}

    def call(
        self,
        function_id: int,
        nonce: str,
        data: dict[str, JsonValue] | None = None,
    ) -> FunctionCallResponse:
        """Call a function asynchronously.

        The nonce binds a session. Its access token is passed to the function and used
        to instantiate the client inside ``handle``. Do not put secrets in ``data``.

        Args:
            function_id: ID of the function to call.
            nonce: Nonce from the Sessions API.
            data: Input passed to the function as the ``data`` argument.

        Returns:
            The created function call.
        """
        body: dict[str, JsonValue] = {"nonce": nonce}
        if data is not None:
            body["data"] = data
        request = RequestMessage(
            endpoint_url=self._make_url(f"/functions/{function_id}/call"),
            method="POST",
            body_content=body,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return FunctionCallResponse.model_validate_json(response.body)

    def retrieve(
        self,
        function_id: int,
        items: Sequence[InternalId],
        ignore_unknown_ids: bool = False,
    ) -> builtins.list[FunctionCallResponse]:
        """Retrieve function calls by ID.

        Args:
            function_id: ID of the function that owns the calls.
            items: Call IDs to retrieve. At most 10 000 IDs are sent per request.
            ignore_unknown_ids: Whether to ignore IDs that are not found.

        Returns:
            The retrieved function calls.
        """
        return self._request_item_response(
            items,
            method="retrieve",
            extra_body={"ignoreUnknownIds": ignore_unknown_ids},
            endpoint=self._calls_path(function_id, "/byids"),
        )

    def paginate(
        self,
        function_id: int,
        filter: FunctionCallFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[FunctionCallResponse]:
        """Get a page of function calls.

        Args:
            function_id: ID of the function that owns the calls.
            filter: Optional filter on status, schedule, and time range.
            limit: Maximum number of calls to return.
            cursor: Cursor for pagination.

        Returns:
            A page of function calls.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=self._list_body(filter),
            endpoint_path=self._calls_path(function_id, "/list"),
        )

    def iterate(
        self,
        function_id: int,
        filter: FunctionCallFilter | None = None,
        limit: int | None = None,
    ) -> Iterable[builtins.list[FunctionCallResponse]]:
        """Iterate over function calls.

        Args:
            function_id: ID of the function that owns the calls.
            filter: Optional filter on status, schedule, and time range.
            limit: Maximum total number of calls to return.

        Returns:
            Pages of function calls.
        """
        return self._iterate(
            limit=limit,
            body=self._list_body(filter),
            endpoint_path=self._calls_path(function_id, "/list"),
        )

    def list(
        self,
        function_id: int,
        filter: FunctionCallFilter | None = None,
        limit: int | None = None,
    ) -> builtins.list[FunctionCallResponse]:
        """List function calls.

        Args:
            function_id: ID of the function that owns the calls.
            filter: Optional filter on status, schedule, and time range.
            limit: Maximum total number of calls to return.

        Returns:
            The function calls.
        """
        return self._list(
            limit=limit,
            body=self._list_body(filter),
            endpoint_path=self._calls_path(function_id, "/list"),
        )

    def get_response(self, function_id: int, call_id: int) -> FunctionCallResult:
        """Retrieve the response from a function call.

        Args:
            function_id: ID of the function that owns the call.
            call_id: ID of the call.

        Returns:
            The call id and the value returned by the function.
        """
        request = RequestMessage(
            endpoint_url=self._make_url(self._calls_path(function_id, f"/{call_id}/response")),
            method="GET",
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return FunctionCallResult.model_validate_json(response.body)

    def get_logs(self, function_id: int, call_id: int) -> builtins.list[FunctionCallLogEntry]:
        """Retrieve logs from a function call.

        Args:
            function_id: ID of the function that owns the call.
            call_id: ID of the call.

        Returns:
            Log lines written by the function.
        """
        request = RequestMessage(
            endpoint_url=self._make_url(self._calls_path(function_id, f"/{call_id}/logs")),
            method="GET",
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return FunctionCallLogs.model_validate_json(response.body).items
