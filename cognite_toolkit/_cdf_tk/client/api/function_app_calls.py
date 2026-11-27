"""Function app calls API for calling function app endpoints and inspecting the calls.

Based on the API specification at:
https://api-docs.cognite.com/20230101-beta/tag/Function-Apps

Note: This is a beta API and may change in future releases.
"""

import builtins
from collections.abc import Iterable
from urllib.parse import quote

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_app import (
    FunctionAppCallPayload,
    FunctionAppCallRuntimeResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.function_call import (
    FunctionCallLogEntry,
    FunctionCallLogs,
    FunctionCallResponse,
    FunctionCallResult,
)


class FunctionAppCallsAPI(CDFResourceAPI[FunctionCallResponse]):
    """API for calling function apps and inspecting those calls.

    The list endpoint has no limit or cursor. It returns every call, and ``limit`` is applied client side.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/function-apps/{functionExternalId}/calls", item_limit=1),
                "list": Endpoint(method="GET", path="/function-apps/{functionExternalId}/calls", item_limit=1000),
            },
            api_version="beta",
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[FunctionCallResponse]:
        return PagedResponse[FunctionCallResponse].model_validate_json(response.body)

    @staticmethod
    def _calls_path(function_external_id: str, suffix: str = "") -> str:
        return f"/function-apps/{quote(function_external_id, safe='')}/calls{suffix}"

    def _get(self, path: str) -> SuccessResponse:
        request = RequestMessage(endpoint_url=self._make_url(path), method="GET", api_version=self._api_version)
        return self._http_client.request_single_retries(request).get_success_or_raise(request)

    def create(self, data: FunctionAppCallPayload, nonce: str) -> FunctionAppCallRuntimeResponse:
        """Call a function app endpoint synchronously.

        The nonce binds a session. Its access token is passed to the function. Do not put secrets in ``data``.

        Args:
            data: The function app external ID, and the path, HTTP method, and body forwarded to the function app.
            nonce: Nonce from the Sessions API.

        Returns:
            The runtime status, and the response envelope of the function app if the call completed.
        """
        request = RequestMessage(
            endpoint_url=self._make_url(self._calls_path(data.function_external_id)),
            method="POST",
            body_content={"data": data.dump(), "nonce": nonce},
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return FunctionAppCallRuntimeResponse.model_validate_json(response.body)

    def paginate(self, function_external_id: str, limit: int = 100) -> PagedResponse[FunctionCallResponse]:
        """Get the calls of a function app. The endpoint has no cursor, so there is never a next page.

        Args:
            function_external_id: External ID of the function app.
            limit: Maximum number of calls to return.

        Returns:
            A page of function calls.
        """
        return PagedResponse[FunctionCallResponse](items=self.list(function_external_id, limit=limit))

    def iterate(
        self, function_external_id: str, limit: int | None = None
    ) -> Iterable[builtins.list[FunctionCallResponse]]:
        """Iterate over the calls of a function app. All calls are returned in a single batch.

        Args:
            function_external_id: External ID of the function app.
            limit: Maximum number of calls to return. None returns all.

        Returns:
            Batches of function calls.
        """
        yield self.list(function_external_id, limit=limit)

    def list(self, function_external_id: str, limit: int | None = None) -> builtins.list[FunctionCallResponse]:
        """List the calls of a function app.

        Args:
            function_external_id: External ID of the function app.
            limit: Maximum number of calls to return. None returns all.

        Returns:
            The function calls.
        """
        return self._collect_pages(
            PagedResponse[FunctionCallResponse],
            self._calls_path(function_external_id),
            "GET",
            limit=limit,
            page_size=None,
        )

    def logs(self, function_external_id: str, call_id: int) -> builtins.list[FunctionCallLogEntry]:
        """Retrieve the logs of a function app call.

        Args:
            function_external_id: External ID of the function app.
            call_id: ID of the call.

        Returns:
            Log lines written by the function.
        """
        response = self._get(self._calls_path(function_external_id, f"/{call_id}/logs"))
        return FunctionCallLogs.model_validate_json(response.body).items

    def response(self, function_external_id: str, call_id: int) -> FunctionCallResult:
        """Retrieve the response of a completed function app call.

        Args:
            function_external_id: External ID of the function app.
            call_id: ID of the call.

        Returns:
            The call id and the value returned by the function.
        """
        response = self._get(self._calls_path(function_external_id, f"/{call_id}/response"))
        return FunctionCallResult.model_validate_json(response.body)
