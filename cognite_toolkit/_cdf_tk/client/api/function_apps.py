"""Function apps API for reading Cognite Functions that serve HTTP endpoints.

Based on the API specification at:
https://api-docs.cognite.com/20230101-beta/tag/Function-Apps

Note: This is a beta API and may change in future releases.
"""

import builtins
from collections.abc import Iterable, Sequence
from urllib.parse import quote

from cognite_toolkit._cdf_tk.client.api.function_app_calls import FunctionAppCallsAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    FailedResponse,
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.function_app import FunctionAppResponse


class FunctionAppsAPI(CDFResourceAPI[FunctionAppResponse]):
    """API for reading function apps.

    The list endpoint has no cursor, so 1000 is the most functions ``list(limit=None)`` can return.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="GET", path="/function-apps/{functionExternalId}", item_limit=1),
                "list": Endpoint(method="GET", path="/function-apps", item_limit=1000),
            },
            api_version="beta",
        )
        self.calls = FunctionAppCallsAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[FunctionAppResponse]:
        return PagedResponse[FunctionAppResponse].model_validate_json(response.body)

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[FunctionAppResponse]:
        """Retrieve function apps by external ID. Each function is retrieved with its own request.

        Args:
            items: External IDs of the functions to retrieve.
            ignore_unknown_ids: Whether to skip functions that are not found.

        Returns:
            The retrieved functions.
        """
        results: builtins.list[FunctionAppResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(f"/function-apps/{quote(item.external_id, safe='')}"),
                method="GET",
                api_version=self._api_version,
            )
            response = self._http_client.request_single_retries(request)
            if isinstance(response, SuccessResponse):
                results.append(FunctionAppResponse.model_validate_json(response.body))
            elif ignore_unknown_ids and isinstance(response, FailedResponse) and response.status_code == 404:
                continue
            else:
                response.get_success_or_raise(request)
        return results

    def paginate(self, limit: int = 100) -> PagedResponse[FunctionAppResponse]:
        """Get a page of function apps. The endpoint has no cursor, so there is never a next page.

        Args:
            limit: Maximum number of functions to return.

        Returns:
            A page of functions.
        """
        return self._paginate(limit=limit)

    def iterate(self, limit: int | None = None) -> Iterable[builtins.list[FunctionAppResponse]]:
        """Iterate over function apps.

        Args:
            limit: Maximum number of functions to return. None returns at most 1000.

        Returns:
            Batches of functions.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> builtins.list[FunctionAppResponse]:
        """List function apps.

        Args:
            limit: Maximum number of functions to return. None returns at most 1000.

        Returns:
            The functions.
        """
        return self._list(limit=limit)
