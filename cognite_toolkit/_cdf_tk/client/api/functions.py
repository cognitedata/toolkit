"""Functions API for managing CDF functions.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Functions/operation/postFunctions
"""

import builtins
from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.api.function_calls import FunctionCallsAPI
from cognite_toolkit._cdf_tk.client.api.function_schedules import FunctionSchedulesAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import FunctionFilter
from cognite_toolkit._cdf_tk.client.resource_classes.function import (
    FunctionLimits,
    FunctionRequest,
    FunctionResponse,
    FunctionsActivation,
)


class FunctionsAPI(CDFResourceAPI[FunctionResponse]):
    """API for managing CDF functions.

    Note: Functions do not support update operations.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/functions", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/functions/byids", item_limit=10),
                "delete": Endpoint(method="POST", path="/functions/delete", item_limit=10),
                # The list endpoint has no cursor, so this is the most functions list(limit=None) can return.
                "list": Endpoint(method="POST", path="/functions/list", item_limit=10_000),
            },
        )
        self.schedules = FunctionSchedulesAPI(http_client)
        self.calls = FunctionCallsAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[FunctionResponse]:
        return PagedResponse[FunctionResponse].model_validate_json(response.body)

    def create(self, items: Sequence[FunctionRequest]) -> builtins.list[FunctionResponse]:
        """Create functions in CDF.

        Args:
            items: List of FunctionRequest objects to create.

        Returns:
            List of created FunctionResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[FunctionResponse]:
        """Retrieve functions from CDF by ID.

        Args:
            items: List of ExternalId or InternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of retrieved FunctionResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete functions from CDF.

        Args:
            items: List of ExternalId or InternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    @staticmethod
    def _list_body(filter: FunctionFilter | None) -> dict[str, Any] | None:
        if filter is None:
            return None
        dumped = filter.dump()
        return {"filter": dumped} if dumped else None

    def paginate(
        self,
        filter: FunctionFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[FunctionResponse]:
        """Get a page of functions from CDF.

        Args:
            filter: Optional filter on name, owner, status, and more.
            limit: Maximum number of functions to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of FunctionResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit, body=self._list_body(filter))

    def iterate(
        self,
        filter: FunctionFilter | None = None,
        limit: int | None = None,
    ) -> Iterable[builtins.list[FunctionResponse]]:
        """Iterate over functions in CDF.

        Args:
            filter: Optional filter on name, owner, status, and more.
            limit: Maximum total number of functions to return.

        Returns:
            Iterable of lists of FunctionResponse objects.
        """
        return self._iterate(limit=limit, body=self._list_body(filter))

    def list(self, filter: FunctionFilter | None = None, limit: int | None = None) -> builtins.list[FunctionResponse]:
        """List functions in CDF.

        Args:
            filter: Optional filter on name, owner, status, and more.
            limit: Maximum total number of functions to return.

        Returns:
            List of FunctionResponse objects.
        """
        return self._list(limit=limit, body=self._list_body(filter))

    def limits(self) -> FunctionLimits:
        """Get function limits for the CDF project.

        Returns:
            FunctionLimits object containing the service limits.
        """
        request = RequestMessage(
            endpoint_url=self._make_url("/functions/limits"),
            method="GET",
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return FunctionLimits.model_validate_json(response.body)

    def _activation_status(self, method: Literal["GET", "POST"]) -> FunctionsActivation:
        request = RequestMessage(
            endpoint_url=self._make_url("/functions/status"),
            method=method,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return FunctionsActivation.model_validate_json(response.body)

    def activate(self) -> FunctionsActivation:
        """Activate Cognite Functions for the project.

        This creates the backend infrastructure required to run functions.

        Returns:
            The activation status after the request is accepted.
        """
        return self._activation_status("POST")

    def status(self) -> FunctionsActivation:
        """Get the Cognite Functions activation status for the project.

        Returns:
            Whether Functions are inactive, requested, or activated.
        """
        return self._activation_status("GET")
