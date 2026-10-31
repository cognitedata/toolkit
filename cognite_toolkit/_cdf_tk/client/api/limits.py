"""Limits API for project-level limit values.

Based on the alpha API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Limits
"""

import builtins
from collections.abc import Iterable, Sequence
from typing import Any
from urllib.parse import quote

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    FailedResponse,
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import LimitId
from cognite_toolkit._cdf_tk.client.resource_classes.limits import LimitValueResponse


class LimitsAPI(CDFResourceAPI[LimitValueResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                # The API only supports retrieving one limit at a time: GET /limits/values/{limitId}.
                "retrieve": Endpoint(method="GET", path="/limits/values/{limitId}", item_limit=1),
                "list": Endpoint(method="POST", path="/limits/values/list", item_limit=1000),
            },
            api_version="alpha",
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[LimitValueResponse]:
        return PagedResponse[LimitValueResponse].model_validate_json(response.body)

    def retrieve(self, items: Sequence[LimitId], ignore_unknown_ids: bool = False) -> builtins.list[LimitValueResponse]:
        """Retrieve limit values by ID.

        Args:
            items: The limit IDs to retrieve, e.g. ``LimitId(limit_id="atlas.monthly_ai_tokens")``.
            ignore_unknown_ids: Skip limits that do not exist. When False, a missing limit raises.

        Returns:
            The retrieved limit values.
        """
        endpoint = self._method_endpoint_map["retrieve"]
        results: builtins.list[LimitValueResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path.format(limitId=quote(item.limit_id, safe=""))),
                method=endpoint.method,
                api_version=self._api_version,
            )
            response = self._http_client.request_single_retries(request)
            if isinstance(response, SuccessResponse):
                results.append(LimitValueResponse.model_validate_json(response.body))
            elif (
                ignore_unknown_ids
                and isinstance(response, FailedResponse)
                and 400 <= response.status_code < 500
                and response.status_code != 429
            ):
                continue
            else:
                _ = response.get_success_or_raise(request)
        return results

    @staticmethod
    def _create_body(prefix: str | None) -> dict[str, Any] | None:
        if prefix is None:
            return None
        return {"filter": {"prefix": {"property": ["limitId"], "value": prefix}}}

    def paginate(
        self, prefix: str | None = None, limit: int = 1000, cursor: str | None = None
    ) -> PagedResponse[LimitValueResponse]:
        """Fetch a single page of limit values.

        Args:
            prefix: Only return limits whose ID starts with this prefix, e.g. ``atlas.`` for all atlas limits.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            A page of limit values.
        """
        return self._paginate(limit=limit, cursor=cursor, body=self._create_body(prefix))

    def iterate(
        self, prefix: str | None = None, limit: int | None = None
    ) -> Iterable[builtins.list[LimitValueResponse]]:
        """Iterate over limit values.

        Args:
            prefix: Only return limits whose ID starts with this prefix, e.g. ``atlas.`` for all atlas limits.
            limit: Maximum number of items to return. None returns all.

        Returns:
            Iterable of batches of limit values.
        """
        return self._iterate(limit=limit, body=self._create_body(prefix))

    def list(self, prefix: str | None = None, limit: int | None = None) -> builtins.list[LimitValueResponse]:
        """List limit values.

        Args:
            prefix: Only return limits whose ID starts with this prefix, e.g. ``atlas.`` for all atlas limits.
            limit: Maximum number of items to return. None returns all.

        Returns:
            The limit values.
        """
        return self._list(limit=limit, body=self._create_body(prefix))
