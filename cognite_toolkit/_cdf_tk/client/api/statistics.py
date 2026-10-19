"""Statistics API for data modeling usage and limits.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Statistics
"""

import builtins
from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.statistics import (
    ProjectStatisticsResponse,
    SpaceStatisticsResponse,
)


class SpaceStatisticsAPI(CDFResourceAPI[SpaceStatisticsResponse]):
    """API for data modeling statistics grouped by space."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                # The list endpoint returns every space in one response and does not paginate.
                "list": Endpoint(method="GET", path="/models/statistics/spaces", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/models/statistics/spaces/byids", item_limit=100),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SpaceStatisticsResponse]:
        return PagedResponse[SpaceStatisticsResponse].model_validate_json(response.body)

    def retrieve(self, items: Sequence[SpaceId]) -> builtins.list[SpaceStatisticsResponse]:
        """Retrieve statistics for specific spaces.

        Args:
            items: Spaces to retrieve statistics for. At most 100 spaces are sent per request.

        Returns:
            Statistics for the requested spaces.
        """
        return self._request_item_response(items, "retrieve")

    def list(self) -> builtins.list[SpaceStatisticsResponse]:
        """Retrieve statistics for every space in the project.

        Returns:
            Statistics for each space.
        """
        endpoint = self._method_endpoint_map["list"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path),
            method=endpoint.method,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return self._validate_page_response(response).items


class StatisticsAPI(CDFResourceAPI[ProjectStatisticsResponse]):
    """API for project-wide and per-space data modeling statistics and limits."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="GET", path="/models/statistics", item_limit=1),
            },
        )
        self.spaces = SpaceStatisticsAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ProjectStatisticsResponse]:
        raise NotImplementedError("Project statistics is a single object, not a page of items.")

    def retrieve(self) -> ProjectStatisticsResponse:
        """Retrieve project-wide statistics and limits.

        Returns statistics and limits for data modeling resources across the project.

        Returns:
            Project statistics and limits.
        """
        endpoint = self._method_endpoint_map["retrieve"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path),
            method=endpoint.method,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return ProjectStatisticsResponse.model_validate_json(response.body)
