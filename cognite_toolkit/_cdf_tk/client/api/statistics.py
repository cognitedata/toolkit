"""Statistics API for project-wide data modeling usage and limits.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Statistics/operation/getStatistics
"""

from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.statistics import ProjectStatistics


class StatisticsAPI(CDFResourceAPI[ProjectStatistics]):
    """API for project-wide data modeling statistics and limits."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="GET", path="/models/statistics", item_limit=1),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ProjectStatistics]:
        raise NotImplementedError("Project statistics is a single object, not a page of items.")

    def retrieve(self) -> ProjectStatistics:
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
        return ProjectStatistics.model_validate_json(response.body)
