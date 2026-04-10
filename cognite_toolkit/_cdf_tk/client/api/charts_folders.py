from collections.abc import Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.chart_folder import (
    ChartFolderRequest,
    ChartFolderResponse,
)


class ChartFoldersAPI(CDFResourceAPI[ChartFolderResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/charts/monitoring/folders", item_limit=1000),
                "list": Endpoint(method="GET", path="/charts/monitoring/folders", item_limit=1000),
            },
        )

    def _make_url(self, path: str = "") -> str:
        return self._http_client.config.create_app_url(path)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ChartFolderResponse]:
        return PagedResponse[ChartFolderResponse].model_validate_json(response.body)

    def create(self, items: Sequence[ChartFolderRequest]) -> list[ChartFolderResponse]:
        """Create chart folders in CDF.

        Args:
            items: Chart folder request objects to create.
        Returns:
            Created chart folder response objects.
        """
        return self._request_item_response(items, "create")

    def list(self) -> list[ChartFolderResponse]:
        """List charts folders in CDF.

        Returns:
            List of chart folder response objects.
        """
        endpoint = self._method_endpoint_map["list"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path),
            method=endpoint.method,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return self._validate_page_response(response).items
