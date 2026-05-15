"""AppsAPI: Custom apps deployed via the CDF App Hosting API."""

from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.http_client._data_classes import FailedResponse
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse


class AppsAPI(CDFResourceAPI[AppResponse]):
    """Client for the CDF App Hosting API (/apphosting/apps)."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/apphosting/apps", item_limit=1),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[AppResponse]:
        return PagedResponse[AppResponse].model_validate_json(response.body)

    def create(self, items: Sequence[AppRequest]) -> list[AppResponse]:
        """POST /apphosting/apps — create apps."""
        return self._request_item_response(items, "create")

    def retrieve(self, external_id: str) -> AppResponse | None:
        """GET /apphosting/apps/{externalId} — fetch app-level metadata (name, description)."""
        request = RequestMessage(
            endpoint_url=self._make_url(f"/apphosting/apps/{external_id}"),
            method="GET",
        )
        result = self._http_client.request_single_retries(request)
        if isinstance(result, FailedResponse) and result.status_code == 404:
            return None
        return AppResponse.model_validate_json(result.get_success_or_raise(request).body)
