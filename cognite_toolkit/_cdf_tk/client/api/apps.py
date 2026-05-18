"""AppsAPI: Custom apps deployed via the CDF App Hosting API."""

from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.api.app_versions import AppVersionsAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.http_client._data_classes import FailedResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse


class AppsAPI(CDFResourceAPI[AppResponse]):
    """Client for the CDF App Hosting API (/apphosting/apps)."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/apphosting/apps", item_limit=1),
                "retrieve": Endpoint(method="GET", path="/apphosting/apps/{externalId}", item_limit=1),
            },
        )
        self.versions = AppVersionsAPI(http_client)

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[AppResponse]:
        return PagedResponse[AppResponse].model_validate_json(response.body)

    def create(self, items: Sequence[AppRequest]) -> list[AppResponse]:
        """POST /apphosting/apps — create apps."""
        return self._request_item_response(items, "create")

    def retrieve(self, external_ids: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[AppResponse]:
        """GET /apphosting/apps/{externalId} — fetch app-level metadata (name, description)."""
        results: list[AppResponse] = []
        endpoint = self._method_endpoint_map["retrieve"]
        for external_id in external_ids:
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path.format(externalId=external_id.external_id)),
                method=endpoint.method,
            )
            result = self._http_client.request_single_retries(request)
            if isinstance(result, SuccessResponse):
                results.append(AppResponse.model_validate_json(result.body))
            elif isinstance(result, FailedResponse) and result.status_code == 404 and ignore_unknown_ids:
                continue
            else:
                result.get_success_or_raise(request)
        return results
