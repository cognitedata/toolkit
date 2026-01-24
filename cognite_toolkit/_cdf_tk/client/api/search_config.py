from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.search_config import (
    SearchConfigRequest,
    SearchConfigResponse,
    SearchConfigViewId,
)


class SearchConfigurationsAPI(CDFResourceAPI[SearchConfigViewId, SearchConfigRequest, SearchConfigResponse]):
    """API for managing Search Configurations using the CDFResourceAPI pattern.

    This API manages search view configurations at:
    /apps/v1/projects/{project}/storage/config/apps/search/views
    """

    BASE_PATH = "/storage/config/apps/search/views"

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path=f"{self.BASE_PATH}/upsert", item_limit=1),
                "list": Endpoint(method="POST", path=f"{self.BASE_PATH}/list", item_limit=1000),
            },
        )

    def _make_url(self, path: str = "") -> str:
        """Create the full URL for this resource endpoint using the apps URL format."""
        return self._http_client.config.create_app_url(path)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SearchConfigResponse]:
        return PagedResponse[SearchConfigResponse].model_validate_json(response.body)

    def create(self, items: Sequence[SearchConfigRequest]) -> list[SearchConfigResponse]:
        """Create or update a search configurations.

        Args:
            items: The search configuration to create or update.

        Returns:
            The created or updated search configurations.
        """
        endpoint = self._method_endpoint_map["upsert"]
        results: list[SearchConfigResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                body_content=item.model_dump(mode="json", by_alias=True),
            )
            result = self._http_client.request_single_retries(request)
            response = result.get_success_or_raise()
            results.append(SearchConfigResponse.model_validate_json(response.body))
        return results

    def update(self, items: Sequence[SearchConfigRequest]) -> list[SearchConfigResponse]:
        """Update a search configurations.

        Args:
            items: The search configuration to update.

        Returns:
            The updated search configurations.
        """
        return self.create(items)

    def paginate(self) -> PagedResponse[SearchConfigResponse]:
        """Get a single page of search configurations.

        Returns:
            PagedResponse of SearchConfigResponse objects.
        """
        return self._paginate(cursor=None, limit=100)

    def iterate(self) -> Iterable[list[SearchConfigResponse]]:
        """Iterate over all search configurations.

        Returns:
            Iterable of lists of SearchConfigResponse objects.
        """
        return self._iterate(limit=None)

    def list(self) -> list[SearchConfigResponse]:
        """List all search configurations.


        Returns:
            List of SearchConfigResponse objects.
        """
        return self._list(limit=None)
