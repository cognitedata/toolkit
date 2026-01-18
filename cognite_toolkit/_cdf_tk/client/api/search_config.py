from collections.abc import Iterable

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse2,
    RequestMessage2,
    SuccessResponse2,
)
from cognite_toolkit._cdf_tk.client.resource_classes.search_config_resource import (
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
                "upsert": Endpoint(
                    method="POST", path=f"{self.BASE_PATH}/upsert", item_limit=1, concurrency_max_workers=1
                ),
                "list": Endpoint(method="POST", path=f"{self.BASE_PATH}/list", item_limit=1000),
            },
        )

    def _make_url(self, path: str = "") -> str:
        """Create the full URL for this resource endpoint using the apps URL format."""
        return self._http_client.config.create_app_url(path)

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[SearchConfigResponse]:
        return PagedResponse[SearchConfigResponse].model_validate_json(response.body)

    def upsert(self, item: SearchConfigRequest) -> SearchConfigResponse:
        """Create or update a search configuration.

        Args:
            item: The search configuration to create or update.

        Returns:
            The created or updated search configuration.
        """
        endpoint = self._method_endpoint_map["upsert"]
        request = RequestMessage2(
            endpoint_url=self._make_url(endpoint.path),
            method=endpoint.method,
            body_content=item.model_dump(mode="json", by_alias=True),
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()
        return SearchConfigResponse.model_validate_json(response.body)

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[SearchConfigResponse]:
        """Get a single page of search configurations.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SearchConfigResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(
        self,
        limit: int | None = None,
    ) -> Iterable[list[SearchConfigResponse]]:
        """Iterate over all search configurations.

        Args:
            limit: Maximum total number of items to return.

        Returns:
            Iterable of lists of SearchConfigResponse objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> list[SearchConfigResponse]:
        """List all search configurations.

        Args:
            limit: Maximum number of items to return.

        Returns:
            List of SearchConfigResponse objects.
        """
        return self._list(limit=limit)
