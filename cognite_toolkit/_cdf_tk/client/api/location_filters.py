from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse2,
    RequestMessage2,
    SuccessResponse2,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.location_filter import (
    LocationFilterRequest,
    LocationFilterResponse,
)


class LocationFiltersAPI(CDFResourceAPI[ExternalId, LocationFilterRequest, LocationFilterResponse]):
    """API for managing Location Filters using the CDFResourceAPI pattern.

    This API manages location filter configurations at:
    /apps/v1/projects/{project}/storage/config/locationfilters
    """

    BASE_PATH = "/storage/config/locationfilters"

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path=self.BASE_PATH, item_limit=1, concurrency_max_workers=1),
                "retrieve": Endpoint(method="GET", path=self.BASE_PATH, item_limit=1, concurrency_max_workers=1),
                "update": Endpoint(method="PUT", path=self.BASE_PATH, item_limit=1, concurrency_max_workers=1),
                "delete": Endpoint(method="DELETE", path=self.BASE_PATH, item_limit=1, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path=f"{self.BASE_PATH}/list", item_limit=1000),
            },
        )

    def _make_url(self, path: str = "") -> str:
        """Create the full URL for this resource endpoint using the apps URL format."""
        return self._http_client.config.create_app_url(path)

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[LocationFilterResponse]:
        return PagedResponse[LocationFilterResponse].model_validate_json(response.body)

    def create(self, item: LocationFilterRequest) -> LocationFilterResponse:
        """Create a new location filter.

        Args:
            item: The location filter to create.

        Returns:
            The created location filter.
        """
        endpoint = self._method_endpoint_map["create"]
        request = RequestMessage2(
            endpoint_url=self._make_url(endpoint.path),
            method=endpoint.method,
            body_content=item.model_dump(mode="json", by_alias=True),
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()
        return LocationFilterResponse.model_validate_json(response.body)

    def retrieve(self, id: int) -> LocationFilterResponse:
        """Retrieve a single location filter by ID.

        Args:
            id: The ID of the location filter to retrieve.

        Returns:
            The retrieved location filter.
        """
        endpoint = self._method_endpoint_map["retrieve"]
        request = RequestMessage2(
            endpoint_url=self._make_url(f"{endpoint.path}/{id}"),
            method=endpoint.method,
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()
        return LocationFilterResponse.model_validate_json(response.body)

    def retrieve_multiple(self, ids: Sequence[int]) -> list[LocationFilterResponse]:
        """Retrieve multiple location filters by their IDs.

        Args:
            ids: The IDs of the location filters to retrieve.

        Returns:
            List of retrieved location filters.
        """
        return [self.retrieve(id) for id in ids]

    def update(self, id: int, item: LocationFilterRequest) -> LocationFilterResponse:
        """Update an existing location filter.

        Args:
            id: The ID of the location filter to update.
            item: The updated location filter data.

        Returns:
            The updated location filter.
        """
        endpoint = self._method_endpoint_map["update"]
        request = RequestMessage2(
            endpoint_url=self._make_url(f"{endpoint.path}/{id}"),
            method=endpoint.method,
            body_content=item.model_dump(mode="json", by_alias=True),
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()
        return LocationFilterResponse.model_validate_json(response.body)

    def delete(self, id: int) -> None:
        """Delete a location filter.

        Args:
            id: The ID of the location filter to delete.
        """
        endpoint = self._method_endpoint_map["delete"]
        request = RequestMessage2(
            endpoint_url=self._make_url(f"{endpoint.path}/{id}"),
            method=endpoint.method,
        )
        result = self._http_client.request_single_retries(request)
        result.get_success_or_raise()

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
        flat: bool = True,
    ) -> PagedResponse[LocationFilterResponse]:
        """Get a single page of location filters.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.
            flat: Whether to return a flat list (default True).

        Returns:
            PagedResponse of LocationFilterResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit, body={"flat": flat})

    def iterate(
        self,
        limit: int | None = None,
        flat: bool = True,
    ) -> Iterable[list[LocationFilterResponse]]:
        """Iterate over all location filters.

        Args:
            limit: Maximum total number of items to return.
            flat: Whether to return a flat list (default True).

        Returns:
            Iterable of lists of LocationFilterResponse objects.
        """
        return self._iterate(limit=limit, body={"flat": flat})

    def list(self, limit: int | None = None, flat: bool = True) -> list[LocationFilterResponse]:
        """List all location filters.

        Args:
            limit: Maximum number of items to return.
            flat: Whether to return a flat list (default True).

        Returns:
            List of LocationFilterResponse objects.
        """
        return self._list(limit=limit, body={"flat": flat})
