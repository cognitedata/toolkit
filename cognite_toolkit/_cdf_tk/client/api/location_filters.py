from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.location_filter import (
    LocationFilterRequest,
    LocationFilterResponse,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence


class LocationFiltersAPI(CDFResourceAPI[InternalId, LocationFilterRequest, LocationFilterResponse]):
    """API for managing Location Filters using the CDFResourceAPI pattern.

    This API manages location filter configurations at:
    /apps/v1/projects/{project}/storage/config/locationfilters
    """

    BASE_PATH = "/storage/config/locationfilters"

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path=self.BASE_PATH, item_limit=1),
                "retrieve": Endpoint(method="POST", path=f"{self.BASE_PATH}/byids", item_limit=1000),
                "update": Endpoint(method="PUT", path=f"{self.BASE_PATH}/{{id}}", item_limit=1),
                "delete": Endpoint(method="DELETE", path=f"{self.BASE_PATH}/{{id}}", item_limit=1),
                "list": Endpoint(method="POST", path=f"{self.BASE_PATH}/list", item_limit=1000),
            },
        )

    def _make_url(self, path: str = "") -> str:
        """Create the full URL for this resource endpoint using the apps URL format."""
        return self._http_client.config.create_app_url(path)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[LocationFilterResponse]:
        return PagedResponse[LocationFilterResponse].model_validate_json(response.body)

    def create(self, items: Sequence[LocationFilterRequest]) -> list[LocationFilterResponse]:
        """Create a new location filter.

        Args:
            items: The location filter to create.

        Returns:
            The created location filter.
        """
        endpoint = self._method_endpoint_map["create"]
        results: list[LocationFilterResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                body_content=item.model_dump(mode="json", by_alias=True),
            )
            result = self._http_client.request_single_retries(request)
            response = result.get_success_or_raise()
            results.append(LocationFilterResponse.model_validate_json(response.body))
        return results

    def retrieve(self, items: Sequence[InternalId]) -> list[LocationFilterResponse]:
        """Retrieve a single location filter by ID.

        Args:
            items: The ID of the location filter to retrieve.

        Returns:
            The retrieved location filter.
        """
        endpoint = self._method_endpoint_map["retrieve"]
        results: list[LocationFilterResponse] = []
        for chunk in chunker_sequence(items, endpoint.item_limit):
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                body_content={"ids": [item.id for item in chunk]},
            )
            result = self._http_client.request_single_retries(request)
            response = result.get_success_or_raise()
            results.extend(self._validate_page_response(response).items)
        return results

    def update(self, items: Sequence[LocationFilterRequest]) -> list[LocationFilterResponse]:
        """Update an existing location filter.

        Args:
            items: The updated location filter data.

        Returns:
            The updated location filter.
        """
        endpoint = self._method_endpoint_map["update"]
        results: list[LocationFilterResponse] = []
        for item in items:
            if item.id is None:
                raise ValueError("Item must have an ID for update operation.")
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path.format(id=item.id)),
                method=endpoint.method,
                body_content=item.model_dump(mode="json", by_alias=True),
            )
            result = self._http_client.request_single_retries(request)
            response = result.get_success_or_raise()
            parsed = LocationFilterResponse.model_validate_json(response.body)
            parsed.id = item.id
            results.append(parsed)
        return results

    def delete(self, items: Sequence[InternalId]) -> list[LocationFilterResponse]:
        """Delete a location filter.

        Args:
            items: The ID of the location filter to delete.

        Returns:
            list[LocationFilterResponse]: The deleted location filters.
        """
        endpoint = self._method_endpoint_map["delete"]
        results: list[LocationFilterResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path.format(id=item.id)),
                method=endpoint.method,
            )
            result = self._http_client.request_single_retries(request)
            response = result.get_success_or_raise()
            results.append(LocationFilterResponse.model_validate_json(response.body))
        return results

    def paginate(
        self,
        flat: bool = True,
    ) -> PagedResponse[LocationFilterResponse]:
        """Get a single page of location filters.

        Args:
            flat: Whether to return a flat list (default True).

        Returns:
            PagedResponse of LocationFilterResponse objects.
        """
        return self._paginate(cursor=None, limit=100, body={"flat": flat})

    def iterate(
        self,
        flat: bool = True,
    ) -> Iterable[list[LocationFilterResponse]]:
        """Iterate over all location filters.

        Args:
            flat: Whether to return a flat list (default True).

        Returns:
            Iterable of lists of LocationFilterResponse objects.
        """
        return self._iterate(limit=None, body={"flat": flat})

    def list(self, flat: bool = True) -> list[LocationFilterResponse]:
        """List all location filters.

        Args:
            flat: Whether to return a flat list (default True).

        Returns:
            List of LocationFilterResponse objects.
        """
        return self._list(limit=None, body={"flat": flat})
