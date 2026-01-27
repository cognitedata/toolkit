"""Spaces API for managing CDF data modeling spaces.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Spaces/operation/ApplySpaces
"""

from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceReference, SpaceRequest, SpaceResponse


class SpacesAPI(CDFResourceAPI[SpaceReference, SpaceRequest, SpaceResponse]):
    """API for managing CDF data modeling spaces.

    Spaces use an apply/upsert pattern for create and update operations.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/models/spaces", item_limit=100),
                "retrieve": Endpoint(method="POST", path="/models/spaces/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/models/spaces/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/models/spaces", item_limit=1000),
            },
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[SpaceResponse]:
        return PagedResponse[SpaceResponse].model_validate_json(response.body)

    def apply(self, items: Sequence[SpaceRequest]) -> list[SpaceResponse]:
        """Apply (create or update) spaces in CDF.

        Args:
            items: List of SpaceRequest objects to apply.

        Returns:
            List of applied SpaceResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def retrieve(self, items: Sequence[SpaceReference]) -> list[SpaceResponse]:
        """Retrieve spaces from CDF.

        Args:
            items: List of SpaceReference objects to retrieve.

        Returns:
            List of retrieved SpaceResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def delete(self, items: Sequence[SpaceReference]) -> None:
        """Delete spaces from CDF.

        Args:
            items: List of SpaceReference objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        include_global: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[SpaceResponse]:
        """Get a page of spaces from CDF.

        Args:
            include_global: Whether to include global spaces.
            limit: Maximum number of spaces to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SpaceResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            params={"includeGlobal": include_global},
        )

    def iterate(
        self,
        include_global: bool = False,
        limit: int | None = None,
    ) -> Iterable[list[SpaceResponse]]:
        """Iterate over all spaces in CDF.

        Args:
            include_global: Whether to include global spaces.
            limit: Maximum total number of spaces to return.

        Returns:
            Iterable of lists of SpaceResponse objects.
        """
        return self._iterate(
            limit=limit,
            params={"includeGlobal": include_global},
        )

    def list(self, include_global: bool = False, limit: int | None = None) -> list[SpaceResponse]:
        """List all spaces in CDF.

        Args:
            include_global: Whether to include global spaces.
            limit: Maximum total number of spaces to return.

        Returns:
            List of SpaceResponse objects.
        """
        return self._list(limit=limit, params={"includeGlobal": include_global})
