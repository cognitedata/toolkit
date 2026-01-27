from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.securitycategory import (
    SecurityCategoryRequest,
    SecurityCategoryResponse,
)


class SecurityCategoriesAPI(CDFResourceAPI[InternalId, SecurityCategoryRequest, SecurityCategoryResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/securitycategories", item_limit=1000),
                "delete": Endpoint(method="POST", path="/securitycategories/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/securitycategories", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SecurityCategoryResponse]:
        return PagedResponse[SecurityCategoryResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalId]:
        return ResponseItems[InternalId].model_validate_json(response.body)

    def create(self, items: Sequence[SecurityCategoryRequest]) -> list[SecurityCategoryResponse]:
        """Create security categories in CDF.

        Args:
            items: List of SecurityCategoryRequest objects to create.
        Returns:
            List of created SecurityCategoryResponse objects.
        """
        return self._request_item_response(items, "create")

    def delete(self, items: Sequence[InternalId]) -> None:
        """Delete security categories from CDF.

        Args:
            items: List of InternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        sort: Literal["ASC", "DESC"] = "ASC",
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[SecurityCategoryResponse]:
        """Iterate over all security categories in CDF.

        Args:
            sort: Sort descending or ascending.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SecurityCategoryResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit, params={"sort": sort})

    def iterate(
        self,
        sort: Literal["ASC", "DESC"] = "ASC",
        limit: int = 100,
    ) -> Iterable[list[SecurityCategoryResponse]]:
        """Iterate over all security categories in CDF.

        Args:
            sort: Sort descending or ascending.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of SecurityCategoryResponse objects.
        """
        return self._iterate(limit=limit, params={"sort": sort})

    def list(
        self,
        limit: int | None = 100,
    ) -> list[SecurityCategoryResponse]:
        """List all security categories in CDF.

        Returns:
            List of SecurityCategoryResponse objects.
        """
        return self._list(limit=limit)
