"""Functions API for managing CDF functions.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Functions/operation/postFunctions
"""

from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse2, SuccessResponse2
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionRequest, FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId


class FunctionsAPI(CDFResourceAPI[InternalId, FunctionRequest, FunctionResponse]):
    """API for managing CDF functions.

    Note: Functions do not support update operations.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/functions", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/functions/byids", item_limit=1000),
                "delete": Endpoint(method="POST", path="/functions/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/functions", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[FunctionResponse]:
        return PagedResponse[FunctionResponse].model_validate_json(response.body)

    def create(self, items: Sequence[FunctionRequest]) -> list[FunctionResponse]:
        """Create functions in CDF.

        Args:
            items: List of FunctionRequest objects to create.

        Returns:
            List of created FunctionResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[InternalId]) -> list[FunctionResponse]:
        """Retrieve functions from CDF by ID.

        Args:
            items: List of InternalId objects to retrieve.

        Returns:
            List of retrieved FunctionResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def delete(self, items: Sequence[InternalId]) -> None:
        """Delete functions from CDF.

        Args:
            items: List of InternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[FunctionResponse]:
        """Get a page of functions from CDF.

        Args:
            limit: Maximum number of functions to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of FunctionResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
        )

    def iterate(
        self,
        limit: int | None = None,
    ) -> Iterable[list[FunctionResponse]]:
        """Iterate over all functions in CDF.

        Args:
            limit: Maximum total number of functions to return.

        Returns:
            Iterable of lists of FunctionResponse objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> list[FunctionResponse]:
        """List all functions in CDF.

        Args:
            limit: Maximum total number of functions to return.

        Returns:
            List of FunctionResponse objects.
        """
        return self._list(limit=limit)
