"""Views API for managing CDF data modeling views.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Views/operation/ApplyViews
"""

from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import ViewFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ViewReference,
    ViewRequest,
    ViewResponse,
)


class ViewsAPI(CDFResourceAPI[ViewReference, ViewRequest, ViewResponse]):
    """API for managing CDF data modeling views.

    Views use an apply/upsert pattern for create and update operations.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/models/views", item_limit=100),
                "retrieve": Endpoint(method="POST", path="/models/views/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/models/views/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/models/views", item_limit=1000),
            },
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[ViewResponse]:
        return PagedResponse[ViewResponse].model_validate_json(response.body)

    def create(self, items: Sequence[ViewRequest]) -> list[ViewResponse]:
        """Apply (create or update) views in CDF.

        Args:
            items: List of ViewRequest objects to apply.

        Returns:
            List of applied ViewResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def update(self, items: Sequence[ViewRequest]) -> list[ViewResponse]:
        """Apply (create or update) views in CDF.

        Args:
            items: List of ViewRequest objects to apply.
        Returns:
            List of applied ViewResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def retrieve(self, items: Sequence[ViewReference], include_inherited_properties: bool = True) -> list[ViewResponse]:
        """Retrieve views from CDF.

        Args:
            items: List of ViewReference objects to retrieve.
            include_inherited_properties: Whether to include inherited properties.

        Returns:
            List of retrieved ViewResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"includeInheritedProperties": include_inherited_properties}
        )

    def delete(self, items: Sequence[ViewReference]) -> None:
        """Delete views from CDF.

        Args:
            items: List of ViewReference objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        filter: ViewFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ViewResponse]:
        """Get a page of views from CDF.

        Args:
            filter: ViewFilter to filter views.
            limit: Maximum number of views to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of ViewResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            params=filter.dump() if filter else None,
        )

    def iterate(
        self,
        filter: ViewFilter | None = None,
        limit: int | None = None,
    ) -> Iterable[list[ViewResponse]]:
        """Iterate over all views in CDF.

        Args:
            filter: ViewFilter to filter views.
            limit: Maximum total number of views to return.

        Returns:
            Iterable of lists of ViewResponse objects.
        """
        return self._iterate(
            limit=limit,
            params=filter.dump() if filter else None,
        )

    def list(
        self,
        filter: ViewFilter | None = None,
        limit: int | None = None,
    ) -> list[ViewResponse]:
        """List all views in CDF.

        Args:
            filter: ViewFilter to filter views.
            limit: Maximum total number of views to return.

        Returns:
            List of ViewResponse objects.
        """
        return self._list(limit=limit, params=filter.dump() if filter else None)
