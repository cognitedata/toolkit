"""Groups API for managing CDF access groups.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse2, SuccessResponse2
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupRequest, GroupResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId


class GroupsAPI(CDFResourceAPI[InternalId, GroupRequest, GroupResponse]):
    """API for managing CDF access groups.

    Note: Groups do not support update operations.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/groups", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(method="POST", path="/groups/byids", item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/groups/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="GET", path="/groups", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[GroupResponse]:
        return PagedResponse[GroupResponse].model_validate_json(response.body)

    def create(self, items: Sequence[GroupRequest]) -> list[GroupResponse]:
        """Create groups in CDF.

        Args:
            items: List of GroupRequest objects to create.

        Returns:
            List of created GroupResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[InternalId]) -> list[GroupResponse]:
        """Retrieve groups from CDF by ID.

        Args:
            items: List of InternalId objects to retrieve.

        Returns:
            List of retrieved GroupResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def delete(self, items: Sequence[InternalId]) -> None:
        """Delete groups from CDF.

        Args:
            items: List of InternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        all_groups: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[GroupResponse]:
        """Get a page of groups from CDF.

        Args:
            all_groups: Whether to return all groups (requires admin permissions).
            limit: Maximum number of groups to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of GroupResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            params={"all": all_groups} if all_groups else None,
        )

    def iterate(
        self,
        all_groups: bool = False,
        limit: int | None = None,
    ) -> Iterable[list[GroupResponse]]:
        """Iterate over all groups in CDF.

        Args:
            all_groups: Whether to return all groups (requires admin permissions).
            limit: Maximum total number of groups to return.

        Returns:
            Iterable of lists of GroupResponse objects.
        """
        return self._iterate(
            limit=limit,
            params={"all": all_groups} if all_groups else None,
        )

    def list(self, all_groups: bool = False, limit: int | None = None) -> list[GroupResponse]:
        """List all groups in CDF.

        Args:
            all_groups: Whether to return all groups (requires admin permissions).
            limit: Maximum total number of groups to return.

        Returns:
            List of GroupResponse objects.
        """
        return self._list(limit=limit, params={"all": all_groups} if all_groups else None)
