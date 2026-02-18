"""Groups API for managing CDF access groups.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupRequest, GroupResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence


class GroupsAPI(CDFResourceAPI[InternalId, GroupRequest, GroupResponse]):
    """API for managing CDF access groups.

    Note: Groups do not support update operations.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/groups", item_limit=1000),
                "delete": Endpoint(method="POST", path="/groups/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/groups", item_limit=1000),
            },
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[GroupResponse]:
        return PagedResponse[GroupResponse].model_validate_json(response.body)

    def create(self, items: Sequence[GroupRequest]) -> list[GroupResponse]:
        """Create groups in CDF.

        Args:
            items: List of GroupRequest objects to create.

        Returns:
            List of created GroupResponse objects.
        """
        return self._request_item_response(items, "create")

    def delete(self, items: Sequence[InternalId]) -> None:
        """Delete groups from CDF.

        Args:
            items: List of InternalId objects to delete.
        """
        # Custom implementation since delete does not wrap the items in a {"id": ...} structure
        endpoint = self._method_endpoint_map["delete"]
        for chunk in chunker_sequence(items, endpoint.item_limit):
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                body_content={"items": [item.id for item in chunk]},
            )
            response = self._http_client.request_single_retries(request)
            response.get_success_or_raise()

    def list(self, all_groups: bool = False) -> list[GroupResponse]:
        """List all groups in CDF.

        Args:
            all_groups: Whether to return all groups (requires admin permissions).

        Returns:
            List of GroupResponse objects.
        """
        endpoint = self._method_endpoint_map["list"]
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                parameters={"all": all_groups},
            )
        ).get_success_or_raise()
        return self._validate_page_response(response).items
