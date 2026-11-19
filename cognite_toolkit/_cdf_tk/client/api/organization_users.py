"""Organization Users API for listing the users of a CDF organization (beta).

Based on the API specification at:
https://api-docs.cognite.com/20230101-beta/tag/User-profiles
"""

import builtins
from collections.abc import Iterable

from cognite_toolkit._cdf_tk.client.api.project import ProjectAPI
from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.organization_user import OrganizationUser

_AUTH_V0_URL = "https://auth.cognite.com/api/v0"


class OrganizationUsersAPI(CDFResourceAPI[OrganizationUser]):
    """API for the Cognite Organization Users endpoint.

    Unlike most CDF APIs, this API is not project-scoped and is served from
    `https://auth.cognite.com/api/v0/orgs/{org}/users`.
    """

    def __init__(self, http_client: HTTPClient, project_api: ProjectAPI) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "list": Endpoint(method="GET", path="/orgs/{org}/users", item_limit=1000),
            },
            api_version="beta",
        )
        self._project_api = project_api

    def _make_url(self, path: str = "") -> str:
        """Create the full URL for this resource endpoint."""
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{_AUTH_V0_URL}{path}"

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[OrganizationUser]:
        return PagedResponse[OrganizationUser].model_validate_json(response.body)

    def paginate(self, org: str | None = None, cursor: str | None = None) -> PagedResponse[OrganizationUser]:
        """Fetch a page of users in the organization. The API returns up to 1000 users per page.

        Args:
            org: The ID of the organization. Defaults to the organization of the current project.
            cursor: Cursor for pagination.

        Returns:
            A page of organization users and the cursor for the next page.
        """
        endpoint = self._method_endpoint_map["list"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path.format(org=org or self._project_api.get_organization_id())),
            method=endpoint.method,
            parameters={"cursor": cursor} if cursor is not None else None,
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return self._validate_page_response(response)

    def iterate(
        self, org: str | None = None, limit: int | None = None, cursor: str | None = None
    ) -> Iterable[builtins.list[OrganizationUser]]:
        """Iterate over the users in the organization.

        The API does not accept a page size, so the last page is truncated client side to respect ``limit``.

        Args:
            org: The ID of the organization. Defaults to the organization of the current project.
            limit: Maximum number of users to return. None returns all.
            cursor: Cursor to start from.

        Returns:
            Pages of organization users.
        """
        org_id = org or self._project_api.get_organization_id()
        next_cursor = cursor
        total = 0
        while True:
            page = self.paginate(org=org_id, cursor=next_cursor)
            items = page.items if limit is None else page.items[: limit - total]
            yield items
            total += len(items)
            if page.next_cursor is None or not page.items or (limit is not None and total >= limit):
                break
            next_cursor = page.next_cursor

    def list(self, org: str | None = None, limit: int | None = None) -> builtins.list[OrganizationUser]:
        """List the users in the organization.

        Args:
            org: The ID of the organization. Defaults to the organization of the current project.
            limit: Maximum number of users to return. None returns all.

        Returns:
            The organization users.
        """
        return [user for page in self.iterate(org=org, limit=limit) for user in page]
