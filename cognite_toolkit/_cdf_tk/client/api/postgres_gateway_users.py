"""Postgres Gateway Users API.

Based on https://api-docs.cognite.com/20230101/tag/Postgres-Gateway-Users
"""

import builtins
from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayUserId
from cognite_toolkit._cdf_tk.client.resource_classes.postgres_gateway_user import (
    PostgresGatewayUserCreatedResponse,
    PostgresGatewayUserRequest,
    PostgresGatewayUserResponse,
)

_ITEM_LIMIT = 100


class PostgresGatewayUsersAPI(CDFResourceAPI[PostgresGatewayUserResponse]):
    """Postgres users that own the foreign tables exposed through the Postgres Gateway."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/postgresgateway", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/postgresgateway/byids", item_limit=10),
                "update": Endpoint(method="POST", path="/postgresgateway/update", item_limit=1),
                "delete": Endpoint(method="POST", path="/postgresgateway/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/postgresgateway", item_limit=_ITEM_LIMIT),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[PostgresGatewayUserResponse]:
        return PagedResponse[PostgresGatewayUserResponse].model_validate_json(response.body)

    @staticmethod
    def _serialize_create(items: Sequence[PostgresGatewayUserRequest]) -> builtins.list[dict[str, Any]]:
        return [item.model_dump(exclude={"username"}) for item in items]

    def create(self, items: Sequence[PostgresGatewayUserRequest]) -> builtins.list[PostgresGatewayUserCreatedResponse]:
        """Create postgres gateway users.

        The API generates the username, password, and host. The password is only returned here.
        Each user is created with its own request.

        Args:
            items: Users to create. Each must have session credentials. The username is ignored.
        Returns:
            The created users, including the password and host.
        """
        created: builtins.list[PostgresGatewayUserCreatedResponse] = []
        for response in self._chunk_requests(items, "create", self._serialize_create):
            created.extend(PagedResponse[PostgresGatewayUserCreatedResponse].model_validate_json(response.body).items)
        return created

    def retrieve(
        self, items: Sequence[PostgresGatewayUserId], ignore_unknown_ids: bool = False
    ) -> builtins.list[PostgresGatewayUserResponse]:
        """Retrieve postgres gateway users by username.

        Args:
            items: Usernames to retrieve. At most 10 per request.
            ignore_unknown_ids: Ignore usernames that are not found.
        Returns:
            The retrieved users.
        """
        return self._request_item_response(items, "retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def update(
        self, items: Sequence[PostgresGatewayUserRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[PostgresGatewayUserResponse]:
        """Update the session credentials of postgres gateway users.

        Args:
            items: Users to update. Each must have a username and session credentials.
            mode: ``replace`` sets every provided field. ``patch`` sets only fields that were assigned.
        Returns:
            The updated users.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[PostgresGatewayUserId], ignore_unknown_ids: bool = False) -> None:
        """Delete postgres gateway users by username.

        Args:
            items: Usernames to delete. Each user is deleted with its own request.
            ignore_unknown_ids: Ignore usernames that are not found.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self, limit: int = _ITEM_LIMIT, cursor: str | None = None
    ) -> PagedResponse[PostgresGatewayUserResponse]:
        """Fetch one page of postgres gateway users.

        Args:
            limit: Maximum number of users to return. The server caps this at 100.
            cursor: Cursor for the next page.
        Returns:
            One page of users.
        """
        return self._paginate(limit=limit, cursor=cursor)

    def iterate(self, limit: int | None = None) -> Iterable[builtins.list[PostgresGatewayUserResponse]]:
        """Iterate over postgres gateway users.

        Args:
            limit: Maximum number of users to return in total. None returns all users.
        Returns:
            Pages of users.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> builtins.list[PostgresGatewayUserResponse]:
        """List postgres gateway users.

        Args:
            limit: Maximum number of users to return. None returns all users.
        Returns:
            Users.
        """
        return self._list(limit=limit)
