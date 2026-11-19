"""Postgres Gateway Tables API.

Based on https://api-docs.cognite.com/20230101/tag/Postgres-Gateway-Tables
"""

import builtins
from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayTableId
from cognite_toolkit._cdf_tk.client.resource_classes.postgres_gateway_table import (
    PostgresGatewayTableRequest,
    PostgresGatewayTableResponseUnion,
)

_ITEM_LIMIT = 100


class PostgresGatewayTablesAPI(CDFResourceAPI[PostgresGatewayTableResponseUnion]):
    """Foreign tables exposed through the Postgres Gateway. Each table belongs to a postgres gateway user.

    The username is a path parameter, so requests are grouped by username.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/postgresgateway/tables/{username}", item_limit=10),
                "retrieve": Endpoint(method="POST", path="/postgresgateway/tables/{username}/byids", item_limit=10),
                "delete": Endpoint(method="POST", path="/postgresgateway/tables/{username}/delete", item_limit=10),
                "list": Endpoint(method="GET", path="/postgresgateway/tables/{username}", item_limit=_ITEM_LIMIT),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[PostgresGatewayTableResponseUnion]:
        return PagedResponse[PostgresGatewayTableResponseUnion].model_validate_json(response.body)

    @staticmethod
    def _tables_path(username: str) -> str:
        return f"/postgresgateway/tables/{username}"

    @staticmethod
    def _serialize_tablenames(items: Sequence[PostgresGatewayTableId]) -> builtins.list[dict[str, Any]]:
        return [{"tablename": item.tablename} for item in items]

    @staticmethod
    def _stamp_username(
        tables: builtins.list[PostgresGatewayTableResponseUnion], username: str
    ) -> builtins.list[PostgresGatewayTableResponseUnion]:
        for table in tables:
            table.username = username
        return tables

    def create(self, items: Sequence[PostgresGatewayTableRequest]) -> builtins.list[PostgresGatewayTableResponseUnion]:
        """Create postgres gateway tables.

        Args:
            items: Tables to create. Each table is created for the user given by its username.
        Returns:
            The created tables.
        """
        created: builtins.list[PostgresGatewayTableResponseUnion] = []
        for (username,), group in self._group_items_by_text_field(items, "username").items():
            tables = self._request_item_response(group, "create", endpoint=self._tables_path(username))
            created.extend(self._stamp_username(tables, username))
        return created

    def retrieve(
        self, items: Sequence[PostgresGatewayTableId], ignore_unknown_ids: bool = False
    ) -> builtins.list[PostgresGatewayTableResponseUnion]:
        """Retrieve postgres gateway tables.

        Args:
            items: Tables to retrieve.
            ignore_unknown_ids: Ignore tables that are not found.
        Returns:
            The retrieved tables.
        """
        retrieved: builtins.list[PostgresGatewayTableResponseUnion] = []
        for (username,), group in self._group_items_by_text_field(items, "username").items():
            for response in self._chunk_requests(
                group,
                "retrieve",
                self._serialize_tablenames,
                extra_body={"ignoreUnknownIds": ignore_unknown_ids},
                endpoint_path=f"{self._tables_path(username)}/byids",
            ):
                retrieved.extend(self._stamp_username(self._validate_page_response(response).items, username))
        return retrieved

    def delete(self, items: Sequence[PostgresGatewayTableId], ignore_unknown_ids: bool = False) -> None:
        """Delete postgres gateway tables.

        Args:
            items: Tables to delete.
            ignore_unknown_ids: Ignore tables that are not found.
        """
        for (username,), group in self._group_items_by_text_field(items, "username").items():
            list(
                self._chunk_requests(
                    group,
                    "delete",
                    self._serialize_tablenames,
                    extra_body={"ignoreUnknownIds": ignore_unknown_ids},
                    endpoint_path=f"{self._tables_path(username)}/delete",
                )
            )

    def paginate(
        self, username: str, include_built_ins: bool = False, limit: int = _ITEM_LIMIT, cursor: str | None = None
    ) -> PagedResponse[PostgresGatewayTableResponseUnion]:
        """Fetch one page of the tables of a postgres gateway user.

        Args:
            username: The user to list tables for.
            include_built_ins: Include the built-in tables.
            limit: Maximum number of tables to return. The server caps this at 100.
            cursor: Cursor for the next page.
        Returns:
            One page of tables.
        """
        page = self._paginate(
            limit=limit,
            cursor=cursor,
            params={"includeBuiltIns": include_built_ins},
            endpoint_path=self._tables_path(username),
        )
        self._stamp_username(page.items, username)
        return page

    def iterate(
        self, username: str, include_built_ins: bool = False, limit: int | None = None
    ) -> Iterable[builtins.list[PostgresGatewayTableResponseUnion]]:
        """Iterate over the tables of a postgres gateway user.

        Args:
            username: The user to list tables for.
            include_built_ins: Include the built-in tables.
            limit: Maximum number of tables to return in total. None returns all tables.
        Returns:
            Pages of tables.
        """
        for page in self._iterate(
            limit=limit, params={"includeBuiltIns": include_built_ins}, endpoint_path=self._tables_path(username)
        ):
            yield self._stamp_username(page, username)

    def list(
        self, username: str, include_built_ins: bool = False, limit: int | None = None
    ) -> builtins.list[PostgresGatewayTableResponseUnion]:
        """List the tables of a postgres gateway user.

        Args:
            username: The user to list tables for.
            include_built_ins: Include the built-in tables.
            limit: Maximum number of tables to return. None returns all tables.
        Returns:
            Tables.
        """
        return self._stamp_username(
            self._list(
                limit=limit, params={"includeBuiltIns": include_built_ins}, endpoint_path=self._tables_path(username)
            ),
            username,
        )
