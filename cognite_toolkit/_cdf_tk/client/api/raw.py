from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.data_classes.raw import RAWDatabase, RAWTable
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage2, SuccessResponse2


class RawDatabasesAPI(CDFResourceAPI[RAWDatabase, RAWDatabase, RAWDatabase]):
    """API for managing RAW databases in CDF.

    This API provides methods to create, list, and delete RAW databases.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client,
            {
                "create": Endpoint(method="POST", path="/raw/dbs", item_limit=1000),
                "delete": Endpoint(method="POST", path="/raw/dbs/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/raw/dbs", item_limit=1000),
            },
        )

    def _page_response(self, response: SuccessResponse2) -> PagedResponse[RAWDatabase]:
        return PagedResponse[RAWDatabase].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[RAWDatabase]:
        return ResponseItems[RAWDatabase].model_validate_json(response.body)

    def create(self, items: Sequence[RAWDatabase]) -> list[RAWDatabase]:
        """Create databases in CDF.

        Args:
            items: List of RAWDatabase objects to create.

        Returns:
            List of created RAWDatabase objects.
        """
        return self._request_item_response(list(items), "create")

    def delete(self, items: Sequence[RAWDatabase], recursive: bool = False) -> None:
        """Delete databases from CDF.

        Args:
            items: List of RAWDatabase objects to delete.
            recursive: Whether to delete tables within the database recursively.
        """
        self._request_no_response(list(items), "delete", extra_body={"recursive": recursive})

    def iterate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[RAWDatabase]:
        """Iterate over all databases in CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of RAWDatabase objects.
        """
        return self._iterate(limit=limit, cursor=cursor)

    def list(self, limit: int | None = None) -> list[RAWDatabase]:
        """List all databases in CDF.

        Args:
            limit: Maximum number of databases to return. If None, returns all databases.

        Returns:
            List of RAWDatabase objects.
        """
        return self._list(limit=limit)


class RawTablesAPI(CDFResourceAPI[RAWTable, RAWTable, RAWTable]):
    """API for managing RAW tables in CDF.

    This API provides methods to create, list, and delete RAW tables within a database.

    Note: This API requires db_name as a path parameter for all operations,
    so it overrides several base class methods to handle dynamic paths.
    """

    # Item limit for chunking requests
    _ITEM_LIMIT = 1000

    def __init__(self, http_client: HTTPClient) -> None:
        # We pass empty endpoint map since paths are dynamic (depend on db_name)
        super().__init__(http_client, {})

    def _page_response(self, response: SuccessResponse2) -> PagedResponse[RAWTable]:
        """Parse a page response. Note: db_name must be injected separately."""
        return PagedResponse[RAWTable].model_validate_json(response.body)

    def _page_response_with_db(self, response: SuccessResponse2, db_name: str) -> PagedResponse[RAWTable]:
        """Parse response and inject db_name into each table."""
        parsed = self._page_response(response)
        # Inject db_name into each table since it's not returned by the API
        for item in parsed.items:
            item.db_name = db_name
        return parsed

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[RAWTable]:
        """Parse a reference response. Note: db_name must be injected separately."""
        return ResponseItems[RAWTable].model_validate_json(response.body)

    def _reference_response_with_db(self, response: SuccessResponse2, db_name: str) -> ResponseItems[RAWTable]:
        """Parse reference response and inject db_name into each table."""
        parsed = self._reference_response(response)
        for item in parsed.items:
            item.db_name = db_name
        return parsed

    def create(self, db_name: str, items: Sequence[RAWTable]) -> list[RAWTable]:
        """Create tables in a database in CDF.

        Args:
            db_name: The name of the database to create tables in.
            items: List of RAWTable objects to create.

        Returns:
            List of created RAWTable objects.
        """
        request = RequestMessage2(
            endpoint_url=self._make_url(f"/raw/dbs/{db_name}/tables"),
            method="POST",
            body_content={"items": self._serialize_items(items)},
        )
        response = self._http_client.request_single_retries(request)
        return self._page_response_with_db(response.get_success_or_raise(), db_name).items

    def delete(self, db_name: str, items: Sequence[RAWTable]) -> None:
        """Delete tables from a database in CDF.

        Args:
            db_name: The name of the database to delete tables from.
            items: List of RAWTable objects to delete.
        """
        request = RequestMessage2(
            endpoint_url=self._make_url(f"/raw/dbs/{db_name}/tables/delete"),
            method="POST",
            body_content={"items": self._serialize_items(items)},
        )
        response = self._http_client.request_single_retries(request)
        response.get_success_or_raise()

    def iterate(
        self,
        db_name: str,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[RAWTable]:
        """Iterate over all tables in a database in CDF.

        Args:
            db_name: The name of the database to list tables from.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of RAWTable objects.
        """
        params: dict[str, str | int] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor

        request = RequestMessage2(
            endpoint_url=self._make_url(f"/raw/dbs/{db_name}/tables"),
            method="GET",
            parameters=params,
        )
        response = self._http_client.request_single_retries(request)
        return self._page_response_with_db(response.get_success_or_raise(), db_name)

    def list(self, db_name: str, limit: int | None = None) -> list[RAWTable]:
        """List all tables in a database in CDF.

        Args:
            db_name: The name of the database to list tables from.
            limit: Maximum number of tables to return. If None, returns all tables.

        Returns:
            List of RAWTable objects.
        """
        all_items: list[RAWTable] = []
        next_cursor: str | None = None
        total = 0

        while True:
            effective_limit = self._ITEM_LIMIT if limit is None else min(limit - total, self._ITEM_LIMIT)
            page = self.iterate(db_name=db_name, limit=effective_limit, cursor=next_cursor)
            all_items.extend(page.items)
            total += len(page.items)
            if page.next_cursor is None or (limit is not None and total >= limit):
                break
            next_cursor = page.next_cursor

        return all_items
