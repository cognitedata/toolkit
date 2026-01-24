from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWDatabase, RAWTable


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

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[RAWDatabase]:
        return PagedResponse[RAWDatabase].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[RAWDatabase]:
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

    def paginate(
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
        return self._paginate(limit=limit, cursor=cursor)

    def iterate(
        self,
        limit: int = 100,
    ) -> Iterable[list[RAWDatabase]]:
        """Iterate over all databases in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of RAWDatabase objects.
        """
        return self._iterate(limit=limit)

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

    def __init__(self, http_client: HTTPClient) -> None:
        # We pass empty endpoint map since paths are dynamic (depend on db_name)
        super().__init__(
            http_client,
            {
                "create": Endpoint(method="POST", path="/raw/dbs/{dbName}/tables", item_limit=1000),
                "delete": Endpoint(method="POST", path="/raw/dbs/{dbName}/tables/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/raw/dbs/{dbName}/tables", item_limit=1000),
            },
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[RAWTable]:
        """Parse a page response. Note: db_name must be injected separately."""
        return PagedResponse[RAWTable].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[RAWTable]:
        """Parse a reference response. Note: db_name must be injected separately."""
        return ResponseItems[RAWTable].model_validate_json(response.body)

    def create(self, items: Sequence[RAWTable], ensure_parent: bool = False) -> list[RAWTable]:
        """Create tables in a database in CDF.

        Args:
            items: List of RAWTable objects to create.
            ensure_parent: Create database if it doesn't exist already.

        Returns:
            List of created RAWTable objects.
        """
        result: list[RAWTable] = []
        for (db_name,), group in self._group_items_by_text_field(items, "db_name").items():
            if not db_name:
                raise ValueError("db_name must be set on all RAWTable items for creation.")
            endpoint = f"/raw/dbs/{db_name}/tables"
            created = self._request_item_response(
                group, "create", params={"ensureParent": ensure_parent}, endpoint=endpoint
            )
            for table in created:
                result.append(RAWTable(db_name=db_name, name=table.name))
        return result

    def delete(self, items: Sequence[RAWTable]) -> None:
        """Delete tables from a database in CDF.

        Args:
            items: List of RAWTable objects to delete.
        """
        for (db_name,), group in self._group_items_by_text_field(items, "db_name").items():
            if not db_name:
                raise ValueError("db_name must be set on all RAWTable items for deletion.")
            endpoint = f"/raw/dbs/{db_name}/tables/delete"
            self._request_no_response(list(group), "delete", endpoint=endpoint)

    def paginate(
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
        return self._paginate(cursor=cursor, limit=limit, endpoint_path=f"/raw/dbs/{db_name}/tables")

    def iterate(
        self,
        db_name: str,
        limit: int = 100,
    ) -> Iterable[list[RAWTable]]:
        """Iterate over all tables in a database in CDF.

        Args:
            db_name: The name of the database to list tables from.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of RAWTable objects.
        """
        return self._iterate(limit=limit, endpoint_path=f"/raw/dbs/{db_name}/tables")

    def list(self, db_name: str, limit: int | None = None) -> list[RAWTable]:
        """List all tables in a database in CDF.

        Args:
            db_name: The name of the database to list tables from.
            limit: Maximum number of tables to return. If None, returns all tables.

        Returns:
            List of RAWTable objects.
        """
        return self._list(limit, endpoint_path=f"/raw/dbs/{db_name}/tables")


class RawAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self.databases = RawDatabasesAPI(http_client)
        self.tables = RawTablesAPI(http_client)
