from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import RawDatabaseId, RawTableId
from cognite_toolkit._cdf_tk.client.resource_classes.raw import (
    RAWDatabaseRequest,
    RAWDatabaseResponse,
    RawProfileResponse,
    RAWTableRequest,
    RAWTableResponse,
)


class RawDatabasesAPI(CDFResourceAPI[RawDatabaseId, RAWDatabaseRequest, RAWDatabaseResponse]):
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

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[RAWDatabaseResponse]:
        return PagedResponse[RAWDatabaseResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[RAWDatabaseResponse]:
        return ResponseItems[RAWDatabaseResponse].model_validate_json(response.body)

    def create(self, items: Sequence[RAWDatabaseRequest]) -> list[RAWDatabaseResponse]:
        """Create databases in CDF.

        Args:
            items: List of RAWDatabase objects to create.

        Returns:
            List of created RAWDatabase objects.
        """
        return self._request_item_response(list(items), "create")

    def delete(self, items: Sequence[RawDatabaseId], recursive: bool = False) -> None:
        """Delete databases from CDF.

        Args:
            items: List of RAWDatabase objects to delete.
            recursive: Whether to delete tables within the database recursively.
        """
        self._request_no_response(items, "delete", extra_body={"recursive": recursive})

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[RAWDatabaseResponse]:
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
    ) -> Iterable[list[RAWDatabaseResponse]]:
        """Iterate over all databases in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of RAWDatabase objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> list[RAWDatabaseResponse]:
        """List all databases in CDF.

        Args:
            limit: Maximum number of databases to return. If None, returns all databases.

        Returns:
            List of RAWDatabase objects.
        """
        return self._list(limit=limit)


class RawTablesAPI(CDFResourceAPI[RawTableId, RAWTableRequest, RAWTableResponse]):
    """API for managing RAW tables in CDF.

    This API provides methods to create, list, and delete RAW tables within a database.

    Note: This API requires db_name as a path parameter for all operations,
    so it overrides several base class methods to handle dynamic paths.
    """

    DEFAULT_PROFILE_LIMIT = 1000
    MAX_PROFILE_LIMIT = 1_000_000

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

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[RAWTableResponse]:
        """Parse a page response. Note: db_name must be injected separately."""
        return PagedResponse[RAWTableResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[RAWTableResponse]:
        """Parse a reference response. Note: db_name must be injected separately."""
        return ResponseItems[RAWTableResponse].model_validate_json(response.body)

    def create(self, items: Sequence[RAWTableRequest], ensure_parent: bool = False) -> list[RAWTableResponse]:
        """Create tables in a database in CDF.

        Args:
            items: List of RAWTable objects to create.
            ensure_parent: Create database if it doesn't exist already.

        Returns:
            List of created RAWTable objects.
        """
        result: list[RAWTableResponse] = []
        for (db_name,), group in self._group_items_by_text_field(items, "db_name").items():
            if not db_name:
                raise ValueError("db_name must be set on all RAWTable items for creation.")
            endpoint = f"/raw/dbs/{db_name}/tables"
            created = self._request_item_response(
                group, "create", params={"ensureParent": ensure_parent}, endpoint=endpoint
            )
            for table in created:
                table.db_name = db_name
                result.append(table)
        return result

    def delete(self, items: Sequence[RawTableId]) -> None:
        """Delete tables from a database in CDF.

        Args:
            items: List of RAWTable objects to delete.
        """
        for (db_name,), group in self._group_items_by_text_field(items, "db_name").items():
            if not db_name:
                raise ValueError("db_name must be set on all RAWTable items for deletion.")
            endpoint = f"/raw/dbs/{db_name}/tables/delete"
            self._request_no_response(list(group), "delete", endpoint=endpoint)

    def profile(
        self, table: RawTableId, limit: int = DEFAULT_PROFILE_LIMIT, timeout_seconds: int | None = None
    ) -> RawProfileResponse:
        """Profiles a table in the specified database and returns the results.

        This is a hidden endpoint that is not part of the official CDF API. However, it is used by the Fusion UI
        to profile tables in the database. This is implemented internally in Cognite Toolkit as Toolkit offers
        profiling of raw tables. This is used to show how data flows into CDF resources.

        Args:
        Args:
            table (RawTableId): The identifier of the table to profile.
            limit (int, optional): The maximum number of rows to profile. Defaults to DEFAULT_PROFILE_LIMIT.
            timeout_seconds (int, optional): The timeout for the profiling operation in seconds. Defaults to global_config.timeout_seconds.

        Returns:
            RawProfileResponse: The results of the profiling operation.

        """
        if limit <= 0 or limit > self.MAX_PROFILE_LIMIT:
            raise ValueError(f"Limit must be between 1 and {self.MAX_PROFILE_LIMIT}, got {limit}.")
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._http_client.config.create_api_url("/profiler/raw"),
                method="POST",
                body_content={"database": table.db_name, "table": table.name, "limit": limit},
                client_timeout=timeout_seconds,
            )
        ).get_success_or_raise()
        return RawProfileResponse.model_validate_json(response.body)

    def paginate(
        self,
        db_name: str,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[RAWTableResponse]:
        """Iterate over all tables in a database in CDF.

        Args:
            db_name: The name of the database to list tables from.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of RAWTable objects.
        """
        page = self._paginate(cursor=cursor, limit=limit, endpoint_path=f"/raw/dbs/{db_name}/tables")
        for table in page.items:
            table.db_name = db_name
        return page

    def iterate(
        self,
        db_name: str,
        limit: int | None = 100,
    ) -> Iterable[list[RAWTableResponse]]:
        """Iterate over all tables in a database in CDF.

        Args:
            db_name: The name of the database to list tables from.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of RAWTable objects.
        """
        for table in self._iterate(limit=limit, endpoint_path=f"/raw/dbs/{db_name}/tables"):
            for t in table:
                t.db_name = db_name
            yield table

    def list(self, db_name: str, limit: int | None = None) -> list[RAWTableResponse]:
        """List all tables in a database in CDF.

        Args:
            db_name: The name of the database to list tables from.
            limit: Maximum number of tables to return. If None, returns all tables.

        Returns:
            List of RAWTable objects.
        """
        listed = self._list(limit, endpoint_path=f"/raw/dbs/{db_name}/tables")
        for table in listed:
            table.db_name = db_name
        return listed


class RawAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self.databases = RawDatabasesAPI(http_client)
        self.tables = RawTablesAPI(http_client)
