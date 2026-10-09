import builtins
from collections.abc import Iterable, Sequence
from typing import Any
from urllib.parse import quote

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import (
    FailedResponse,
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import RawDatabaseId, RawRowId, RawTableId
from cognite_toolkit._cdf_tk.client.resource_classes.raw import (
    RAWDatabaseRequest,
    RAWDatabaseResponse,
    RawProfileResponse,
    RAWRowRequest,
    RAWRowResponse,
    RAWTableRequest,
    RAWTableResponse,
)


class RawDatabasesAPI(CDFResourceAPI[RAWDatabaseResponse]):
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

    def create(self, items: Sequence[RAWDatabaseRequest]) -> builtins.list[RAWDatabaseResponse]:
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
        limit: int | None = 100,
    ) -> Iterable[builtins.list[RAWDatabaseResponse]]:
        """Iterate over all databases in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of RAWDatabase objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> builtins.list[RAWDatabaseResponse]:
        """List all databases in CDF.

        Args:
            limit: Maximum number of databases to return. If None, returns all databases.

        Returns:
            List of RAWDatabase objects.
        """
        return self._list(limit=limit)


class RawTablesAPI(CDFResourceAPI[RAWTableResponse]):
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
        self.rows = RawRowsAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[RAWTableResponse]:
        """Parse a page response. Note: db_name must be injected separately."""
        return PagedResponse[RAWTableResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[RAWTableResponse]:
        """Parse a reference response. Note: db_name must be injected separately."""
        return ResponseItems[RAWTableResponse].model_validate_json(response.body)

    def create(self, items: Sequence[RAWTableRequest], ensure_parent: bool = False) -> builtins.list[RAWTableResponse]:
        """Create tables in a database in CDF.

        Args:
            items: List of RAWTable objects to create.
            ensure_parent: Create database if it doesn't exist already.

        Returns:
            List of created RAWTable objects.
        """
        result: builtins.list[RAWTableResponse] = []
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
            table (RawTableId): The identifier of the table to profile.
            limit (int, optional): The maximum number of rows to profile. Defaults to DEFAULT_PROFILE_LIMIT.
            timeout_seconds (int, optional): The timeout for the profiling operation in seconds. Defaults to global_config.timeout_seconds.

        Returns:
            RawProfileResponse: The results of the profiling operation.

        """
        if limit <= 0 or limit > self.MAX_PROFILE_LIMIT:
            raise ValueError(f"Limit must be between 1 and {self.MAX_PROFILE_LIMIT}, got {limit}.")
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url("/profiler/raw"),
            method="POST",
            body_content={"database": table.db_name, "table": table.name, "limit": limit},
            client_timeout=timeout_seconds,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
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
    ) -> Iterable[builtins.list[RAWTableResponse]]:
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

    def list(self, db_name: str, limit: int | None = None) -> builtins.list[RAWTableResponse]:
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


class RawRowsAPI(CDFResourceAPI[RAWRowResponse]):
    """API for managing rows in a RAW table.

    Insert and delete accept up to 10,000 rows per request. The database and table name are path
    parameters, so items are grouped by those fields before each request.

    https://api-docs.cognite.com/20230101/tag/Raw/operation/postRows
    https://api-docs.cognite.com/20230101/tag/Raw/operation/deleteRows
    https://api-docs.cognite.com/20230101/tag/Raw/operation/getRow
    https://api-docs.cognite.com/20230101/tag/Raw/operation/getRows
    """

    ROW_LIMIT = 10_000

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client,
            {
                "create": Endpoint(
                    method="POST", path="/raw/dbs/{dbName}/tables/{tableName}/rows", item_limit=self.ROW_LIMIT
                ),
                "delete": Endpoint(
                    method="POST",
                    path="/raw/dbs/{dbName}/tables/{tableName}/rows/delete",
                    item_limit=self.ROW_LIMIT,
                ),
                "retrieve": Endpoint(
                    method="GET", path="/raw/dbs/{dbName}/tables/{tableName}/rows/{rowKey}", item_limit=1
                ),
                "list": Endpoint(
                    method="GET", path="/raw/dbs/{dbName}/tables/{tableName}/rows", item_limit=self.ROW_LIMIT
                ),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[RAWRowResponse]:
        return PagedResponse[RAWRowResponse].model_validate_json(response.body)

    def create(self, items: Sequence[RAWRowRequest], ensure_parent: bool = False) -> None:
        """Insert rows into one or more RAW tables.

        Existing rows with the same key are replaced. The endpoint returns an empty body.

        Args:
            items: Rows to insert. Each item must set ``db_name`` and ``table_name``.
            ensure_parent: Create the database and table when they do not already exist.
        """
        for (db_name, table_name), group in self._group_items_by_text_field(items, "db_name", "table_name").items():
            self._require_parent(db_name, table_name)
            self._request_no_response(
                group,
                "create",
                params={"ensureParent": ensure_parent},
                endpoint=self._rows_collection_path(db_name, table_name),
            )

    def delete(self, items: Sequence[RawRowId]) -> None:
        """Delete rows from one or more RAW tables.

        Args:
            items: Row identifiers to delete.
        """
        for (db_name, table_name), group in self._group_items_by_text_field(items, "db_name", "table_name").items():
            self._require_parent(db_name, table_name)
            self._request_no_response(group, "delete", endpoint=self._rows_delete_path(db_name, table_name))

    def retrieve(self, items: Sequence[RawRowId], ignore_unknown_ids: bool = False) -> builtins.list[RAWRowResponse]:
        """Retrieve rows by key.

        Args:
            items: Row identifiers to retrieve.
            ignore_unknown_ids: Skip rows that do not exist. When False, a missing row raises.

        Returns:
            The retrieved rows.
        """
        result: builtins.list[RAWRowResponse] = []
        endpoint = self._method_endpoint_map["retrieve"]
        for item in items:
            self._require_parent(item.db_name, item.table_name)
            request = RequestMessage(
                endpoint_url=self._make_url(self._row_path(item.db_name, item.table_name, item.key)),
                method=endpoint.method,
            )
            response = self._http_client.request_single_retries(request)
            if isinstance(response, SuccessResponse):
                row = RAWRowResponse.model_validate_json(response.body)
                row.db_name = item.db_name
                row.table_name = item.table_name
                result.append(row)
            elif (
                ignore_unknown_ids
                and isinstance(response, FailedResponse)
                and 400 <= response.status_code < 500
                and response.status_code != 429
            ):
                continue
            else:
                _ = response.get_success_or_raise(request)
        return result

    def paginate(
        self,
        db_name: str,
        table_name: str,
        limit: int = 100,
        cursor: str | None = None,
        columns: Sequence[str] | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
    ) -> PagedResponse[RAWRowResponse]:
        """Fetch one page of rows from a RAW table.

        Args:
            db_name: Database that contains the table.
            table_name: Table to read.
            limit: Maximum number of rows to return. The API allows 1 to 10,000.
            cursor: Cursor from a previous page.
            columns: Column names to return, joined with commas. Omit to return every column.
                Pass ``[","]`` to retrieve only row keys.
            min_last_updated_time: Exclusive lower bound on ``lastUpdatedTime``, in epoch milliseconds.
            max_last_updated_time: Inclusive upper bound on ``lastUpdatedTime``, in epoch milliseconds.

        Returns:
            One page of rows. Each row has ``db_name`` and ``table_name`` set.
        """
        self._require_parent(db_name, table_name)
        page = self._paginate(
            cursor=cursor,
            limit=limit,
            params=self._row_list_params(columns, min_last_updated_time, max_last_updated_time),
            endpoint_path=self._rows_collection_path(db_name, table_name),
        )
        self._stamp_parent(page.items, db_name, table_name)
        return page

    def iterate(
        self,
        db_name: str,
        table_name: str,
        limit: int | None = 100,
        columns: Sequence[str] | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
    ) -> Iterable[builtins.list[RAWRowResponse]]:
        """Iterate over rows in a RAW table.

        Args:
            db_name: Database that contains the table.
            table_name: Table to read.
            limit: Maximum number of rows to return in total. ``None`` reads every row.
            columns: Column names to return. Omit to return every column.
            min_last_updated_time: Exclusive lower bound on ``lastUpdatedTime``, in epoch milliseconds.
            max_last_updated_time: Inclusive upper bound on ``lastUpdatedTime``, in epoch milliseconds.

        Returns:
            Pages of rows. Each row has ``db_name`` and ``table_name`` set.
        """
        self._require_parent(db_name, table_name)
        params = self._row_list_params(columns, min_last_updated_time, max_last_updated_time)
        for page in self._iterate(
            limit=limit, params=params, endpoint_path=self._rows_collection_path(db_name, table_name)
        ):
            self._stamp_parent(page, db_name, table_name)
            yield page

    def list(
        self,
        db_name: str,
        table_name: str,
        limit: int | None = None,
        columns: Sequence[str] | None = None,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
    ) -> builtins.list[RAWRowResponse]:
        """List rows in a RAW table.

        Args:
            db_name: Database that contains the table.
            table_name: Table to read.
            limit: Maximum number of rows to return. ``None`` returns every row.
            columns: Column names to return. Omit to return every column.
            min_last_updated_time: Exclusive lower bound on ``lastUpdatedTime``, in epoch milliseconds.
            max_last_updated_time: Inclusive upper bound on ``lastUpdatedTime``, in epoch milliseconds.

        Returns:
            Rows in the table. Each row has ``db_name`` and ``table_name`` set.
        """
        self._require_parent(db_name, table_name)
        listed = self._list(
            limit,
            params=self._row_list_params(columns, min_last_updated_time, max_last_updated_time),
            endpoint_path=self._rows_collection_path(db_name, table_name),
        )
        self._stamp_parent(listed, db_name, table_name)
        return listed

    @staticmethod
    def _require_parent(db_name: str, table_name: str) -> None:
        if not db_name or not table_name:
            raise ValueError("db_name and table_name must be set on all raw row items.")

    @staticmethod
    def _rows_collection_path(db_name: str, table_name: str) -> str:
        return f"/raw/dbs/{quote(db_name, safe='')}/tables/{quote(table_name, safe='')}/rows"

    @classmethod
    def _rows_delete_path(cls, db_name: str, table_name: str) -> str:
        return f"{cls._rows_collection_path(db_name, table_name)}/delete"

    @classmethod
    def _row_path(cls, db_name: str, table_name: str, key: str) -> str:
        return f"{cls._rows_collection_path(db_name, table_name)}/{quote(key, safe='')}"

    @staticmethod
    def _row_list_params(
        columns: Sequence[str] | None,
        min_last_updated_time: int | None,
        max_last_updated_time: int | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if columns is not None:
            params["columns"] = ",".join(columns)
        if min_last_updated_time is not None:
            params["minLastUpdatedTime"] = min_last_updated_time
        if max_last_updated_time is not None:
            params["maxLastUpdatedTime"] = max_last_updated_time
        return params

    @staticmethod
    def _stamp_parent(rows: Iterable[RAWRowResponse], db_name: str, table_name: str) -> None:
        for row in rows:
            row.db_name = db_name
            row.table_name = table_name


class RawAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self.databases = RawDatabasesAPI(http_client)
        self.tables = RawTablesAPI(http_client)
