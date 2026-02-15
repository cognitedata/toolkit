from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import SequenceRowFilter
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import SequenceRowId
from cognite_toolkit._cdf_tk.client.resource_classes.sequence_rows import (
    SequenceRowsRequest,
    SequenceRowsResponse,
)


class SequenceRowsAPI(CDFResourceAPI[SequenceRowId, SequenceRowsRequest, SequenceRowsResponse]):
    """API for managing sequence row data in CDF.

    This handles inserting, deleting, and retrieving rows from sequences.
    The sequence rows endpoints differ from standard CRUD APIs: insert and delete
    operate on batches of items, while retrieve and retrieve_latest operate on a
    single sequence at a time.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/sequences/data", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path="/sequences/data/list", item_limit=1000),
                "delete": Endpoint(
                    method="POST", path="/sequences/data/delete", item_limit=1000, concurrency_max_workers=1
                ),
            },
        )
        self._latest_endpoint = Endpoint(method="POST", path="/sequences/data/latest", item_limit=1000)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SequenceRowsResponse]:
        return PagedResponse[SequenceRowsResponse].model_validate_json(response.body)

    def _post_single(self, path: str, body: dict[str, Any]) -> str:
        """Send a single-object POST request and return the response body.

        Unlike the items-based endpoints, the retrieve and retrieve_latest
        endpoints accept a single object body (no "items" wrapper).
        """
        request = RequestMessage(
            endpoint_url=self._make_url(path),
            method="POST",
            body_content=body,
        )
        result = self._http_client.request_single_retries(request)
        return result.get_success_or_raise().body

    def create(self, items: Sequence[SequenceRowsRequest]) -> None:
        """Insert rows into one or more sequences.

        This overwrites data in rows and columns that exist.

        Args:
            items: Sequence of SequenceRowsRequest objects, each specifying a
                sequence (by external_id) and the rows/columns to insert.
        """
        self._request_no_response(items, "create")

    def delete(self, items: Sequence[SequenceRowId]) -> None:
        """Delete rows from one or more sequences.

        All columns are affected for the specified row numbers.

        Args:
            items: Sequence of SequenceRowId objects, each specifying a sequence (by external_id) and the row numbers to delete.
        """
        self._request_no_response(items, "delete")

    def latest(self, external_id: str, before: int | None = None) -> SequenceRowsResponse:
        """Retrieve the latest rows for a given sequence.

        This endpoint returns the latest rows for a single sequence, with an optional
        filter for which rows to include. The response structure is the same as the
        list endpoint but without pagination (all rows are returned in one response).

        Args:
            external_id: External ID of the sequence to retrieve rows for.
            before: Optional filter to only include rows with row_number less than this value.

        Returns:
            SequenceRowsResponse containing the latest rows for the specified sequence.
        """
        body: dict[str, Any] = {"externalId": external_id}
        if before is not None:
            body["before"] = before

        result = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._make_url(self._latest_endpoint.path),
                method=self._latest_endpoint.method,
                body_content=body,
                disable_gzip=self._disable_gzip,
                api_version=self._api_version,
            )
        )
        response = result.get_success_or_raise()
        return SequenceRowsResponse.model_validate_json(response.body)

    # Overridden form of the _paginate method to handle the unique pagination structure of sequence rows endpoints
    def _paginate(
        self,
        limit: int,
        cursor: str | None = None,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        endpoint_path: str | None = None,
    ) -> PagedResponse[SequenceRowsResponse]:
        """Fetch a single page of resources.

        Args:
            params: Query parameters for the request. Supported parameters depend on
                the resource type but typically include:
                - cursor: Cursor for pagination
                - limit: Maximum number of items (defaults to list limit)
                - space: Filter by space
                - includeGlobal: Whether to include global resources
            body : Body content for the request, if applicable.
                limit: Maximum number of items to return in the page.
                cursor: Cursor for pagination.
            limit: Maximum number of items to return in the page.
            cursor: Cursor for pagination.

        Returns:
            A Page containing the items and the cursor for the next page.
        """
        endpoint = self._method_endpoint_map["list"]
        if not (0 < limit <= endpoint.item_limit):
            raise ValueError(f"Limit must be between 1 and {endpoint.item_limit}, got {limit}.")

        body_content: dict[str, Any] = {"limit": limit, **(body or {})}
        if cursor is not None:
            body_content["cursor"] = cursor

        request = RequestMessage(
            endpoint_url=self._make_url(endpoint_path or endpoint.path),
            method=endpoint.method,
            body_content=body_content,
            disable_gzip=self._disable_gzip,
            api_version=self._api_version,
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()

        parsed = SequenceRowsResponse.model_validate_json(response.body)
        return PagedResponse(items=[parsed], nextCursor=parsed.next_cursor)

    def paginate(
        self, filter: SequenceRowFilter, limit: int = 100, cursor: str | None = None
    ) -> PagedResponse[SequenceRowsResponse]:
        """Iterate over sequence rows in CDF with pagination.

        Args:
            filter: Which sequence rows to paginate over.
            limit: Maximum number of rows to return per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SequenceResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit, body=filter.dump())

    # Overridden to count on the number of rows rather than number of items.
    def _iterate(
        self,
        limit: int | None = None,
        cursor: str | None = None,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        endpoint_path: str | None = None,
    ) -> Iterable[list[SequenceRowsResponse]]:
        """Iterate over all resources, handling pagination automatically."""
        next_cursor = cursor
        total = 0
        endpoint = self._method_endpoint_map["list"]
        while True:
            page_limit = endpoint.item_limit if limit is None else min(limit - total, endpoint.item_limit)
            page = self._paginate(
                limit=page_limit, cursor=next_cursor, params=params, body=body, endpoint_path=endpoint_path
            )
            yield page.items
            total += len(page.items[0].rows)
            if page.next_cursor is None or (limit is not None and total >= limit):
                break
            next_cursor = page.next_cursor

    def iterate(self, filter: SequenceRowFilter, limit: int | None = 100) -> Iterable[list[SequenceRowsResponse]]:
        """Iterate over sequence rows in CDF.

        Args:
            filter: Which sequence rows to iterate over.
            limit: Maximum number of rows to return. If None, iterates over all rows (may be slow for large sequences).

        Returns:
            Iterable of lists of SequenceResponse objects.
        """
        return self._iterate(limit=limit, body=filter.dump())

    def list(self, filter: SequenceRowFilter, limit: int | None = 100) -> list[SequenceRowsResponse]:
        """List sequence rows for a given sequence in CDF.

        Args:
            filter: Which sequence rows to list.
            limit: Maximum number of rows to return. If None, returns all rows (may be slow for large sequences).

        Returns:
            List of SequenceResponse objects.
        """
        # The sequence rows list endpoint returns one sequence's rows at a time (a single SequenceRowsResponse
        # per page), so we aggregate all rows into the first response rather than returning multiple responses.
        # Since each page contains exactly one item, we extend first_response.rows with subsequent pages' rows.
        first_response: SequenceRowsResponse | None = None
        for batch in self._iterate(limit=limit, body=filter.dump()):
            if not batch:
                break
            if first_response is None:
                first_response = batch[0]
            else:
                first_response.rows.extend(batch[0].rows)
        return [first_response] if first_response is not None else []
