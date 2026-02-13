from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.sequence_rows import (
    SequenceRowsDeleteRequest,
    SequenceRowsRequest,
    SequenceRowsResponse,
)

_RETRIEVE_ROW_LIMIT = 10000


class SequenceRowsAPI(CDFResourceAPI[ExternalId, SequenceRowsRequest, SequenceRowsResponse]):
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
                "delete": Endpoint(
                    method="POST", path="/sequences/data/delete", item_limit=1000, concurrency_max_workers=1
                ),
            },
        )

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

    def insert(self, items: Sequence[SequenceRowsRequest]) -> None:
        """Insert rows into one or more sequences.

        This overwrites data in rows and columns that exist.

        Args:
            items: Sequence of SequenceRowsRequest objects, each specifying a
                sequence (by external_id) and the rows/columns to insert.
        """
        self._request_no_response(items, "create")

    def delete(self, items: Sequence[SequenceRowsDeleteRequest]) -> None:
        """Delete rows from one or more sequences.

        All columns are affected for the specified row numbers.

        Args:
            items: Sequence of SequenceRowsDeleteRequest objects, each specifying a
                sequence (by external_id or id) and the row numbers to delete.
        """
        self._request_no_response(items, "delete")

    def retrieve(
        self,
        *,
        external_id: str | None = None,
        id: int | None = None,
        start: int = 0,
        end: int | None = None,
        limit: int = 100,
        columns: list[str] | None = None,
    ) -> SequenceRowsResponse:
        """Retrieve rows from a sequence.

        Note that the API uses a dynamic limit on the number of rows returned
        based on the number and type of columns. Use the next_cursor on the
        response to paginate and retrieve all data.

        Args:
            external_id: The external ID of the sequence. Mutually exclusive with id.
            id: The internal ID of the sequence. Mutually exclusive with external_id.
            start: Lowest row number included. Defaults to 0.
            end: Get rows up to, but excluding, this row number. No limit by default.
            limit: Maximum number of rows returned. Between 1 and 10000. Defaults to 100.
            columns: Columns to include, specified as external IDs.
                If not set, all available columns are returned.

        Returns:
            SequenceRowsResponse containing the row data and an optional next_cursor
            for pagination.

        Raises:
            ValueError: If neither external_id nor id is provided, or both are provided.
        """
        body = self._build_retrieve_body(
            external_id=external_id,
            id=id,
            start=start,
            end=end,
            limit=limit,
            columns=columns,
        )
        response_body = self._post_single("/sequences/data/list", body=body)
        return SequenceRowsResponse.model_validate_json(response_body)

    def retrieve_all(
        self,
        *,
        external_id: str | None = None,
        id: int | None = None,
        start: int = 0,
        end: int | None = None,
        columns: list[str] | None = None,
    ) -> SequenceRowsResponse:
        """Retrieve all rows from a sequence, handling pagination automatically.

        Args:
            external_id: The external ID of the sequence. Mutually exclusive with id.
            id: The internal ID of the sequence. Mutually exclusive with external_id.
            start: Lowest row number included. Defaults to 0.
            end: Get rows up to, but excluding, this row number. No limit by default.
            columns: Columns to include, specified as external IDs.
                If not set, all available columns are returned.

        Returns:
            SequenceRowsResponse with all rows. The next_cursor will be None.
        """
        first = True
        result: SequenceRowsResponse | None = None
        for page in self.iterate(
            external_id=external_id,
            id=id,
            start=start,
            end=end,
            columns=columns,
        ):
            if first:
                result = page
                first = False
            else:
                # Accumulate rows from subsequent pages
                result.rows.extend(page.rows)  # type: ignore[union-attr]
        if result is None:
            raise ValueError("No data returned from the sequence.")
        result.next_cursor = None
        return result

    def iterate(
        self,
        *,
        external_id: str | None = None,
        id: int | None = None,
        start: int = 0,
        end: int | None = None,
        columns: list[str] | None = None,
    ) -> Iterable[SequenceRowsResponse]:
        """Iterate over rows from a sequence page by page.

        Yields one SequenceRowsResponse per page. Use this for memory-efficient
        processing of large sequences.

        Args:
            external_id: The external ID of the sequence. Mutually exclusive with id.
            id: The internal ID of the sequence. Mutually exclusive with external_id.
            start: Lowest row number included. Defaults to 0.
            end: Get rows up to, but excluding, this row number. No limit by default.
            columns: Columns to include, specified as external IDs.
                If not set, all available columns are returned.

        Yields:
            SequenceRowsResponse for each page of results.
        """
        cursor: str | None = None
        while True:
            body = self._build_retrieve_body(
                external_id=external_id,
                id=id,
                start=start,
                end=end,
                limit=_RETRIEVE_ROW_LIMIT,
                columns=columns,
                cursor=cursor,
            )
            response_body = self._post_single("/sequences/data/list", body=body)
            page = SequenceRowsResponse.model_validate_json(response_body)
            yield page
            if page.next_cursor is None:
                break
            cursor = page.next_cursor

    def retrieve_latest(
        self,
        *,
        external_id: str | None = None,
        id: int | None = None,
        columns: list[str] | None = None,
        before: int | None = None,
    ) -> SequenceRowsResponse:
        """Retrieve the last row from a sequence.

        The last row is the one with the highest row number, which is not
        necessarily the one that was ingested most recently.

        Args:
            external_id: The external ID of the sequence. Mutually exclusive with id.
            id: The internal ID of the sequence. Mutually exclusive with external_id.
            columns: Columns to include, specified as external IDs.
                If not set, all available columns are returned.
            before: Get rows up to, but not including, this row number.

        Returns:
            SequenceRowsResponse containing the last row.

        Raises:
            ValueError: If neither external_id nor id is provided, or both are provided.
        """
        body = self._build_identifier_body(external_id=external_id, id=id)
        if columns is not None:
            body["columns"] = columns
        if before is not None:
            body["before"] = before
        response_body = self._post_single("/sequences/data/latest", body=body)
        return SequenceRowsResponse.model_validate_json(response_body)

    @staticmethod
    def _build_identifier_body(
        *,
        external_id: str | None = None,
        id: int | None = None,
    ) -> dict[str, Any]:
        """Build the identifier portion of the request body."""
        if external_id is not None and id is not None:
            raise ValueError("Only one of external_id or id can be provided, not both.")
        if external_id is not None:
            return {"externalId": external_id}
        if id is not None:
            return {"id": id}
        raise ValueError("Either external_id or id must be provided.")

    @classmethod
    def _build_retrieve_body(
        cls,
        *,
        external_id: str | None = None,
        id: int | None = None,
        start: int = 0,
        end: int | None = None,
        limit: int = 100,
        columns: list[str] | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Build the full request body for row retrieval."""
        body = cls._build_identifier_body(external_id=external_id, id=id)
        body["start"] = start
        body["limit"] = limit
        if end is not None:
            body["end"] = end
        if columns is not None:
            body["columns"] = columns
        if cursor is not None:
            body["cursor"] = cursor
        return body
