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
            items: Sequence of SequenceRowsDeleteRequest objects, each specifying a
                sequence (by external_id or id) and the row numbers to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self, filter: SequenceRowFilter, limit: int = 100, cursor: str | None = None
    ) -> PagedResponse[SequenceRowsResponse]:
        """Iterate over sequence rows in CDF with pagination.

        Args:
            filter: Which sequence rows to paginate over.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SequenceResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit, body=filter.dump())

    def iterate(self, filter: SequenceRowFilter, limit: int = 100) -> Iterable[list[SequenceRowsResponse]]:
        """Iterate over sequence rows in CDF.

        Args:
            filter: Which sequence rows to iterate over.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of SequenceResponse objects.
        """
        return self._iterate(limit=limit, body=filter.dump())

    def list(self, filter: SequenceRowFilter, limit: int | None = 100) -> list[SequenceRowsResponse]:
        """List sequence rows for a given sequence in CDF.

        Args:
            filter: Which sequence rows to list.
            limit: Maximum number of items to return.

        Returns:
            List of SequenceResponse objects.
        """
        return self._list(limit=limit, body=filter.dump())
