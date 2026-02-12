from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordIdentifier, RecordRequest, RecordResponse


class RecordsAPI(CDFResourceAPI[RecordIdentifier, RecordRequest, RecordResponse]):
    """API for managing records in CDF streams.

    Records are scoped to a stream, so the stream_id is passed to each method,
    following the same pattern as RawTablesAPI with db_name.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/streams/{streamId}/records", item_limit=1000),
                "upsert": Endpoint(method="POST", path="/streams/{streamId}/records/upsert", item_limit=1000),
                "delete": Endpoint(method="POST", path="/streams/{streamId}/records/delete", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[RecordResponse]:
        return PagedResponse[RecordResponse].model_validate_json(response.body)

    def ingest(self, stream_id: str, items: Sequence[RecordRequest]) -> None:
        """Ingest records into a stream.

        Args:
            stream_id: The external ID of the stream to ingest records into.
            items: Sequence of RecordRequest items to ingest.
        """
        endpoint = f"/streams/{stream_id}/records"
        self._request_no_response(items, "create", endpoint=endpoint)

    def upsert(self, stream_id: str, items: Sequence[RecordRequest]) -> None:
        """Upsert records into a stream (create or update).

        Args:
            stream_id: The external ID of the stream to upsert records into.
            items: Sequence of RecordRequest items to upsert.
        """
        endpoint = f"/streams/{stream_id}/records/upsert"
        self._request_no_response(items, "upsert", endpoint=endpoint)

    def delete(self, stream_id: str, items: Sequence[RecordIdentifier]) -> None:
        """Delete records from a stream.

        Args:
            stream_id: The external ID of the stream to delete records from.
            items: Sequence of RecordIdentifier objects to delete.
        """
        endpoint = f"/streams/{stream_id}/records/delete"
        self._request_no_response(items, "delete", endpoint=endpoint)
