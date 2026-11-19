from collections import defaultdict
from collections.abc import Sequence
from typing import cast

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.records import (
    RecordAggregateResponse,
    RecordId,
    RecordRequest,
    RecordResponse,
    RecordSyncResponse,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence


class RecordsAPI:
    _INGEST_ENDPOINT = "/streams/{streamId}/records"
    _UPSERT_ENDPOINT = "/streams/{streamId}/records/upsert"
    _DELETE_ENDPOINT = "/streams/{streamId}/records/delete"
    _AGGREGATE_ENDPOINT = "/streams/{streamId}/records/aggregate"
    _FILTER_ENDPOINT = "/streams/{streamId}/records/filter"
    _SYNC_ENDPOINT = "/streams/{streamId}/records/sync"
    _FILTER_IN_MAX_VALUES = 100
    _WRITE_ITEM_LIMIT = 1000

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def ingest(self, stream_external_id: str, items: Sequence[RecordRequest]) -> None:
        """Ingest records into a stream.

        Records are written asynchronously, they are available for retrieval shortly after the request returns.
        Use :meth:`upsert` to create or update records in a mutable stream.

        Args:
            stream_external_id: External ID of the stream.
            items: Records to ingest.
        """
        self._write(self._INGEST_ENDPOINT, stream_external_id, items)

    def upsert(self, stream_external_id: str, items: Sequence[RecordRequest]) -> None:
        """Create or update records in a mutable stream.

        Args:
            stream_external_id: External ID of the stream.
            items: Records to create or update.
        """
        self._write(self._UPSERT_ENDPOINT, stream_external_id, items)

    def delete(self, stream_external_id: str, items: Sequence[RecordId]) -> None:
        """Delete records from a mutable stream. Unknown records are ignored.

        Args:
            stream_external_id: External ID of the stream.
            items: Records to delete.
        """
        self._write(self._DELETE_ENDPOINT, stream_external_id, items)

    def _write(
        self, endpoint: str, stream_external_id: str, items: Sequence[RecordRequest] | Sequence[RecordId]
    ) -> None:
        url = self._http_client.config.create_api_url(endpoint.format(streamId=stream_external_id))
        for chunk in chunker_sequence(items, self._WRITE_ITEM_LIMIT):
            request = RequestMessage(
                endpoint_url=url, method="POST", body_content={"items": [item.dump() for item in chunk]}
            )
            self._http_client.request_single_retries(request).get_success_or_raise(request)

    def aggregate(
        self,
        stream_external_id: str,
        aggregates: dict[str, JsonValue],
        filter: dict[str, JsonValue] | None = None,
        last_updated_time: dict[str, int | str] | None = None,
        target_units: dict[str, JsonValue] | None = None,
        include_typing: bool | None = None,
    ) -> RecordAggregateResponse:
        """Aggregate records in a stream.

        Args:
            stream_external_id: External ID of the stream.
            aggregates: Aggregates keyed by a user-chosen identifier, for example {"total": {"count": {}}}.
            filter: Optional filter for the records to aggregate.
            last_updated_time: Optional time range filter, e.g. {"gte": ..., "lt": ...}. Required for
                immutable streams.
            target_units: Optional unit conversion, either {"unitSystemName": ...} or {"properties": [...]}.
            include_typing: Whether to include type information for the aggregated properties.

        Returns:
            The aggregate results keyed by the identifiers in ``aggregates``.
        """
        body: dict[str, JsonValue] = {"aggregates": aggregates}
        if filter is not None:
            body["filter"] = filter
        if last_updated_time is not None:
            body["lastUpdatedTime"] = cast(JsonValue, last_updated_time)
        if target_units is not None:
            body["targetUnits"] = target_units
        if include_typing is not None:
            body["includeTyping"] = include_typing
        url = self._http_client.config.create_api_url(self._AGGREGATE_ENDPOINT.format(streamId=stream_external_id))
        request = RequestMessage(endpoint_url=url, method="POST", body_content=body)
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return RecordAggregateResponse.model_validate_json(response.body)

    def retrieve(
        self,
        stream_external_id: str,
        items: Sequence[RecordId],
        last_updated_time: dict[str, int] | None = None,
    ) -> list[RecordResponse]:
        """Retrieve records from a stream matching the given record IDs.

        Args:
            stream_external_id: External ID of the stream to filter.
            items: Record IDs to look up.
            last_updated_time: Optional time range filter, e.g. {"gte": ..., "lt": ...}.

        Returns:
            List of matching RecordResponse items.
        """
        by_space: dict[str, list[str]] = defaultdict(list)
        for item in items:
            by_space[item.space].append(item.external_id)

        results: list[RecordResponse] = []
        url = self._http_client.config.create_api_url(self._FILTER_ENDPOINT.format(streamId=stream_external_id))
        for space, external_ids in by_space.items():
            for id_batch in chunker_sequence(external_ids, self._FILTER_IN_MAX_VALUES):
                body: dict[str, JsonValue] = {
                    "filter": {
                        "and": [
                            {"equals": {"property": ["space"], "value": space}},
                            {"in": {"property": ["externalId"], "values": list(id_batch)}},
                        ]
                    },
                    "limit": min(len(id_batch), 1000),
                }
                if last_updated_time is not None:
                    body["lastUpdatedTime"] = cast(JsonValue, last_updated_time)

                request = RequestMessage(endpoint_url=url, method="POST", body_content=body)
                result = self._http_client.request_single_retries(request)
                response = result.get_success_or_raise(request)
                page = PagedResponse[RecordResponse].model_validate_json(response.body)
                results.extend(page.items)

        return results

    def sync(
        self,
        stream_external_id: str,
        sources: list[dict[str, JsonValue]],
        filter: dict[str, JsonValue],
        limit: int,
        initialize_cursor: str | None = None,
        cursor: str | None = None,
    ) -> RecordSyncResponse:
        """Fetch one page of records from a stream via the sync endpoint.

        Args:
            stream_external_id: External ID of the stream.
            sources: List of source specifications (container + properties).
            filter: Filter expression for the records.
            limit: Maximum number of records to return per page.
            initialize_cursor: Cursor timestamp to initialize from (mutually exclusive with cursor).
            cursor: Continuation cursor from a previous sync response.

        Returns:
            RecordSyncResponse with items, nextCursor, hasNext, and the uncompressed response size in bytes.
        """
        body: dict[str, JsonValue] = {
            "sources": cast(JsonValue, sources),
            "filter": filter,
            "limit": limit,
        }
        if initialize_cursor is not None:
            body["initializeCursor"] = initialize_cursor
        if cursor is not None:
            body["cursor"] = cursor

        url = self._http_client.config.create_api_url(self._SYNC_ENDPOINT.format(streamId=stream_external_id))
        request = RequestMessage(endpoint_url=url, method="POST", body_content=body)
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise(request)
        sync_response = RecordSyncResponse.model_validate_json(response.body)
        sync_response.response_bytes = len(response.content)
        return sync_response
