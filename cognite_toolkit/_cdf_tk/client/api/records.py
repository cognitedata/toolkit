from collections import defaultdict
from collections.abc import Sequence

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordId, RecordResponse, RecordSyncResponse
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence


class RecordsAPI:
    _FILTER_ENDPOINT = "/streams/{streamId}/records/filter"
    _SYNC_ENDPOINT = "/streams/{streamId}/records/sync"
    _FILTER_IN_MAX_VALUES = 100

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

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
                    body["lastUpdatedTime"] = last_updated_time  # type: ignore[assignment]

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
            RecordSyncResponse with items, nextCursor, and hasNext.
        """
        body: dict[str, JsonValue] = {
            "sources": sources,  # type: ignore[dict-item]
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
        return RecordSyncResponse.model_validate_json(response.body)
