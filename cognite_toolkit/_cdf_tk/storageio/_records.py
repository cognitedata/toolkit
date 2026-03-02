from collections.abc import Iterable, Sequence
from typing import ClassVar

from pydantic import BaseModel, Field

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsRequest, ItemsResultList
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest, RecordResponse
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import Page, UploadableStorageIO, UploadItem
from .selectors import RecordContainerSelector


class RecordSyncResponse(BaseModel):
    """Response from the Records /sync endpoint."""

    items: list[RecordResponse]
    next_cursor: str = Field(alias="nextCursor")
    has_next: bool = Field(alias="hasNext")


class RecordIO(UploadableStorageIO[RecordContainerSelector, RecordResponse, RecordRequest]):  # pyright: ignore[reportInvalidTypeArguments]
    KIND = "Records"
    SUPPORTED_DOWNLOAD_FORMATS: ClassVar[frozenset[str]] = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS: ClassVar[frozenset[str]] = frozenset({".gz"})
    SUPPORTED_READ_FORMATS: ClassVar[frozenset[str]] = frozenset({".ndjson"})
    UPLOAD_ENDPOINT = "/streams/{streamId}/records"
    SYNC_ENDPOINT = "/streams/{streamId}/records/sync"
    # TODO: Replace with adaptive limit that targets ~3MB uncompressed response size
    CHUNK_SIZE = 500
    MAX_TOTAL_RECORDS = 1_000_000
    BASE_SELECTOR = RecordContainerSelector

    def as_id(self, item: RecordResponse) -> str:
        return f"{item.space}:{item.external_id}"

    def count(self, selector: RecordContainerSelector) -> int | None:
        return None

    def stream_data(self, selector: RecordContainerSelector, limit: int | None = None) -> Iterable[Page]:
        effective_limit = min(limit, self.MAX_TOTAL_RECORDS) if limit is not None else self.MAX_TOTAL_RECORDS
        url = self.SYNC_ENDPOINT.format(streamId=selector.stream.external_id)
        config = self.client.config
        container = selector.container

        has_data_filter: dict[str, object] = {
            "hasData": [
                {
                    "type": "container",
                    "space": container.space,
                    "externalId": container.external_id,
                }
            ]
        }
        record_filter: dict[str, object]
        if selector.instance_spaces:
            space_filter: dict[str, object] = {
                "in": {
                    "property": ["space"],
                    "values": list(selector.instance_spaces),
                }
            }
            record_filter = {"and": [has_data_filter, space_filter]}
        else:
            record_filter = has_data_filter

        body: dict[str, object] = {
            "sources": [
                {
                    "source": {
                        "type": "container",
                        "space": container.space,
                        "externalId": container.external_id,
                    },
                    "properties": ["*"],
                }
            ],
            "filter": record_filter,
            "initializeCursor": selector.initialize_cursor or "365d-ago",
            "limit": min(self.CHUNK_SIZE, effective_limit),
        }

        total = 0
        while True:
            page_limit = min(self.CHUNK_SIZE, effective_limit - total)
            if page_limit <= 0:
                break
            body["limit"] = page_limit

            result = self.client.http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=config.create_api_url(url),
                    method="POST",
                    body_content=body,  # type: ignore[arg-type]
                )
            )
            response = result.get_success_or_raise()

            sync_response = RecordSyncResponse.model_validate_json(response.body)
            if not sync_response.items:
                break

            remaining = effective_limit - total
            items = sync_response.items[:remaining]
            total += len(items)
            yield Page(worker_id="main", items=items, next_cursor=sync_response.next_cursor)  # pyright: ignore[reportArgumentType]

            if not sync_response.has_next or total >= effective_limit:
                break

            body.pop("initializeCursor", None)
            body["cursor"] = sync_response.next_cursor

    def data_to_json_chunk(
        self, data_chunk: Sequence[RecordResponse], selector: RecordContainerSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        return [record.as_request_resource().dump() for record in data_chunk]

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> RecordRequest:
        return RecordRequest.model_validate(item_json)

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[RecordRequest]],  # pyright: ignore[reportInvalidTypeArguments]
        http_client: HTTPClient,
        selector: RecordContainerSelector | None = None,
    ) -> ItemsResultList:
        if selector is None:
            raise ToolkitValueError("Selector must be provided for RecordIO upload_items")
        url = self.UPLOAD_ENDPOINT.format(streamId=selector.stream.external_id)
        config = http_client.config
        return http_client.request_items_retries(
            message=ItemsRequest(
                endpoint_url=config.create_api_url(url),
                method="POST",
                items=data_chunk,
            )
        )
