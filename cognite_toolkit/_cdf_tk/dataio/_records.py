import json
from collections.abc import Iterable

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsRequest, ItemsResultList
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest, RecordResponse, RecordSyncResponse
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamResponse
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.datamodel import ContainerCRUD, SpaceCRUD
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.streams import StreamIO
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename
from cognite_toolkit._cdf_tk.utils.time import timestamp_to_ms
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import StorageIOConfig
from ._base import Bookmark, ConfigurableDataIO, DataItem, Page, UploadableDataIO
from .progress import CursorBookmark, NoBookmark
from .selectors import RecordContainerSelector


class RecordIO(
    ConfigurableDataIO[RecordContainerSelector, RecordResponse],
    UploadableDataIO[RecordContainerSelector, RecordResponse, RecordRequest],
):  # pyright: ignore[reportInvalidTypeArguments]
    KIND = "Records"
    UPLOAD_ENDPOINT = "/streams/{streamId}/records"
    UPSERT_ENDPOINT = "/streams/{streamId}/records/upsert"
    _AGGREGATE_ENDPOINT = "/streams/{streamId}/records/aggregate"
    # TODO: Replace with adaptive limit that targets ~3MB uncompressed response size
    CHUNK_SIZE = 500
    MAX_TOTAL_RECORDS = 1_000_000
    BASE_SELECTOR = RecordContainerSelector

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._stream_by_external_id: dict[str, StreamResponse] = {}

    def _get_stream(self, stream_external_id: str) -> StreamResponse:
        """Get a stream by external ID, caching the response."""
        if stream_external_id not in self._stream_by_external_id:
            streams = StreamIO.create_loader(self.client).retrieve([ExternalId(external_id=stream_external_id)])
            if not streams:
                raise ToolkitValueError(f"Stream '{stream_external_id}' does not exist or is not accessible.")
            self._stream_by_external_id[stream_external_id] = streams[0]
        return self._stream_by_external_id[stream_external_id]

    def count(self, selector: RecordContainerSelector) -> int | None:
        if selector.initialize_cursor is None:
            raise ToolkitValueError("initialize_cursor must be set on the selector for download operations")
        sync_filter = self._build_sync_filter(selector)
        start_ms = timestamp_to_ms(selector.initialize_cursor)
        total = 0
        stream = self._get_stream(selector.stream.external_id)
        aggregate_url = self.client.http_client.config.create_api_url(
            self._AGGREGATE_ENDPOINT.format(streamId=selector.stream.external_id)
        )
        for last_updated_time in StreamIO.last_updated_time_windows(stream, start_ms=start_ms):
            body: dict[str, JsonValue] = {
                "filter": sync_filter,
                "aggregates": {"total": {"count": {}}},
            }
            if last_updated_time is not None:
                body["lastUpdatedTime"] = last_updated_time  # type: ignore[assignment]
            request = RequestMessage(endpoint_url=aggregate_url, method="POST", body_content=body)
            result = self.client.http_client.request_single_retries(request)
            response = result.get_success_or_raise(request)
            data = json.loads(response.body)
            total += int(data["aggregates"]["total"]["count"])
        return total

    def configurations(self, selector: RecordContainerSelector) -> Iterable[StorageIOConfig]:
        spaces = {selector.container.space}
        if selector.instance_spaces:
            spaces.update(selector.instance_spaces)
        space_crud = SpaceCRUD.create_loader(self.client)
        for space in space_crud.retrieve([SpaceId(space=s) for s in spaces]):
            if space.is_global:
                continue
            yield StorageIOConfig(
                kind=SpaceCRUD.kind,
                folder_name=SpaceCRUD.folder_name,
                value=space_crud.dump_resource(space),
                filename=sanitize_filename(space.space),
            )

        container_crud = ContainerCRUD.create_loader(self.client)
        for container in container_crud.retrieve([selector.container.as_id()]):
            if container.is_global:
                continue
            yield StorageIOConfig(
                kind=ContainerCRUD.kind,
                folder_name=ContainerCRUD.folder_name,
                value=container_crud.dump_resource(container),
                filename=sanitize_filename(f"{container.space}_{container.external_id}"),
            )

        stream_crud = StreamIO.create_loader(self.client)
        for stream in stream_crud.retrieve([ExternalId(external_id=selector.stream.external_id)]):
            yield StorageIOConfig(
                kind=StreamIO.kind,
                folder_name=StreamIO.folder_name,
                value=stream_crud.dump_resource(stream),
                filename=sanitize_filename(selector.stream.external_id),
            )

    @staticmethod
    def _build_sync_filter(selector: RecordContainerSelector) -> dict[str, JsonValue]:
        """Build a filter dict for the records sync endpoint."""
        has_data_filter: dict[str, JsonValue] = {
            "hasData": [
                {
                    "type": "container",
                    "space": selector.container.space,
                    "externalId": selector.container.external_id,
                }
            ]
        }
        if not selector.instance_spaces:
            return has_data_filter
        space_filter: dict[str, JsonValue] = {
            "in": {
                "property": ["space"],
                "values": list(selector.instance_spaces),
            }
        }
        return {"and": [has_data_filter, space_filter]}

    def stream_data(
        self,
        selector: RecordContainerSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page]:
        if selector.initialize_cursor is None:
            # This should never happen as we always set initialize_cursor on the selector for download operations.
            raise ToolkitValueError("initialize_cursor must be set on the selector for download operations")
        effective_limit = min(limit, self.MAX_TOTAL_RECORDS) if limit is not None else self.MAX_TOTAL_RECORDS

        sources: list[dict[str, JsonValue]] = [
            {
                "source": {
                    "type": "container",
                    "space": selector.container.space,
                    "externalId": selector.container.external_id,
                },
                "properties": ["*"],
            }
        ]
        sync_filter = self._build_sync_filter(selector)
        initialize_cursor: str | None = selector.initialize_cursor
        cursor: str | None = None

        total = 0
        while True:
            remaining = effective_limit - total
            page_limit = min(self.CHUNK_SIZE, remaining)
            if page_limit <= 0:
                break

            sync_response: RecordSyncResponse = self.client.records.sync(
                stream_external_id=selector.stream.external_id,
                sources=sources,
                filter=sync_filter,
                limit=page_limit,
                initialize_cursor=initialize_cursor,
                cursor=cursor,
            )
            initialize_cursor = None
            total += len(sync_response.items)
            if sync_response.items:
                items = [
                    DataItem(tracking_id=f"{item.space}:{item.external_id}", item=item) for item in sync_response.items
                ]
                page_bookmark: Bookmark = (
                    CursorBookmark(cursor=sync_response.next_cursor)
                    if sync_response.next_cursor is not None
                    else NoBookmark()
                )
                yield self.emit_registered_page(Page(worker_id="main", items=items, bookmark=page_bookmark))
            if not sync_response.has_next or total >= effective_limit:
                break

            cursor = sync_response.next_cursor

    def data_to_json_chunk(
        self, data_chunk: Page[RecordResponse], selector: RecordContainerSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        result = [
            DataItem(tracking_id=item.tracking_id, item=item.item.as_request_resource().dump())
            for item in data_chunk.items
        ]
        return data_chunk.create_from(result)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> RecordRequest:
        return RecordRequest.model_validate(item_json)

    def upload_items(
        self,
        data_chunk: Page[RecordRequest],
        http_client: HTTPClient,
        selector: RecordContainerSelector | None = None,
    ) -> ItemsResultList:
        if selector is None:
            raise ToolkitValueError("Selector must be provided for RecordIO upload_items")
        stream = self._get_stream(selector.stream.external_id)
        endpoint_template = self.UPSERT_ENDPOINT if stream.type == "Mutable" else self.UPLOAD_ENDPOINT
        url = endpoint_template.format(streamId=selector.stream.external_id)
        config = http_client.config
        return http_client.request_items_retries(
            message=ItemsRequest(
                endpoint_url=config.create_api_url(url),
                method="POST",
                items=data_chunk.items,
            )
        )
