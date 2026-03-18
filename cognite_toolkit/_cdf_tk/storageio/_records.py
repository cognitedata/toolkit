import json
import time
from collections.abc import Iterable, Sequence
from datetime import timedelta
from typing import ClassVar

from pydantic import Field, TypeAdapter

from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsRequest, ItemsResultList
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest, RecordResponse
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import ContainerCRUD, SpaceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.streams import StreamCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename
from cognite_toolkit._cdf_tk.utils.time import timestamp_to_ms
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import StorageIOConfig
from ._base import Bookmark, ConfigurableStorageIO, Page, UploadableStorageIO, UploadItem
from .selectors import RecordContainerSelector


class RecordSyncResponse(PagedResponse[RecordResponse]):
    """Response from the Records /sync endpoint.

    Extends PagedResponse with `has_next` because the records service deviates from
    the typical API behavior. The records service may return fewer items than requested
    per page in case of large records being queried. We must therefore rely on `has_next`
    rather than the number of returned items to decide whether to continue pagination.
    """

    has_next: bool = Field(alias="hasNext")


class RecordIO(
    ConfigurableStorageIO[RecordContainerSelector, RecordResponse],
    UploadableStorageIO[RecordContainerSelector, RecordResponse, RecordRequest],
):  # pyright: ignore[reportInvalidTypeArguments]
    KIND = "Records"
    SUPPORTED_DOWNLOAD_FORMATS: ClassVar[frozenset[str]] = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS: ClassVar[frozenset[str]] = frozenset({".gz"})
    SUPPORTED_READ_FORMATS: ClassVar[frozenset[str]] = frozenset({".ndjson"})
    UPLOAD_ENDPOINT = "/streams/{streamId}/records"
    SYNC_ENDPOINT = "/streams/{streamId}/records/sync"
    AGGREGATE_ENDPOINT = "/streams/{streamId}/records/aggregate"
    # TODO: Replace with adaptive limit that targets ~3MB uncompressed response size
    CHUNK_SIZE = 500
    MAX_TOTAL_RECORDS = 1_000_000
    BASE_SELECTOR = RecordContainerSelector
    _TIMEDELTA_ADAPTER: ClassVar[TypeAdapter[timedelta]] = TypeAdapter(timedelta)

    def as_id(self, item: RecordResponse) -> str:
        return f"{item.space}:{item.external_id}"

    def _get_max_filtering_interval_ms(self, stream_external_id: str) -> int | None:
        """Get the stream's maxFilteringInterval in ms, returning None for mutable streams."""
        streams = self.client.streams.retrieve([ExternalId(external_id=stream_external_id)])
        stream = streams[0] if streams else None
        if stream is None or stream.type == "Mutable":
            return None
        if stream.settings and stream.settings.limits.max_filtering_interval:
            td = self._TIMEDELTA_ADAPTER.validate_python(stream.settings.limits.max_filtering_interval)
            return int(td.total_seconds() * 1000)
        return None

    def count(self, selector: RecordContainerSelector) -> int | None:
        if selector.initialize_cursor is None:
            raise ToolkitValueError("initialize_cursor must be set on the selector for download operations")
        url = self.AGGREGATE_ENDPOINT.format(streamId=selector.stream.external_id)
        start_ms = timestamp_to_ms(selector.initialize_cursor)
        now_ms = int(time.time() * 1000)
        sync_filter = self._build_sync_filter(selector)
        # Immutable streams enforce a maxFilteringInterval for the lastUpdatedTime filter which is
        # specified in its settings, so we split the time range for downloading records into windows that
        # fit within that limit. Mutable streams have no such restriction, so we query the full range in one go.
        max_interval_ms = self._get_max_filtering_interval_ms(selector.stream.external_id) or (now_ms - start_ms)

        total = 0
        window_start = start_ms
        while window_start < now_ms:
            window_end = min(window_start + max_interval_ms, now_ms)
            body: dict[str, object] = {
                "filter": sync_filter,
                "lastUpdatedTime": {"gte": window_start, "lt": window_end},
                "aggregates": {"total": {"count": {}}},
            }
            request = RequestMessage(
                endpoint_url=self.client.config.create_api_url(url),
                method="POST",
                body_content=body,  # type: ignore[arg-type]
            )
            result = self.client.http_client.request_single_retries(request)
            response = result.get_success_or_raise(request)
            data = json.loads(response.body)
            total += int(data["aggregates"]["total"]["count"])
            window_start = window_end
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

        stream_crud = StreamCRUD.create_loader(self.client)
        for stream in stream_crud.retrieve([ExternalId(external_id=selector.stream.external_id)]):
            yield StorageIOConfig(
                kind=StreamCRUD.kind,
                folder_name=StreamCRUD.folder_name,
                value=stream_crud.dump_resource(stream),
                filename=sanitize_filename(selector.stream.external_id),
            )

    @staticmethod
    def _build_sync_filter(selector: RecordContainerSelector) -> dict[str, object]:
        """Build a filter dict for the records sync endpoint."""
        has_data_filter: dict[str, object] = {
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
        space_filter: dict[str, object] = {
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
        url = self.SYNC_ENDPOINT.format(streamId=selector.stream.external_id)

        body: dict[str, object] = {
            "sources": [
                {
                    "source": {
                        "type": "container",
                        "space": selector.container.space,
                        "externalId": selector.container.external_id,
                    },
                    "properties": ["*"],
                }
            ],
            "filter": self._build_sync_filter(selector),
            "initializeCursor": selector.initialize_cursor,
            "limit": min(self.CHUNK_SIZE, effective_limit),
        }

        total = 0
        while True:
            remaining = effective_limit - total
            page_limit = min(self.CHUNK_SIZE, remaining)
            if page_limit <= 0:
                break
            body["limit"] = page_limit
            request = RequestMessage(
                endpoint_url=self.client.config.create_api_url(url),
                method="POST",
                body_content=body,  # type: ignore[arg-type]
            )
            result = self.client.http_client.request_single_retries(request)
            response = result.get_success_or_raise(request)

            sync_response = RecordSyncResponse.model_validate_json(response.body)
            total += len(sync_response.items)
            if sync_response.items:
                yield Page(
                    worker_id="main", items=sync_response.items, bookmark=Bookmark(cursor=sync_response.next_cursor)
                )  # pyright: ignore[reportArgumentType]
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
