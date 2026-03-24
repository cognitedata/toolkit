import json
from collections.abc import Iterable
from typing import ClassVar

from pydantic import Field

from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsRequest, ItemsResultList
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest, RecordResponse
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import ContainerCRUD, SpaceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.streams import StreamCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import StorageIOConfig
from ._base import Bookmark, ConfigurableStorageIO, DataItem, Page, UploadableStorageIO
from .progress import CursorBookmark, NoBookmark
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
    def count(self, selector: RecordContainerSelector) -> int | None:
        url = self.AGGREGATE_ENDPOINT.format(streamId=selector.stream.external_id)
        sync_filter = self._build_sync_filter(selector)
        total = 0
        stream_crud = StreamCRUD.create_loader(self.client)
        for last_updated_time in stream_crud.iter_last_updated_time_windows(selector.stream.external_id):
            body: dict[str, object] = {
                "filter": sync_filter,
                "aggregates": {"total": {"count": {}}},
            }
            if last_updated_time is not None:
                body["lastUpdatedTime"] = last_updated_time
            request = RequestMessage(
                endpoint_url=self.client.config.create_api_url(url),
                method="POST",
                body_content=body,  # type: ignore[arg-type]
            )
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
                items = [
                    DataItem(tracking_id=f"{item.space}:{item.external_id}", item=item) for item in sync_response.items
                ]
                page_bookmark: Bookmark = (
                    CursorBookmark(cursor=sync_response.next_cursor)
                    if sync_response.next_cursor is not None
                    else NoBookmark()
                )
                yield Page(worker_id="main", items=items, bookmark=page_bookmark)
            if not sync_response.has_next or total >= effective_limit:
                break

            body.pop("initializeCursor", None)
            body["cursor"] = sync_response.next_cursor

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
        url = self.UPLOAD_ENDPOINT.format(streamId=selector.stream.external_id)
        config = http_client.config
        return http_client.request_items_retries(
            message=ItemsRequest(
                endpoint_url=config.create_api_url(url),
                method="POST",
                items=data_chunk.items,
            )
        )
