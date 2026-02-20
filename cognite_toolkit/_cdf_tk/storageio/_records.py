from collections.abc import Iterable, Sequence
from typing import ClassVar

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsRequest, ItemsResultList
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest, RecordResponse
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import Page, UploadableStorageIO, UploadItem
from .selectors import RecordContainerSelector


class RecordIO(UploadableStorageIO[RecordContainerSelector, RecordResponse, RecordRequest]):  # pyright: ignore[reportInvalidTypeArguments]
    KIND = "Records"
    SUPPORTED_READ_FORMATS: ClassVar[frozenset[str]] = frozenset({".ndjson"})
    UPLOAD_ENDPOINT = "/streams/{streamId}/records"
    CHUNK_SIZE = 1000
    BASE_SELECTOR = RecordContainerSelector

    def as_id(self, item: RecordResponse) -> str:
        return f"{item.space}:{item.external_id}"

    def count(self, selector: RecordContainerSelector) -> int | None:
        return None

    def stream_data(self, selector: RecordContainerSelector, limit: int | None = None) -> Iterable[Page]:
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: Sequence[RecordResponse], selector: RecordContainerSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError()

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
