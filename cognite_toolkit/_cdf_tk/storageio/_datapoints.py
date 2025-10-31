from collections.abc import Iterable, Sequence

from cognite.client._proto.data_point_insertion_request_pb2 import DataPointInsertionItem
from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem

from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import Page, UploadItem
from ._base import TableUploadableStorageIO
from .selectors import AssetCentricSelector


class DatapointsIO(TableUploadableStorageIO[AssetCentricSelector, DataPointListItem, DataPointInsertionItem]):
    def as_id(self, item: DataPointListItem) -> str:
        raise NotImplementedError()

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[Page]:
        raise NotImplementedError("Download is not yet supported")

    def count(self, selector: AssetCentricSelector) -> int | None:
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: Sequence[DataPointListItem], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError()

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[DataPointInsertionItem]],
        http_client: HTTPClient,
        selector: AssetCentricSelector | None = None,
    ) -> Sequence[HTTPMessage]:
        raise NotImplementedError()

    def row_to_resource(
        self, row: dict[str, JsonVal], selector: AssetCentricSelector | None = None
    ) -> DataPointInsertionItem:
        raise NotImplementedError()

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> DataPointInsertionItem:
        raise NotImplementedError()
