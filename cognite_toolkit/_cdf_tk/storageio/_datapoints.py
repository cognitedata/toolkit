from collections.abc import Iterable, Mapping, Sequence
from typing import ClassVar

from cognite.client._proto.data_point_insertion_request_pb2 import DataPointInsertionItem
from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem

from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import Page, UploadItem
from ._base import TableUploadableStorageIO
from .selectors import DataPointsFileSelector


class DatapointsIO(TableUploadableStorageIO[DataPointsFileSelector, DataPointListItem, DataPointInsertionItem]):
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".csv"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    CHUNK_SIZE = 10_000
    BASE_SELECTOR = DataPointsFileSelector
    KIND = "Datapoints"
    SUPPORTED_READ_FORMATS = frozenset({".csv"})
    UPLOAD_ENDPOINT = "/timeseries/data"
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = None

    def as_id(self, item: DataPointListItem) -> str:
        raise NotImplementedError()

    def stream_data(self, selector: DataPointsFileSelector, limit: int | None = None) -> Iterable[Page]:
        raise NotImplementedError("Download is not yet supported")

    def count(self, selector: DataPointsFileSelector) -> int | None:
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: Sequence[DataPointListItem], selector: DataPointsFileSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError()

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[DataPointInsertionItem]],
        http_client: HTTPClient,
        selector: DataPointsFileSelector | None = None,
    ) -> Sequence[HTTPMessage]:
        raise NotImplementedError()

    def row_to_resource(
        self, row: dict[str, JsonVal], selector: DataPointsFileSelector | None = None
    ) -> DataPointInsertionItem:
        raise NotImplementedError()

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> DataPointInsertionItem:
        raise NotImplementedError()
