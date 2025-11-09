from collections.abc import Iterable, Mapping, Sequence
from typing import ClassVar
from datetime import datetime
from cognite.client._proto.data_point_insertion_request_pb2 import DataPointInsertionItem, DataPointInsertionRequest
from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem
from cognite.client._proto.data_points_pb2 import NumericDatapoints, NumericDatapoint, StringDatapoint, StringDatapoints, InstanceId

from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, DataBodyRequest
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import Page, UploadItem
from ._base import TableUploadableStorageIO
from .selectors import DataPointsFileSelector
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError



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
        raise NotImplementedError(f"Download of {type(DatapointsIO).__name__.removesuffix('IO')} is not yet supported")

    def count(self, selector: DataPointsFileSelector) -> int | None:
        raise NotImplementedError(f"Download of {type(DatapointsIO).__name__.removesuffix('IO')} is not yet supported")

    def data_to_json_chunk(
        self, data_chunk: Sequence[DataPointListItem], selector: DataPointsFileSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise ToolkitNotImplementedError(f"Download of {type(DatapointsIO).__name__.removesuffix('IO')} does not support json format.")

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[DataPointInsertionItem]],
        http_client: HTTPClient,
        selector: DataPointsFileSelector | None = None,
    ) -> Sequence[HTTPMessage]:
        request = DataPointInsertionRequest(
            items=[item.item for item in data_chunk]
        )
        return http_client.request_with_retries(
            DataBodyRequest(
                endpoint_url=http_client.config.create_api_url(self.UPLOAD_ENDPOINT),
                method="POST",
                body_content=request.SerializeToString()
            )
        )

    def row_to_resource(
        self, row: dict[str, JsonVal], selector: DataPointsFileSelector | None = None
    ) -> DataPointInsertionItem:
        if selector is None:
            raise ValueError("Selector must be provided to convert row to DataPointInsertionItem.")
        timestamp = row[selector.timestamp_column]
        timestamp_ms = self._convert_to_timestamp_ms(timestamp)

        return DataPointInsertionItem(
            id=int,
            externalId=str,
            instanceId=InstanceId,
            numericDatapoints=NumericDatapoints(datapoints=[NumericDatapoint(timestamp=timestamp_ms, value=, nullValue=False)])
        )

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> DataPointInsertionItem:
        raise ToolkitNotImplementedError(f"Upload of {type(DatapointsIO).__name__.removesuffix('IO')} does not support json format.")

    @staticmethod
    def _convert_to_timestamp_ms(timestamp: JsonVal) -> int:
        if isinstance(timestamp, int):
            return timestamp
        elif isinstance(timestamp, float):
            return int(timestamp * 1000)
        elif isinstance(timestamp, str):
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        else:
            raise ValueError(f"Unsupported timestamp type: {type(timestamp)}")
