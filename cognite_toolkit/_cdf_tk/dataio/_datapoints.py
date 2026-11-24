import itertools
import math
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping
from typing import Any, ClassVar, Literal, cast

from cognite.client._proto.data_point_insertion_request_pb2 import DataPointInsertionItem, DataPointInsertionRequest
from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem, DataPointListResponse
from cognite.client._proto.data_points_pb2 import (
    InstanceId,
    NumericDatapoint,
    NumericDatapoints,
    StateDatapoint,
    StateDatapoints,
    Status,
    StringDatapoint,
    StringDatapoints,
)
from cognite.client.data_classes.filters import Exists
from cognite.client.data_classes.time_series import TimeSeriesProperty
from pydantic import ConfigDict

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier, RequestResource
from cognite_toolkit._cdf_tk.client.http_client import (
    FailedRequest,
    FailedResponse,
    HTTPClient,
    HTTPResult,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsResultList
from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import CountAggregate
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError, ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    _EpochConverter,
    _Float64Converter,
    _Int32Converter,
    _Int64Converter,
    _TextConverter,
)
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.fileio._readers import MultiFileReader
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import Bookmark, DataItem, Page, TableDataIO, TableUploadableDataIO
from .logger import LogEntryV2, Severity
from .selectors import (
    DataPointsDataSetSelector,
    DataPointsFileSelector,
    DataPointsInstanceSpaceSelector,
    DataPointsSelector,
)
from .selectors._datapoints import STATUS_CODE_COLUMN

# The datapoints API supports timestamps from 1900-01-01T00:00:00Z to 2099-12-31T23:59:59.999Z.
MIN_TIMESTAMP_MS = -2208988800000
MAX_TIMESTAMP_MS = 4102444799999


class DatapointsRequestAdapter(RequestResource):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    datapoints: DataPointInsertionRequest

    def dump(
        self,
        camel_case: bool = True,
        exclude_extra: bool = False,
        context: Literal["api", "toolkit"] = "api",
    ) -> dict[str, Any]:
        return {"datapoints": self.datapoints.SerializeToString()}

    def as_id(self) -> Identifier:
        raise NotImplementedError(
            "DatapointsRequestAdapter does not have an identifier. - it wraps multiple timeseries"
        )


class DatapointsIO(
    TableDataIO[DataPointsSelector, DataPointListResponse],
    TableUploadableDataIO[DataPointsSelector, DataPointListResponse, DatapointsRequestAdapter],
):
    CHUNK_SIZE = 10_000
    DOWNLOAD_CHUNK_SIZE = 100
    BASE_SELECTOR = DataPointsSelector
    KIND = "Datapoints"
    UPLOAD_ENDPOINT = "/timeseries/data"
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = None
    # The count is the number of time series, while the download produces one row per datapoint.
    COUNT_NAME = "Time Series Count"
    COUNT_IS_PROGRESS_TOTAL = False
    MAX_CONSECUTIVE_FAILED_PAGES = 5
    MAX_TOTAL_DATAPOINTS = 10_000_000
    MAX_PER_REQUEST_DATAPOINTS = 100_000
    MAX_PER_REQUEST_DATAPOINTS_AGGREGATION = 10_000
    TIMESERIES_VIEW = ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1")

    def __init__(self, client: ToolkitClient, api_format: Literal["request", "response"] = "request") -> None:
        super().__init__(client, api_format=api_format)
        self._epoc_converter = _EpochConverter(nullable=True)
        self._numeric_converter = _Float64Converter(nullable=True)
        self._string_converter = _TextConverter(nullable=True)
        self._state_converter = _Int32Converter(nullable=True)
        self._status_converter = _Int64Converter(nullable=True)
        self._batch_counter = itertools.count(1)

    def get_schema(self, selector: DataPointsSelector) -> list[SchemaColumn] | None:
        if isinstance(selector, DataPointsFileSelector):
            return [
                SchemaColumn(name=selector.timestamp_column, type="epoch"),
                *(
                    SchemaColumn(name=column.column, type="float" if column.dtype == "numeric" else "string")
                    for column in selector.columns
                ),
            ]
        id_columns = [SchemaColumn(name="externalId", type="string")]
        if isinstance(selector, DataPointsInstanceSpaceSelector):
            id_columns.insert(0, SchemaColumn(name="space", type="string"))
        return [
            *id_columns,
            SchemaColumn(name="timestamp", type="epoch"),
            *self._value_columns(selector),
            SchemaColumn(name=STATUS_CODE_COLUMN, type="integer"),
        ]

    @staticmethod
    def _value_columns(selector: DataPointsSelector) -> list[SchemaColumn]:
        data_type = (
            selector.data_type
            if isinstance(selector, DataPointsDataSetSelector | DataPointsInstanceSpaceSelector)
            else "numeric"
        )
        if data_type == "state":
            return [
                SchemaColumn(name="numericValue", type="integer"),
                SchemaColumn(name="stringValue", type="string"),
            ]
        if data_type == "string":
            return [SchemaColumn(name="value", type="string")]
        return [SchemaColumn(name="value", type="float")]

    def stream_data(
        self,
        selector: DataPointsSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page[DataPointListResponse]]:
        if isinstance(selector, DataPointsDataSetSelector):
            identifier_chunks = self._iterate_data_set_timeseries(selector, limit)
        elif isinstance(selector, DataPointsInstanceSpaceSelector):
            identifier_chunks = self._iterate_instance_space_timeseries(selector, limit)
        else:
            raise RuntimeError(
                f"{type(self).__name__} only supports streaming data for DataPointsDataSetSelector and "
                f"DataPointsInstanceSpaceSelector selectors. Got {type(selector).__name__}."
            )
        timeseries_count = self.count(selector)
        if limit is not None:
            timeseries_count = min(timeseries_count or 0, limit)
        limit_per_timeseries = (
            (self.MAX_TOTAL_DATAPOINTS // timeseries_count) if timeseries_count else self.MAX_PER_REQUEST_DATAPOINTS
        )
        limit_per_timeseries = min(limit_per_timeseries, self.MAX_PER_REQUEST_DATAPOINTS)
        time_range = {
            "start": self._query_time(selector.start, MIN_TIMESTAMP_MS),
            "end": self._query_time(selector.end, MAX_TIMESTAMP_MS),
        }
        for identifiers in identifier_chunks:
            yield from self._stream_timeseries_datapoints(identifiers, time_range, limit_per_timeseries)

    def _query_time(self, value: int | str | None, default: int) -> int | str:
        """Converts the selector start/end to a value accepted by the datapoints API.

        The API accepts milliseconds since epoch, 'now', and relative times such as '2d-ago', but not ISO 8601.
        """
        if value is None:
            return default
        if isinstance(value, int):
            return value
        stripped = value.strip()
        if stripped == "now" or stripped.endswith("-ago"):
            return stripped
        try:
            return cast(int, self._epoc_converter.convert(stripped))
        except ValueError as e:
            raise ToolkitValueError(
                f"Invalid time {value!r}. Expected an ISO 8601 timestamp, milliseconds since epoch, 'now', "
                "or a relative time such as '1d-ago'."
            ) from e

    def _iterate_data_set_timeseries(
        self, selector: DataPointsDataSetSelector, limit: int | None
    ) -> Iterable[list[dict[str, JsonVal]]]:
        pages = self.client.timeseries.iterate(
            data_set_external_ids=[selector.data_set_external_id],
            is_string=selector.data_type.lower() == "string",
            advanced_filter=Exists(TimeSeriesProperty.external_id).dump(),
            limit=limit,
        )
        for page in pages:
            for start in range(0, len(page), self.DOWNLOAD_CHUNK_SIZE):
                if timeseries := page[start : start + self.DOWNLOAD_CHUNK_SIZE]:
                    yield [{"id": ts.id} for ts in timeseries]

    def _iterate_instance_space_timeseries(
        self, selector: DataPointsInstanceSpaceSelector, limit: int | None
    ) -> Iterable[list[dict[str, JsonVal]]]:
        pages = self.client.instances.iterate(
            filter=InstanceFilter(instance_type="node", filter=self._instance_space_filter(selector)),
            limit=limit,
        )
        for page in pages:
            for start in range(0, len(page), self.DOWNLOAD_CHUNK_SIZE):
                if nodes := page[start : start + self.DOWNLOAD_CHUNK_SIZE]:
                    yield [{"instanceId": node.as_id().dump(include_instance_type=False)} for node in nodes]

    def _instance_space_filter(self, selector: DataPointsInstanceSpaceSelector) -> dict[str, JsonVal]:
        view = self.TIMESERIES_VIEW
        return {
            "and": [
                {"equals": {"property": ["node", "space"], "value": selector.instance_space}},
                {
                    "equals": {
                        "property": [view.space, f"{view.external_id}/{view.version}", "type"],
                        "value": selector.data_type,
                    }
                },
            ]
        }

    def _stream_timeseries_datapoints(
        self,
        identifiers: list[dict[str, JsonVal]],
        time_range: dict[str, int | str],
        limit_per_timeseries: int,
    ) -> Iterable[Page[DataPointListResponse]]:
        # Bad and uncertain datapoints are included, such that the download is a complete copy of the time series.
        status_args = {"ignoreBadDataPoints": False, "treatUncertainAsBad": False}
        # Aggregation of datapoints per timeseries
        items = [
            {
                **identifier,
                **time_range,
                **status_args,
                "limit": self.MAX_PER_REQUEST_DATAPOINTS_AGGREGATION // len(identifiers),
                "aggregates": ["count"],
                "granularity": "1200mo",
            }
            for identifier in identifiers
        ]
        aggregate_response = self._list_datapoints(items)
        if isinstance(aggregate_response, str):
            self._log_failed_request(len(identifiers), "count datapoints", aggregate_response)
            return
        remaining_by_id: dict[int, int] = {}
        for dp in aggregate_response.items:
            # State time series return their aggregates in stateAggregateDatapoints.
            aggregates = [*dp.aggregateDatapoints.datapoints, *dp.stateAggregateDatapoints.datapoints]
            count = int(sum(agg.count for agg in aggregates))
            # The count aggregate is eventually consistent, so a count of zero is checked with a single datapoint.
            remaining_by_id[dp.id] = min(count, limit_per_timeseries) or 1

        cursor_by_id: dict[int, str] = {}
        downloaded_by_id: Counter[int] = Counter()
        while remaining_by_id:
            batch = self._create_datapoints_batch(remaining_by_id, cursor_by_id, time_range)
            for item in batch:
                remaining_by_id[item["id"]] -= item["limit"]
            data_response = self._list_datapoints([{**item, **status_args, "includeStatus": True} for item in batch])
            if isinstance(data_response, str):
                self._log_failed_request(len(batch), "download datapoints", data_response)
                for item in batch:
                    remaining_by_id.pop(item["id"], None)
                continue
            batch_truncated: list[int] = []
            for response_item in data_response.items:
                ts_id = response_item.id
                downloaded_by_id[ts_id] += self._datapoint_count(response_item)
                if not response_item.nextCursor:
                    remaining_by_id.pop(ts_id, None)
                elif downloaded_by_id[ts_id] >= limit_per_timeseries:
                    batch_truncated.append(ts_id)
                    remaining_by_id.pop(ts_id, None)
                else:
                    cursor_by_id[ts_id] = response_item.nextCursor
                    if remaining_by_id.get(ts_id, 0) <= 0:
                        # The count can be lower than the actual number of datapoints. We trust the cursor
                        # and continue up to the limit per time series.
                        remaining_by_id[ts_id] = limit_per_timeseries - downloaded_by_id[ts_id]
            # Time series that were deleted after the count are not returned.
            returned_ids = {response_item.id for response_item in data_response.items}
            for item in batch:
                if item["id"] not in returned_ids:
                    remaining_by_id.pop(item["id"], None)

            tracking_id = f"datapoints batch {next(self._batch_counter)}"
            page = self.emit_registered_page(Page("Main", [DataItem(tracking_id=tracking_id, item=data_response)]))
            if batch_truncated:
                self.logger.log(
                    LogEntryV2(
                        id=tracking_id,
                        label="Datapoints limit per time series reached",
                        severity=Severity.warning,
                        message=f"Downloaded {limit_per_timeseries:,} datapoints for {len(batch_truncated):,} time "
                        "series that have more datapoints. The remaining datapoints were not downloaded.",
                    )
                )
            yield page

    @staticmethod
    def _datapoint_count(item: DataPointListItem) -> int:
        return (
            len(item.numericDatapoints.datapoints)
            + len(item.stringDatapoints.datapoints)
            + len(item.stateDatapoints.datapoints)
        )

    def _log_failed_request(self, timeseries_count: int, action: str, error: str) -> None:
        tracking_id = f"datapoints batch {next(self._batch_counter)}"
        self.logger.register([tracking_id])
        self.logger.log(
            LogEntryV2(
                id=tracking_id,
                label=f"Failed to {action}",
                severity=Severity.failure,
                message=f"Failed to {action} for {timeseries_count:,} time series: {error}",
            )
        )

    def _create_datapoints_batch(
        self,
        remaining_by_id: dict[int, int],
        cursor_by_id: dict[int, str],
        time_range: dict[str, int | str],
    ) -> list[dict[str, Any]]:
        batch: list[dict[str, Any]] = []
        batch_count = 0
        for ts_id, remaining in remaining_by_id.items():
            ts_limit = min(remaining, self.MAX_PER_REQUEST_DATAPOINTS - batch_count)
            if ts_limit <= 0:
                break
            item: dict[str, Any] = {"id": ts_id, **time_range, "limit": ts_limit}
            if cursor := cursor_by_id.get(ts_id):
                item["cursor"] = cursor
            batch.append(item)
            batch_count += ts_limit
        return batch

    def _list_datapoints(self, items: list[dict[str, Any]]) -> DataPointListResponse | str:
        """Lists datapoints. Returns the error message if the request fails."""
        response = self.client.http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self.client.config.create_api_url("/timeseries/data/list"),
                method="POST",
                accept="application/protobuf",
                content_type="application/json",
                body_content={"items": items, "ignoreUnknownIds": True},  # type: ignore[dict-item]
            )
        )
        if isinstance(response, SuccessResponse):
            return DataPointListResponse.FromString(response.content)
        if isinstance(response, FailedResponse):
            return f"HTTP {response.status_code}: {response.error.full_message}"
        if isinstance(response, FailedRequest):
            return response.error
        return f"Unexpected response {type(response).__name__}"

    def count(self, selector: DataPointsSelector) -> int | None:
        if isinstance(selector, DataPointsDataSetSelector):
            count = self.client.timeseries.count(
                filter={
                    "dataSetIds": [{"externalId": selector.data_set_external_id}],
                    "isString": selector.data_type == "string",
                },
                # We only want time series that have externalID set.
                advanced_filter=Exists(TimeSeriesProperty.external_id).dump(),
            )
        elif isinstance(selector, DataPointsInstanceSpaceSelector):
            result = self.client.instances.aggregate(
                self.TIMESERIES_VIEW,
                aggregates=[CountAggregate(property="externalId")],
                instance_type="node",
                filter=self._instance_space_filter(selector),
            )
            count = int(result.items[0].aggregates[0].value or 0) if result.items and result.items[0].aggregates else 0
        else:
            return None
        if count == 0 and self._has_timeseries(selector):
            # The count aggregates are eventually consistent, so recently created time series are not counted.
            return None
        return count

    def _has_timeseries(self, selector: DataPointsDataSetSelector | DataPointsInstanceSpaceSelector) -> bool:
        if isinstance(selector, DataPointsDataSetSelector):
            chunks = self._iterate_data_set_timeseries(selector, limit=1)
        else:
            chunks = self._iterate_instance_space_timeseries(selector, limit=1)
        return any(chunks)

    def data_to_json_chunk(
        self, data_chunk: Page[DataPointListResponse], selector: DataPointsSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        raise ToolkitNotImplementedError(
            f"Download of {type(self).__name__.removesuffix('IO')} does not support json format."
        )

    def data_to_row(
        self, data_chunk: Page[DataPointListResponse], selector: DataPointsSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        result: list[DataItem[dict[str, JsonVal]]] = []
        for data_item in data_chunk.items:
            response = data_item.item
            for item in response.items:
                instance_id: dict[str, JsonVal] = {
                    "space": item.instanceId.space,
                    "externalId": item.instanceId.externalId,
                }
                timeseries_id = (
                    instance_id
                    if isinstance(selector, DataPointsInstanceSpaceSelector)
                    else {"externalId": item.externalId}
                )
                for dp in [*item.numericDatapoints.datapoints, *item.stringDatapoints.datapoints]:
                    result.append(
                        DataItem(
                            tracking_id=data_item.tracking_id,
                            item={
                                **timeseries_id,
                                "timestamp": dp.timestamp,
                                "value": None if dp.nullValue else dp.value,
                                STATUS_CODE_COLUMN: self._status_code(dp),
                            },
                        )
                    )
                for state_dp in item.stateDatapoints.datapoints:
                    result.append(
                        DataItem(
                            tracking_id=data_item.tracking_id,
                            item={
                                **instance_id,
                                "timestamp": state_dp.timestamp,
                                "numericValue": state_dp.numericValue if state_dp.HasField("numericValue") else None,
                                "stringValue": state_dp.stringValue if state_dp.HasField("stringValue") else None,
                                STATUS_CODE_COLUMN: self._status_code(state_dp),
                            },
                        )
                    )
        return data_chunk.create_from(result)

    @staticmethod
    def _status_code(dp: NumericDatapoint | StringDatapoint | StateDatapoint) -> int | None:
        # Good (code 0) is the default and is left empty to keep the files small.
        if dp.HasField("status") and dp.status.code:
            return dp.status.code
        return None

    def json_to_row(
        self, item_json: dict[str, JsonVal], selector: DataPointsSelector | None = None
    ) -> dict[str, JsonVal]:
        raise NotImplementedError(f"json_to_row not implemented for {type(self).__name__}")

    def upload_items(
        self,
        data_chunk: Page[DatapointsRequestAdapter],
        http_client: HTTPClient,
        selector: DataPointsSelector | None = None,
    ) -> ItemsResultList:
        results = ItemsResultList()
        for item in data_chunk.items:
            request = item.item.datapoints
            response = self._insert_datapoints(http_client, request)
            missing = self._missing_references(response, request)
            if missing:
                # The API rejects the entire request if any time series is missing. Retry without them.
                request = DataPointInsertionRequest(
                    items=[dp_item for dp_item in request.items if self._reference(dp_item) not in missing]
                )
                self.logger.log(
                    LogEntryV2(
                        id=item.tracking_id,
                        label="Missing time series",
                        severity=Severity.warning,
                        message=f"Skipped datapoints for {len(missing):,} time series that do not exist: "
                        f"{humanize_collection([str(ref[-1]) for ref in missing])}",
                    )
                )
                if not request.items:
                    continue
                response = self._insert_datapoints(http_client, request)
            results.append(response.as_item_response(item.tracking_id))
        return results

    def _insert_datapoints(self, http_client: HTTPClient, request: DataPointInsertionRequest) -> HTTPResult:
        return http_client.request_single_retries(
            RequestMessage(
                endpoint_url=http_client.config.create_api_url(self.UPLOAD_ENDPOINT),
                method="POST",
                content_type="application/protobuf",
                data_content=request.SerializeToString(),
            )
        )

    @classmethod
    def _missing_references(
        cls, response: HTTPResult, request: DataPointInsertionRequest
    ) -> set[tuple[str | int, ...]]:
        if not isinstance(response, FailedResponse) or not response.error.missing:
            return set()
        requested = {cls._reference(item) for item in request.items}
        missing: set[tuple[str | int, ...]] = set()
        for identifier in response.error.missing:
            if isinstance(instance_id := identifier.get("instanceId"), dict):
                missing.add(("instanceId", str(instance_id.get("space")), str(instance_id.get("externalId"))))
            elif "externalId" in identifier:
                missing.add(("externalId", str(identifier["externalId"])))
            elif isinstance(internal_id := identifier.get("id"), int):
                missing.add(("id", internal_id))
        # Only retry if the missing time series are part of the request, otherwise the retry fails the same way.
        return missing if missing <= requested else set()

    @staticmethod
    def _reference(item: DataPointInsertionItem) -> tuple[str | int, ...]:
        reference = item.WhichOneof("timeSeriesReference")
        if reference == "id":
            return ("id", item.id)
        if reference == "externalId":
            return ("externalId", item.externalId)
        return ("instanceId", item.instanceId.space, item.instanceId.externalId)

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: DataPointsSelector | None = None
    ) -> DatapointsRequestAdapter:
        if selector is None:
            raise ValueError("Selector must be provided to convert row to DataPointInsertionItem.")
        # We assume that the row was read using the read_chunks method.
        rows = cast(dict[str, list[Any]], row)
        if isinstance(selector, DataPointsFileSelector):
            datapoints_items = self._rows_to_datapoint_items_file_selector(rows, selector, source_id)
        elif isinstance(selector, DataPointsDataSetSelector):
            datapoints_items = self._rows_to_datapoint_items_data_set_selector(rows, selector, source_id)
        elif isinstance(selector, DataPointsInstanceSpaceSelector):
            datapoints_items = self._rows_to_datapoint_items_instance_space_selector(rows, selector, source_id)
        else:
            raise RuntimeError(
                f"Unsupported selector type {type(selector).__name__} for {type(self).__name__}. Trying to transform {source_id!r} from rows to DataPointInsertionRequest."
            )
        return DatapointsRequestAdapter(datapoints=DataPointInsertionRequest(items=datapoints_items))

    def _rows_to_datapoint_items_file_selector(
        self, rows: dict[str, list[Any]], selector: DataPointsFileSelector, source_id: str
    ) -> list[DataPointInsertionItem]:
        if selector.timestamp_column not in rows:
            raise RuntimeError(f"Timestamp column '{selector.timestamp_column}' not found.")

        skipped: Counter[str] = Counter()
        timestamps: list[int | None] = []
        for raw_timestamp in rows[selector.timestamp_column]:
            try:
                timestamp = cast(int | None, self._epoc_converter.convert(raw_timestamp))
            except ValueError:
                timestamp = None
            if timestamp is None:
                skipped[f"rows without a valid timestamp (column {selector.timestamp_column!r})"] += 1
            timestamps.append(timestamp)

        datapoints_items: list[DataPointInsertionItem] = []
        for col, values in rows.items():
            if col == selector.timestamp_column:
                continue
            column = selector.id_by_column.get(col)
            if column is None:
                skipped[f"values in column {col!r}, which is not in the selector"] += sum(
                    value is not None for value in values
                )
                continue
            if column.dtype not in ("numeric", "string"):
                raise RuntimeError(f"Unsupported dtype {column.dtype} for column {col}.")
            converter = self._numeric_converter if column.dtype == "numeric" else self._string_converter
            datapoints: list[Any] = []
            for timestamp, raw_value in zip(timestamps, values, strict=True):
                # Wide files are sparse, an empty cell means there is no datapoint for this time series.
                if timestamp is None or raw_value is None:
                    continue
                try:
                    value = converter.convert(raw_value)
                except ValueError:
                    skipped[f"values that could not be converted (column {col!r})"] += 1
                    continue
                if value is None:
                    continue
                if column.dtype == "numeric":
                    datapoints.append(NumericDatapoint(timestamp=timestamp, value=cast(float, value)))
                else:
                    datapoints.append(StringDatapoint(timestamp=timestamp, value=cast(str, value)))
            if not datapoints:
                continue
            args: dict[str, Any] = column.as_wrapped_id()
            if column.dtype == "numeric":
                args["numericDatapoints"] = NumericDatapoints(datapoints=datapoints)
            else:
                args["stringDatapoints"] = StringDatapoints(datapoints=datapoints)
            datapoints_items.append(DataPointInsertionItem(**args))

        self._log_skipped_datapoints(source_id, skipped)
        return datapoints_items

    def _rows_to_datapoint_items_data_set_selector(
        self, rows: dict[str, list[Any]], selector: DataPointsDataSetSelector, source_id: str
    ) -> list[DataPointInsertionItem]:
        if missing := selector.required_columns - set(rows):
            raise RuntimeError(f"Columns {humanize_collection(missing)} not found.")
        return self._rows_to_value_datapoint_items(
            [(external_id,) for external_id in rows["externalId"]],
            rows,
            selector.data_type,
            source_id,
            to_id=lambda key: {"externalId": key[0]},
        )

    def _rows_to_datapoint_items_instance_space_selector(
        self, rows: dict[str, list[Any]], selector: DataPointsInstanceSpaceSelector, source_id: str
    ) -> list[DataPointInsertionItem]:
        if missing := selector.required_columns - set(rows):
            raise RuntimeError(f"Columns {humanize_collection(missing)} not found.")
        if selector.data_type == "state":
            return self._rows_to_state_datapoint_items(rows, source_id)
        return self._rows_to_value_datapoint_items(
            list(zip(rows["space"], rows["externalId"], strict=True)),
            rows,
            selector.data_type,
            source_id,
            to_id=lambda key: {"instanceId": InstanceId(space=key[0], externalId=key[1])},
        )

    def _rows_to_value_datapoint_items(
        self,
        keys: list[tuple[str, ...]],
        rows: dict[str, list[Any]],
        data_type: Literal["numeric", "string"],
        source_id: str,
        to_id: Callable[[tuple[str, ...]], dict[str, Any]],
    ) -> list[DataPointInsertionItem]:
        converter = self._numeric_converter if data_type == "numeric" else self._string_converter
        datapoint_cls = NumericDatapoint if data_type == "numeric" else StringDatapoint
        status_codes = rows.get(STATUS_CODE_COLUMN) or [None] * len(keys)
        skipped: Counter[str] = Counter()
        datapoints_by_key: dict[tuple[str, ...], list[Any]] = defaultdict(list)
        # Converting row by row ensures that a value that fails conversion cannot shift the following
        # values onto the wrong timestamps.
        for key, raw_timestamp, raw_value, raw_status in zip(
            keys, rows["timestamp"], rows["value"], status_codes, strict=True
        ):
            try:
                timestamp = cast(int | None, self._epoc_converter.convert(raw_timestamp))
                value = converter.convert(raw_value)
                status_code = cast(int | None, self._status_converter.convert(raw_status))
            except ValueError:
                skipped["rows that could not be converted"] += 1
                continue
            if any(part is None for part in key):
                skipped["rows without a time series identifier"] += 1
            elif timestamp is None:
                skipped["rows without a timestamp"] += 1
            elif value is None and not status_code:
                # Only datapoints with a bad status can be without a value.
                skipped["rows without a value"] += 1
            else:
                args: dict[str, Any] = {"timestamp": timestamp}
                if status_code:
                    args["status"] = Status(code=status_code)
                if value is None:
                    args["nullValue"] = True
                else:
                    args["value"] = value
                datapoints_by_key[key].append(datapoint_cls(**args))
        self._log_skipped_datapoints(source_id, skipped)

        if data_type == "numeric":
            return [
                DataPointInsertionItem(**to_id(key), numericDatapoints=NumericDatapoints(datapoints=dps))
                for key, dps in datapoints_by_key.items()
            ]
        return [
            DataPointInsertionItem(**to_id(key), stringDatapoints=StringDatapoints(datapoints=dps))
            for key, dps in datapoints_by_key.items()
        ]

    def _rows_to_state_datapoint_items(
        self, rows: dict[str, list[Any]], source_id: str
    ) -> list[DataPointInsertionItem]:
        status_codes = rows.get(STATUS_CODE_COLUMN) or [None] * len(rows["timestamp"])
        skipped: Counter[str] = Counter()
        datapoints_by_instance_id: dict[tuple[str, str], list[StateDatapoint]] = defaultdict(list)
        for space, external_id, timestamp, numeric_value, string_value, raw_status in zip(
            rows["space"],
            rows["externalId"],
            rows["timestamp"],
            rows["numericValue"],
            rows["stringValue"],
            status_codes,
            strict=True,
        ):
            try:
                converted_timestamp = cast(int | None, self._epoc_converter.convert(timestamp))
                converted_numeric = cast(int | None, self._state_converter.convert(numeric_value))
                converted_string = cast(str | None, self._string_converter.convert(string_value))
                status_code = cast(int | None, self._status_converter.convert(raw_status))
            except ValueError:
                skipped["rows that could not be converted"] += 1
                continue
            if space is None or external_id is None:
                skipped["rows without a time series identifier"] += 1
            elif converted_timestamp is None:
                skipped["rows without a timestamp"] += 1
            elif converted_numeric is None and converted_string is None and not status_code:
                # Only datapoints with a bad status can be without a value.
                skipped["rows without a value"] += 1
            else:
                datapoints_by_instance_id[(space, external_id)].append(
                    StateDatapoint(
                        timestamp=converted_timestamp,
                        numericValue=converted_numeric,
                        stringValue=converted_string,
                        status=Status(code=status_code) if status_code else None,
                    )
                )
        self._log_skipped_datapoints(source_id, skipped)

        return [
            DataPointInsertionItem(
                instanceId=InstanceId(space=space, externalId=external_id),
                stateDatapoints=StateDatapoints(datapoints=datapoints),
            )
            for (space, external_id), datapoints in datapoints_by_instance_id.items()
        ]

    def _log_skipped_datapoints(self, source_id: str, skipped: Counter[str]) -> None:
        entries = [
            LogEntryV2(
                id=source_id,
                label=f"Skipped {reason}",
                severity=Severity.warning,
                message=f"Skipped {count:,} {reason}.",
            )
            for reason, count in skipped.items()
            if count
        ]
        if entries:
            self.logger.log(entries)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> DatapointsRequestAdapter:
        raise ToolkitNotImplementedError(
            f"Upload of {type(self).__name__.removesuffix('IO')} does not support json format."
        )

    @classmethod
    def read_chunks(cls, reader: MultiFileReader, selector: DataPointsSelector) -> Iterable[Page[dict[str, JsonVal]]]:
        if not reader.is_table:
            raise RuntimeError(f"{cls.__name__} can only read from TableReader instances.")
        rows_per_chunk = cls._rows_per_chunk(selector)
        current_file = None
        column_names: list[str] = []
        batch: dict[str, list[Any]] = {}
        start_row = row_no = 0
        for chunk in reader.read_chunks():
            if reader.current_file != current_file:
                # Chunks never span files, such that the tracking IDs point to rows in a single file.
                if current_file is not None and row_no >= start_row:
                    yield cls._create_chunk_page(current_file.name, start_row, row_no, batch)
                current_file = reader.current_file
                column_names = list(chunk.keys())
                cls._validate_columns(column_names, selector, current_file.name)
                batch = {col: [] for col in column_names}
                start_row, row_no = 1, 0
            row_no += 1
            for col in column_names:
                batch[col].append(chunk.get(col))
            if row_no - start_row + 1 >= rows_per_chunk:
                yield cls._create_chunk_page(current_file.name, start_row, row_no, batch)
                start_row = row_no + 1
                batch = {col: [] for col in column_names}
        if current_file is not None and row_no >= start_row:
            yield cls._create_chunk_page(current_file.name, start_row, row_no, batch)

    @staticmethod
    def _create_chunk_page(
        file_name: str, start_row: int, end_row: int, batch: dict[str, list[Any]]
    ) -> Page[dict[str, JsonVal]]:
        # We cannot guarantee JsonVal here, but that is handled later in the processing pipeline.
        return Page(
            worker_id="main",
            items=[DataItem(tracking_id=f"{file_name} rows {start_row} to {end_row}", item=batch)],  # type: ignore[arg-type]
        )

    @staticmethod
    def _validate_columns(column_names: list[str], selector: DataPointsSelector, file_name: str) -> None:
        if not isinstance(selector, DataPointsDataSetSelector | DataPointsInstanceSpaceSelector):
            return
        columns = set(column_names)
        if selector.required_columns <= columns <= selector.required_columns | selector.optional_columns:
            return
        raise RuntimeError(
            f"When uploading datapoints using a {selector.type} manifest, you must have exactly the "
            f"columns: {humanize_collection(selector.required_columns)} in the file, and optionally "
            f"{humanize_collection(selector.optional_columns)}. Got {humanize_collection(column_names)} in {file_name}."
        )

    @classmethod
    def _rows_per_chunk(cls, selector: DataPointsSelector) -> int:
        # Long files have one datapoint per row, while wide files have one datapoint per time series column.
        datapoints_per_row = len(selector.columns) if isinstance(selector, DataPointsFileSelector) else 1
        return max(1, cls.CHUNK_SIZE // max(1, datapoints_per_row))

    @classmethod
    def count_items(cls, reader: MultiFileReader, selector: DataPointsSelector | None = None) -> int:
        """Counts the number of chunks read_chunks yields, which is what the upload progress bar tracks."""
        if selector is None:
            raise ValueError("A selector is required to correctly count datapoint items.")
        rows_per_chunk = cls._rows_per_chunk(selector)
        return sum(
            math.ceil(reader.create_reader(input_file).count() / rows_per_chunk) for input_file in reader.input_files
        )
