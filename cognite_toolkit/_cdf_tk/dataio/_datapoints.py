import itertools
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from typing import Any, ClassVar, Literal, cast

from cognite.client._proto.data_point_insertion_request_pb2 import DataPointInsertionItem, DataPointInsertionRequest
from cognite.client._proto.data_point_list_response_pb2 import DataPointListResponse
from cognite.client._proto.data_points_pb2 import (
    InstanceId,
    NumericDatapoint,
    NumericDatapoints,
    StateDatapoint,
    StateDatapoints,
    StringDatapoint,
    StringDatapoints,
)
from cognite.client.data_classes.filters import Exists
from cognite.client.data_classes.time_series import TimeSeriesProperty
from pydantic import ConfigDict

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier, RequestResource
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsResultList
from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import CountAggregate
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    _EpochConverter,
    _Float64Converter,
    _Int32Converter,
    _TextConverter,
    _ValueConverter,
)
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.fileio._readers import MultiFileReader
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import Bookmark, DataItem, Page, TableDataIO, TableUploadableDataIO
from .selectors import (
    DataPointsDataSetSelector,
    DataPointsFileSelector,
    DataPointsInstanceSpaceSelector,
    DataPointsSelector,
)


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
    MAX_TOTAL_DATAPOINTS = 10_000_000
    MAX_PER_REQUEST_DATAPOINTS = 100_000
    MAX_PER_REQUEST_DATAPOINTS_AGGREGATION = 10_000
    TIMESERIES_VIEW = ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1")

    def __init__(self, client: ToolkitClient, api_format: Literal["request", "response"] = "request") -> None:
        super().__init__(client, api_format=api_format)
        self._warned_columns: set[str] = set()
        self._epoc_converter = _EpochConverter(nullable=True)
        self._numeric_converter = _Float64Converter(nullable=True)
        self._string_converter = _TextConverter(nullable=True)
        self._state_converter = _Int32Converter(nullable=True)
        self._batch_counter = itertools.count(1)

    def get_schema(self, selector: DataPointsSelector) -> list[SchemaColumn] | None:
        id_columns = [SchemaColumn(name="externalId", type="string")]
        if isinstance(selector, DataPointsInstanceSpaceSelector):
            id_columns.insert(0, SchemaColumn(name="space", type="string"))
        return [
            *id_columns,
            SchemaColumn(name="timestamp", type="epoch"),
            *self._value_columns(selector),
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
        for identifiers in identifier_chunks:
            yield from self._stream_timeseries_datapoints(identifiers, selector, limit_per_timeseries)

    def _iterate_data_set_timeseries(
        self, selector: DataPointsDataSetSelector, limit: int | None
    ) -> Iterable[list[dict[str, Any]]]:
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
    ) -> Iterable[list[dict[str, Any]]]:
        pages = self.client.instances.iterate(
            filter=InstanceFilter(instance_type="node", filter=self._instance_space_filter(selector)),
            limit=limit,
        )
        for page in pages:
            for start in range(0, len(page), self.DOWNLOAD_CHUNK_SIZE):
                if nodes := page[start : start + self.DOWNLOAD_CHUNK_SIZE]:
                    yield [{"instanceId": node.as_id().dump(include_instance_type=False)} for node in nodes]

    def _instance_space_filter(self, selector: DataPointsInstanceSpaceSelector) -> dict[str, Any]:
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
        identifiers: list[dict[str, Any]],
        selector: DataPointsDataSetSelector | DataPointsInstanceSpaceSelector,
        limit_per_timeseries: int,
    ) -> Iterable[Page[DataPointListResponse]]:
        config = self.client.config
        # Aggregation of datapoints per timeseries
        items = [
            {
                **identifier,
                "start": selector.start,
                "end": selector.end,
                "limit": self.MAX_PER_REQUEST_DATAPOINTS_AGGREGATION // len(identifiers),
                "aggregates": ["count"],
                "granularity": "1200mo",
            }
            for identifier in identifiers
        ]
        response = self.client.http_client.request_single_retries(
            RequestMessage(
                endpoint_url=config.create_api_url("/timeseries/data/list"),
                method="POST",
                accept="application/protobuf",
                content_type="application/json",
                body_content={"items": items},  # type: ignore[dict-item]
            )
        )
        if not isinstance(response, SuccessResponse):
            return
        aggregate_response: DataPointListResponse = DataPointListResponse.FromString(response.content)
        remaining_by_id: dict[int, int] = {}
        for dp in aggregate_response.items:
            # State time series return their aggregates in stateAggregateDatapoints.
            aggregates = [*dp.aggregateDatapoints.datapoints, *dp.stateAggregateDatapoints.datapoints]
            if count := int(sum(agg.count for agg in aggregates)):
                remaining_by_id[dp.id] = min(count, limit_per_timeseries)

        cursor_by_id: dict[int, str] = {}
        while remaining_by_id:
            batch = self._create_datapoints_batch(remaining_by_id, cursor_by_id, selector)
            for item in batch:
                remaining_by_id[item["id"]] -= item["limit"]
            data_response = self._fetch_datapoints_batch(batch, config)
            if data_response is None:
                for item in batch:
                    remaining_by_id.pop(item["id"], None)
                continue
            for response_item in data_response.items:
                if response_item.nextCursor and remaining_by_id.get(response_item.id, 0) > 0:
                    cursor_by_id[response_item.id] = response_item.nextCursor
                else:
                    remaining_by_id.pop(response_item.id, None)
            tracking_id = f"datapoints batch {next(self._batch_counter)}"
            yield self.emit_registered_page(Page("Main", [DataItem(tracking_id=tracking_id, item=data_response)]))

    def _create_datapoints_batch(
        self,
        remaining_by_id: dict[int, int],
        cursor_by_id: dict[int, str],
        selector: DataPointsDataSetSelector | DataPointsInstanceSpaceSelector,
    ) -> list[dict[str, Any]]:
        batch: list[dict[str, Any]] = []
        batch_count = 0
        for ts_id, remaining in remaining_by_id.items():
            ts_limit = min(remaining, self.MAX_PER_REQUEST_DATAPOINTS - batch_count)
            if ts_limit <= 0:
                break
            item: dict[str, Any] = {"id": ts_id, "start": selector.start, "end": selector.end, "limit": ts_limit}
            if cursor := cursor_by_id.get(ts_id):
                item["cursor"] = cursor
            batch.append(item)
            batch_count += ts_limit
        return batch

    def _fetch_datapoints_batch(self, batch: list[dict[str, Any]], config: Any) -> DataPointListResponse | None:
        response = self.client.http_client.request_single_retries(
            RequestMessage(
                endpoint_url=config.create_api_url("/timeseries/data/list"),
                method="POST",
                accept="application/protobuf",
                content_type="application/json",
                body_content={"items": batch},  # type: ignore[dict-item]
            )
        )
        if not isinstance(response, SuccessResponse):
            return None
        return DataPointListResponse.FromString(response.content)

    def count(self, selector: DataPointsSelector) -> int | None:
        if isinstance(selector, DataPointsDataSetSelector):
            return self.client.timeseries.count(
                filter={
                    "dataSetIds": [{"externalId": selector.data_set_external_id}],
                    "isString": selector.data_type == "string",
                },
                # We only want time series that have externalID set.
                advanced_filter=Exists(TimeSeriesProperty.external_id).dump(),
            )
        if isinstance(selector, DataPointsInstanceSpaceSelector):
            result = self.client.instances.aggregate(
                self.TIMESERIES_VIEW,
                aggregates=[CountAggregate(property="externalId")],
                instance_type="node",
                filter=self._instance_space_filter(selector),
            )
            if not result.items or not result.items[0].aggregates:
                return 0
            return int(result.items[0].aggregates[0].value or 0)
        return None

    def data_to_json_chunk(
        self, data_chunk: Page[DataPointListResponse], selector: DataPointsSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        raise ToolkitNotImplementedError(
            f"Download of {type(DatapointsIO).__name__.removesuffix('IO')} does not support json format."
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
                            item={**timeseries_id, "timestamp": dp.timestamp, "value": dp.value},
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
                            },
                        )
                    )
        return data_chunk.create_from(result)

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
            response = http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=http_client.config.create_api_url(self.UPLOAD_ENDPOINT),
                    method="POST",
                    content_type="application/protobuf",
                    data_content=item.item.datapoints.SerializeToString(),
                )
            )
            results.append(response.as_item_response(item.tracking_id))
        return results

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

        timestamps = list(
            self._convert_values(
                rows[selector.timestamp_column],
                self._epoc_converter,
                f"timestamps (columns {selector.timestamp_column!r})",
                source_id,
            )
        )

        datapoints_items: list[DataPointInsertionItem] = []
        for col, values in rows.items():
            if col == selector.timestamp_column:
                continue
            column = selector.id_by_column.get(col)
            if column is None:
                self._warn_missing_columns(col)
                continue
            args: dict[str, Any] = column.as_wrapped_id()
            if column.dtype == "numeric":
                number_values = self._convert_values(
                    values, self._numeric_converter, f"numeric datapoints (column {col!r})", source_id
                )
                args["numericDatapoints"] = NumericDatapoints(
                    datapoints=[
                        NumericDatapoint(timestamp=timestamp, value=value)
                        for timestamp, value in zip(timestamps, number_values)
                        if timestamp is not None
                    ]
                )
            elif column.dtype == "string":
                string_values = self._convert_values(
                    values, self._string_converter, f"string datapoints (column {col!r})", source_id
                )
                args["stringDatapoints"] = StringDatapoints(
                    datapoints=[
                        StringDatapoint(timestamp=timestamp, value=value)
                        for timestamp, value in zip(timestamps, string_values)
                        if timestamp is not None
                    ]
                )
            else:
                raise RuntimeError(f"Unsupported dtype {column.dtype} for column {col}.")

            datapoints_items.append(DataPointInsertionItem(**args))

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
        if selector.data_type == "state":
            return self._rows_to_state_datapoint_items(rows, selector, source_id)
        if missing := selector.required_columns - set(rows):
            raise RuntimeError(f"Columns {humanize_collection(missing)} not found.")
        return self._rows_to_value_datapoint_items(
            list(zip(rows["space"], rows["externalId"])),
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
        timestamps = list(
            self._convert_values(
                rows["timestamp"],
                self._epoc_converter,
                "timestamps (column 'timestamp')",
                source_id,
            )
        )
        values = list(
            self._convert_values(
                rows["value"],
                self._numeric_converter if data_type == "numeric" else self._string_converter,
                "values (column 'value')",
                source_id,
            )
        )
        datapoints_by_key: dict[tuple[str, ...], list[tuple[Any, Any]]] = defaultdict(list)
        for key, timestamp, value in zip(keys, timestamps, values):
            datapoints_by_key[key].append((timestamp, value))

        if data_type == "numeric":
            return [
                DataPointInsertionItem(
                    **to_id(key),
                    numericDatapoints=NumericDatapoints(
                        datapoints=[NumericDatapoint(timestamp=timestamp, value=value) for timestamp, value in dps]
                    ),
                )
                for key, dps in datapoints_by_key.items()
            ]
        return [
            DataPointInsertionItem(
                **to_id(key),
                stringDatapoints=StringDatapoints(
                    datapoints=[StringDatapoint(timestamp=timestamp, value=value) for timestamp, value in dps]
                ),
            )
            for key, dps in datapoints_by_key.items()
        ]

    def _rows_to_state_datapoint_items(
        self, rows: dict[str, list[Any]], selector: DataPointsInstanceSpaceSelector, source_id: str
    ) -> list[DataPointInsertionItem]:
        if missing := selector.required_columns - set(rows):
            raise RuntimeError(f"Columns {humanize_collection(missing)} not found.")

        datapoints_by_instance_id: dict[tuple[str, str], list[StateDatapoint]] = defaultdict(list)
        failed_count = 0
        for space, external_id, timestamp, numeric_value, string_value in zip(
            rows["space"], rows["externalId"], rows["timestamp"], rows["numericValue"], rows["stringValue"]
        ):
            try:
                converted_timestamp = cast(int | None, self._epoc_converter.convert(timestamp))
                converted_numeric = cast(int | None, self._state_converter.convert(numeric_value))
                converted_string = cast(str | None, self._string_converter.convert(string_value))
            except ValueError:
                failed_count += 1
                continue
            if converted_timestamp is None:
                failed_count += 1
                continue
            datapoints_by_instance_id[(space, external_id)].append(
                StateDatapoint(
                    timestamp=converted_timestamp, numericValue=converted_numeric, stringValue=converted_string
                )
            )
        if failed_count > 0:
            HighSeverityWarning(
                f"In {source_id}, {failed_count:,} state datapoints could not be converted and will be skipped."
            ).print_warning(console=self.client.console)

        return [
            DataPointInsertionItem(
                instanceId=InstanceId(space=space, externalId=external_id),
                stateDatapoints=StateDatapoints(datapoints=datapoints),
            )
            for (space, external_id), datapoints in datapoints_by_instance_id.items()
        ]

    def _convert_values(
        self, values: list[Any], converter: _ValueConverter, name: str, source_id: str
    ) -> Iterable[Any]:
        failed_count = 0
        for value in values:
            try:
                converted = converter.convert(value)
            except ValueError:
                failed_count += 1
                continue
            yield converted
        if failed_count > 0:
            HighSeverityWarning(
                f"In {source_id}' {failed_count:,} {name} could not be converted and will be skipped."
            ).print_warning(console=self.client.console)

    def _warn_missing_columns(self, column: str) -> None:
        if column not in self._warned_columns:
            HighSeverityWarning(
                f"Column '{column}' not found in selector columns. Skipping this column."
            ).print_warning(console=self.client.console)
            self._warned_columns.add(column)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> DatapointsRequestAdapter:
        raise ToolkitNotImplementedError(
            f"Upload of {type(DatapointsIO).__name__.removesuffix('IO')} does not support json format."
        )

    @classmethod
    def read_chunks(cls, reader: MultiFileReader, selector: DataPointsSelector) -> Iterable[Page[dict[str, JsonVal]]]:
        if not reader.is_table:
            raise RuntimeError(f"{cls.__name__} can only read from TableReader instances.")

        iterator = iter(reader.read_chunks_with_line_numbers())
        try:
            start_row, first = next(iterator)
        except StopIteration:
            # Empty file
            return
        column_names = list(first.keys())
        if isinstance(selector, DataPointsDataSetSelector | DataPointsInstanceSpaceSelector):
            if set(column_names) != selector.required_columns:
                raise RuntimeError(
                    f"When uploading datapoints using a {selector.type} manifest, you must have exactly the "
                    f"columns: {humanize_collection(selector.required_columns)} in the file. Got {humanize_collection(column_names)}. "
                )
        batch: dict[str, list[Any]] = {col: [value] for col, value in first.items()}
        last_row = start_row
        for row_no, chunk in iterator:
            for col, value in chunk.items():
                batch[col].append(value)

            # The number of datapoints is the number of rows times the number of value columns.
            if ((len(column_names) - 1) * len(batch[column_names[0]])) >= cls.CHUNK_SIZE:
                # We cannot guarantee JsonVal here, but that is handled later in the processing pipeline.
                yield Page(
                    worker_id="main",
                    items=[DataItem(tracking_id=f"rows {start_row} to {row_no}", item=batch)],  # type: ignore[arg-type]
                )
                start_row = row_no + 1
                batch = {col: [] for col in column_names}
            last_row = row_no
        if any(batch.values()):
            yield Page(
                worker_id="main",
                items=[DataItem(tracking_id=f"rows {start_row} to {last_row}", item=batch)],  # type: ignore[arg-type]
            )

    @classmethod
    def count_items(cls, reader: MultiFileReader, selector: DataPointsSelector | None = None) -> int:
        if selector is None:
            raise ValueError("A selector is required to correctly count datapoint items.")
        count = reader.count()
        if isinstance(selector, DataPointsFileSelector):
            return count * len(selector.columns)
        return count
