import builtins
from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.datapoints import (
    DatapointAggregate,
    DatapointsDeleteRequest,
    DatapointsQueryDefaults,
    DatapointsQueryRequest,
    DatapointsRequest,
    DatapointsResponse,
    LatestDatapointRequest,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

# A single insert request accepts at most this many data points across all time series.
_INSERT_DATAPOINT_LIMIT = 100_000


class DatapointsAPI(CDFResourceAPI[DatapointsResponse]):
    """Insert, retrieve, and delete time series data points.

    Requests and responses use ``application/json``.

    Limits enforced by CDF:
    - Insert: at most 10,000 time series and 100,000 data points per request.
    - Retrieve and latest: at most 100 time series per request.
    - Retrieve returns at most 100,000 raw data points, or 10,000 aggregated data points, per request.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(
                    method="POST", path="/timeseries/data", item_limit=10_000, concurrency_max_workers=1
                ),
                "retrieve": Endpoint(
                    method="POST", path="/timeseries/data/list", item_limit=100, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/timeseries/data/delete", item_limit=10_000, concurrency_max_workers=1
                ),
            },
        )
        # Latest uses the retrieve method with this path. Both endpoints accept 100 items per request.
        self._latest_endpoint = Endpoint(method="POST", path="/timeseries/data/latest", item_limit=100)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DatapointsResponse]:
        return PagedResponse[DatapointsResponse].model_validate_json(response.body)

    def create(self, items: Sequence[DatapointsRequest]) -> None:
        """Insert data points into one or more time series.

        A point with a timestamp that already exists is overwritten. Requests that exceed the
        per-request limits are split automatically.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/postMultiTimeSeriesDatapoints>`_.

        Args:
            items: Data points to insert, each item targeting one time series.
        """
        endpoint = self._method_endpoint_map["create"]
        for chunk in _chunk_datapoint_inserts(
            items,
            item_limit=endpoint.item_limit,
            datapoint_limit=_INSERT_DATAPOINT_LIMIT,
        ):
            self._request_no_response(chunk, "create")

    def retrieve(
        self,
        items: Sequence[DatapointsQueryRequest],
        *,
        start: int | str | None = None,
        end: int | str | None = None,
        limit: int | None = None,
        aggregates: list[DatapointAggregate] | None = None,
        granularity: str | None = None,
        include_outside_points: bool | None = None,
        time_zone: str | None = None,
        ignore_unknown_ids: bool = False,
    ) -> builtins.list[DatapointsResponse]:
        """Retrieve data points from multiple time series.

        Fields set on an item override the corresponding argument. When aggregates are omitted, raw
        data points are returned. Each returned series includes ``next_cursor`` when more points are
        available; pass that value as ``cursor`` on the next query for that series.

        ``start`` defaults to epoch 0 on the service when omitted, which excludes points before 1970.
        Pass a negative timestamp to include those points.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_.

        Args:
            items: Per-series queries. At most 100 are sent in one request.
            start: Inclusive start time as epoch milliseconds or a timestamp string such as ``1d-ago``.
            end: Exclusive end time as epoch milliseconds or a timestamp string.
            limit: Maximum number of data points per series. The service default is 100.
            aggregates: Aggregates to return instead of raw data points.
            granularity: Aggregation window, for example ``5m`` or ``1h``. Required when aggregates are set.
            include_outside_points: Include the point just before and just after the requested interval.
            time_zone: Time zone used to align aggregates of one hour or longer. The service default is UTC.
            ignore_unknown_ids: Skip time series that do not exist.

        Returns:
            Data points for each found time series, in request order.
        """
        _require_granularity(items, aggregates, granularity)
        extra_body = _query_defaults_body(
            start=start,
            end=end,
            limit=limit,
            aggregates=aggregates,
            granularity=granularity,
            include_outside_points=include_outside_points,
            time_zone=time_zone,
            ignore_unknown_ids=ignore_unknown_ids,
        )
        return self._request_item_response(items, "retrieve", extra_body=extra_body)

    def latest(
        self, items: Sequence[LatestDatapointRequest], ignore_unknown_ids: bool = False
    ) -> builtins.list[DatapointsResponse]:
        """Retrieve the latest data point before a timestamp for one or more time series.

        The latest point is the one with the highest timestamp, which is not necessarily the most
        recently ingested point. A series with no points in range is returned with an empty list.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getLatest>`_.

        Args:
            items: Per-series queries. At most 100 are sent in one request.
            ignore_unknown_ids: Skip time series that do not exist.

        Returns:
            The latest data point for each found time series.
        """
        responses: list[DatapointsResponse] = []
        for chunk in chunker_sequence(list(items), self._latest_endpoint.item_limit):
            responses.extend(
                self._request_item_response(
                    chunk,
                    method="retrieve",
                    extra_body={"ignoreUnknownIds": ignore_unknown_ids},
                    endpoint=self._latest_endpoint.path,
                )
            )
        return responses

    def delete(self, items: Sequence[DatapointsDeleteRequest]) -> None:
        """Delete data points in a time range from one or more time series.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/deleteDatapoints>`_.

        Args:
            items: Delete ranges, each item targeting one time series.
        """
        self._request_no_response(items, "delete")


def _chunk_datapoint_inserts(
    items: Sequence[DatapointsRequest],
    *,
    item_limit: int,
    datapoint_limit: int,
) -> Iterable[list[DatapointsRequest]]:
    """Split insert items so each request stays within the series and data point limits."""
    chunk: list[DatapointsRequest] = []
    point_count = 0
    for item in items:
        offset = 0
        datapoints = item.datapoints
        while offset < len(datapoints):
            if chunk and (len(chunk) >= item_limit or point_count >= datapoint_limit):
                yield chunk
                chunk = []
                point_count = 0
            room = min(datapoint_limit - point_count, len(datapoints) - offset)
            piece = datapoints[offset : offset + room]
            if offset == 0 and room == len(datapoints):
                chunk.append(item)
            else:
                chunk.append(item.model_copy(update={"datapoints": piece}))
            point_count += room
            offset += room
    if chunk:
        yield chunk


def _require_granularity(
    items: Sequence[DatapointsQueryRequest],
    aggregates: list[DatapointAggregate] | None,
    granularity: str | None,
) -> None:
    if aggregates and granularity is None:
        raise ValueError("granularity is required when aggregates are set.")
    missing_item_granularity = any(item.aggregates and not item.granularity and granularity is None for item in items)
    if missing_item_granularity:
        raise ValueError("granularity is required when aggregates are set.")


def _query_defaults_body(
    *,
    start: int | str | None,
    end: int | str | None,
    limit: int | None,
    aggregates: list[DatapointAggregate] | None,
    granularity: str | None,
    include_outside_points: bool | None,
    time_zone: str | None,
    ignore_unknown_ids: bool,
) -> dict[str, Any]:
    values: dict[str, Any] = {"ignore_unknown_ids": ignore_unknown_ids}
    if start is not None:
        values["start"] = start
    if end is not None:
        values["end"] = end
    if limit is not None:
        values["limit"] = limit
    if aggregates is not None:
        values["aggregates"] = aggregates
    if granularity is not None:
        values["granularity"] = granularity
    if include_outside_points is not None:
        values["include_outside_points"] = include_outside_points
    if time_zone is not None:
        values["time_zone"] = time_zone
    return DatapointsQueryDefaults(**values).dump()
