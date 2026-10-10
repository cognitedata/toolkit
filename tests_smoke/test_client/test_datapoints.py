from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import TypeVar

import pytest
from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.datapoints import (
    AggregateDatapointsResponse,
    Datapoint,
    DatapointsDeleteRequest,
    DatapointsQueryRequest,
    DatapointsRequest,
    LatestDatapointRequest,
    NumericDatapointsResponse,
    StringDatapointsResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
from tests_smoke.constants import TIMESERIES_NUMERIC_EXTERNAL_ID, TIMESERIES_STRING_EXTERNAL_ID
from tests_smoke.exceptions import EndpointAssertionError

_START = datetime(2020, 1, 1, tzinfo=timezone.utc)
_END = _START + timedelta(hours=3)
_NUMERIC_POINTS = (
    (_START, 1.0),
    (_START + timedelta(hours=1), 2.0),
    (_START + timedelta(hours=2), 3.0),
)
_STRING_POINTS = (
    (_START, "low"),
    (_START + timedelta(hours=1), "mid"),
    (_START + timedelta(hours=2), "high"),
)
_T = TypeVar("_T")


def _persistent_timeseries(client: ToolkitClient, request: TimeSeriesRequest) -> TimeSeriesResponse:
    endpoint = client.tool.timeseries._method_endpoint_map["create"].path
    retrieved = client.tool.timeseries.retrieve([request.as_id()], ignore_unknown_ids=True)
    series = retrieved[0] if retrieved else client.tool.timeseries.create([request])[0]
    if series.is_string != request.is_string:
        raise EndpointAssertionError(
            endpoint,
            f"Persistent time series {request.external_id} is_string={series.is_string}, expected {request.is_string}.",
        )
    if series.external_id is None:
        raise EndpointAssertionError(endpoint, "Persistent time series is missing an external ID.")
    return series


def _call(endpoint: str, action: Callable[[], _T]) -> _T:
    try:
        return action()
    except (ToolkitAPIError, ValidationError) as error:
        raise EndpointAssertionError(endpoint, str(error)) from error


def _require(condition: bool, endpoint: str, message: str) -> None:
    if not condition:
        raise EndpointAssertionError(endpoint, message)


@pytest.fixture(scope="session")
def smoke_numeric_timeseries(toolkit_client: ToolkitClient) -> TimeSeriesResponse:
    return _persistent_timeseries(
        toolkit_client,
        TimeSeriesRequest(
            external_id=TIMESERIES_NUMERIC_EXTERNAL_ID,
            name="Smoke Test Timeseries",
            is_string=False,
            is_step=True,
        ),
    )


@pytest.fixture(scope="session")
def smoke_string_timeseries(toolkit_client: ToolkitClient) -> TimeSeriesResponse:
    return _persistent_timeseries(
        toolkit_client,
        TimeSeriesRequest(
            external_id=TIMESERIES_STRING_EXTERNAL_ID,
            name="Smoke Test String Timeseries",
            is_string=True,
        ),
    )


def test_datapoints_insert_retrieve_latest_and_delete(
    toolkit_client: ToolkitClient,
    smoke_numeric_timeseries: TimeSeriesResponse,
    smoke_string_timeseries: TimeSeriesResponse,
) -> None:
    api = toolkit_client.tool.timeseries.datapoints
    if smoke_numeric_timeseries.external_id is None or smoke_string_timeseries.external_id is None:
        raise EndpointAssertionError("/timeseries", "Persistent time series is missing an external ID.")
    numeric_external_id = smoke_numeric_timeseries.external_id
    string_external_id = smoke_string_timeseries.external_id

    create_path = api._method_endpoint_map["create"].path
    retrieve_path = api._method_endpoint_map["retrieve"].path
    latest_path = api._latest_endpoint.path
    delete_path = api._method_endpoint_map["delete"].path
    delete_items = [
        DatapointsDeleteRequest(external_id=numeric_external_id, inclusive_begin=_START, exclusive_end=_END),
        DatapointsDeleteRequest(external_id=string_external_id, inclusive_begin=_START, exclusive_end=_END),
    ]

    _call(delete_path, lambda: api.delete(delete_items))
    try:
        _call(
            create_path,
            lambda: api.create(
                [
                    DatapointsRequest(
                        external_id=numeric_external_id,
                        datapoints=[
                            Datapoint(timestamp=timestamp, value=value) for timestamp, value in _NUMERIC_POINTS
                        ],
                    ),
                    DatapointsRequest(
                        external_id=string_external_id,
                        datapoints=[Datapoint(timestamp=timestamp, value=value) for timestamp, value in _STRING_POINTS],
                    ),
                ]
            ),
        )

        numeric = _call(
            retrieve_path,
            lambda: api.retrieve(
                [DatapointsQueryRequest(external_id=numeric_external_id)],
                start=_START,
                end=_END,
            ),
        )
        _require(
            len(numeric) == 1 and isinstance(numeric[0], NumericDatapointsResponse),
            retrieve_path,
            f"Expected one numeric series, got {[type(item).__name__ for item in numeric]}.",
        )
        numeric_series = numeric[0]
        if not isinstance(numeric_series, NumericDatapointsResponse):
            raise EndpointAssertionError(retrieve_path, "Expected a numeric series.")
        _require(
            [(point.timestamp, point.value) for point in numeric_series.datapoints] == list(_NUMERIC_POINTS),
            retrieve_path,
            f"Numeric data points were {[(point.timestamp, point.value) for point in numeric_series.datapoints]}, "
            f"expected {list(_NUMERIC_POINTS)}.",
        )

        string = _call(
            retrieve_path,
            lambda: api.retrieve(
                [DatapointsQueryRequest(external_id=string_external_id)],
                start=_START,
                end=_END,
            ),
        )
        _require(
            len(string) == 1 and isinstance(string[0], StringDatapointsResponse),
            retrieve_path,
            f"Expected one string series, got {[type(item).__name__ for item in string]}.",
        )
        string_series = string[0]
        if not isinstance(string_series, StringDatapointsResponse):
            raise EndpointAssertionError(retrieve_path, "Expected a string series.")
        _require(
            [(point.timestamp, point.value) for point in string_series.datapoints] == list(_STRING_POINTS),
            retrieve_path,
            f"String data points were {[(point.timestamp, point.value) for point in string_series.datapoints]}, "
            f"expected {list(_STRING_POINTS)}.",
        )

        aggregated = _call(
            retrieve_path,
            lambda: api.retrieve(
                [DatapointsQueryRequest(external_id=numeric_external_id)],
                start=_START,
                end=_END,
                aggregates=["average", "count"],
                granularity="1h",
            ),
        )
        _require(
            len(aggregated) == 1 and isinstance(aggregated[0], AggregateDatapointsResponse),
            retrieve_path,
            f"Expected one aggregate series, got {[type(item).__name__ for item in aggregated]}.",
        )
        aggregate_series = aggregated[0]
        if not isinstance(aggregate_series, AggregateDatapointsResponse):
            raise EndpointAssertionError(retrieve_path, "Expected an aggregate series.")
        aggregate_points = [(point.timestamp, point.average, point.count) for point in aggregate_series.datapoints]
        expected_aggregates = [(timestamp, value, 1) for timestamp, value in _NUMERIC_POINTS]
        _require(
            aggregate_points == expected_aggregates,
            retrieve_path,
            f"Aggregates were {aggregate_points}, expected {expected_aggregates}.",
        )

        latest = _call(
            latest_path,
            lambda: api.latest(
                [
                    LatestDatapointRequest(external_id=numeric_external_id, before=_END),
                    LatestDatapointRequest(external_id=string_external_id, before=_END),
                ]
            ),
        )
        _require(len(latest) == 2, latest_path, f"Expected two latest series, got {len(latest)}.")
        latest_numeric, latest_string = latest
        _require(
            isinstance(latest_numeric, NumericDatapointsResponse)
            and [(point.timestamp, point.value) for point in latest_numeric.datapoints] == [_NUMERIC_POINTS[-1]],
            latest_path,
            "Latest numeric data point did not match the last inserted point.",
        )
        _require(
            isinstance(latest_string, StringDatapointsResponse)
            and [(point.timestamp, point.value) for point in latest_string.datapoints] == [_STRING_POINTS[-1]],
            latest_path,
            "Latest string data point did not match the last inserted point.",
        )

        _call(delete_path, lambda: api.delete(delete_items))
        remaining = _call(
            retrieve_path,
            lambda: api.retrieve(
                [
                    DatapointsQueryRequest(external_id=numeric_external_id),
                    DatapointsQueryRequest(external_id=string_external_id),
                ],
                start=_START,
                end=_END,
            ),
        )
        remaining_counts = [len(series.datapoints) for series in remaining]
        _require(
            remaining_counts == [0, 0],
            delete_path,
            f"Delete left data points in the range. Remaining counts were {remaining_counts}.",
        )
    finally:
        _call(delete_path, lambda: api.delete(delete_items))
