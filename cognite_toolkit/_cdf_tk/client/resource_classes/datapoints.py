import builtins
import sys
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BeforeValidator, Field, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    Identifier,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client._types import Timestamp
from cognite_toolkit._cdf_tk.client.identifiers import NodeUntypedId

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


DatapointAggregate = Literal[
    "average",
    "max",
    "maxDatapoint",
    "min",
    "minDatapoint",
    "count",
    "sum",
    "interpolation",
    "stepInterpolation",
    "totalVariation",
    "continuousVariance",
    "discreteVariance",
    "countGood",
    "countUncertain",
    "countBad",
    "durationGood",
    "durationUncertain",
    "durationBad",
]


class DatapointStatus(BaseModelObject):
    """Status code attached to a data point."""

    code: int | None = None
    symbol: str | None = None


class Datapoint(BaseModelObject):
    """A data point to insert."""

    timestamp: Timestamp
    value: int | float | str
    status: DatapointStatus | None = None


class NumericDatapointResponse(BaseModelObject):
    """A raw numeric data point.

    ``value`` is omitted when the point has a bad status. A bad point may instead use one of
    ``NaN``, ``Infinity``, or ``-Infinity``.
    """

    timestamp: Timestamp
    value: int | float | Literal["NaN", "Infinity", "-Infinity"] | None = None
    status: DatapointStatus | None = None


class StringDatapointResponse(BaseModelObject):
    """A raw string data point.

    ``value`` is omitted when the point has a bad status.
    """

    timestamp: Timestamp
    value: str | None = None
    status: DatapointStatus | None = None


class AggregateExtremeDatapoint(BaseModelObject):
    """Timestamp and value of the first minimum or maximum point in an aggregate window."""

    timestamp: Timestamp
    value: int | float
    status: DatapointStatus | None = None


class AggregateDatapointResponse(BaseModelObject):
    """One aggregated window. Only the requested aggregate fields are set."""

    timestamp: Timestamp
    count: int | None = None
    count_good: int | None = None
    count_uncertain: int | None = None
    count_bad: int | None = None
    duration_good: int | None = None
    duration_uncertain: int | None = None
    duration_bad: int | None = None
    average: int | float | None = None
    max: int | float | None = None
    max_datapoint: AggregateExtremeDatapoint | None = None
    min: int | float | None = None
    min_datapoint: AggregateExtremeDatapoint | None = None
    sum: int | float | None = None
    interpolation: int | float | None = None
    step_interpolation: int | float | None = None
    continuous_variance: int | float | None = None
    discrete_variance: int | float | None = None
    total_variation: int | float | None = None


class TimeSeriesIdentifiers(BaseModelObject):
    """Identifiers returned for a time series. More than one may be present."""

    id: int | None = None
    external_id: str | None = None
    instance_id: NodeUntypedId | None = None


class TimeSeriesDatapointSelector(TimeSeriesIdentifiers):
    """A time series selected by exactly one of internal id, external id, or instance id."""

    @model_validator(mode="after")
    def _exactly_one_identifier(self) -> Self:
        selected = sum(value is not None for value in (self.id, self.external_id, self.instance_id))
        if selected != 1:
            raise ValueError("Exactly one of id, external_id, or instance_id must be set.")
        return self


class DatapointsId(Identifier):
    """Identity of a datapoints request, used in errors and logs."""

    id: int | None = None
    external_id: str | None = None
    instance_id: NodeUntypedId | None = None
    timestamps: tuple[Timestamp, ...] = ()
    start: Timestamp | None = None
    end: Timestamp | None = None
    before: Timestamp | None = None
    inclusive_begin: Timestamp | None = None
    exclusive_end: Timestamp | None = None

    def __str__(self) -> str:
        if self.external_id is not None:
            identity = f"externalId='{self.external_id}'"
        elif self.instance_id is not None:
            identity = f"instanceId='{self.instance_id}'"
        elif self.id is not None:
            identity = f"id={self.id}"
        else:
            identity = "undefined"
        details = _identity_details(self)
        if not details:
            return identity
        return f"{identity}, {', '.join(details)}"

    def _as_filename(self, include_type: bool = False) -> str:
        if self.external_id is not None:
            name = self.external_id
        elif self.id is not None:
            name = str(self.id)
        elif self.instance_id is not None:
            name = self.instance_id._as_filename(False)
        else:
            name = "undefined"
        if include_type:
            return f"datapoints-{name}"
        return name


def _identity_details(identifier: DatapointsId) -> list[str]:
    details: list[str] = []
    if identifier.timestamps:
        rendered = ", ".join(str(timestamp) for timestamp in identifier.timestamps)
        details.append(f"timestamps=[{rendered}]")
    if identifier.start is not None:
        details.append(f"start={identifier.start}")
    if identifier.end is not None:
        details.append(f"end={identifier.end}")
    if identifier.before is not None:
        details.append(f"before={identifier.before}")
    if identifier.inclusive_begin is not None:
        details.append(f"inclusiveBegin={identifier.inclusive_begin}")
    if identifier.exclusive_end is not None:
        details.append(f"exclusiveEnd={identifier.exclusive_end}")
    return details


def _selector_fields(item: TimeSeriesIdentifiers) -> dict[str, Any]:
    return {"id": item.id, "external_id": item.external_id, "instance_id": item.instance_id}


class DatapointsRequest(TimeSeriesDatapointSelector, RequestResource):
    """Data points to insert into one time series.

    See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/postMultiTimeSeriesDatapoints>`_.
    """

    datapoints: list[Datapoint] = Field(min_length=1)

    def as_id(self) -> DatapointsId:
        return DatapointsId(
            timestamps=tuple(point.timestamp for point in self.datapoints),
            **_selector_fields(self),
        )


class _UnitTarget(BaseModelObject):
    target_unit: str | None = None
    target_unit_system: str | None = None

    @model_validator(mode="after")
    def _one_unit_target(self) -> Self:
        if self.target_unit is not None and self.target_unit_system is not None:
            raise ValueError("Specify only one of target_unit and target_unit_system.")
        return self


class DatapointsQueryRequest(TimeSeriesDatapointSelector, _UnitTarget, RequestResource):
    """Query for data points from one time series.

    See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_.
    """

    start: Timestamp | None = None
    end: Timestamp | None = None
    limit: int | None = None
    aggregates: list[DatapointAggregate] | None = Field(default=None, min_length=1)
    granularity: str | None = None
    include_outside_points: bool | None = None
    include_status: bool | None = None
    ignore_bad_datapoints: bool | None = None
    treat_uncertain_as_bad: bool | None = None
    cursor: str | None = None
    time_zone: str | None = None

    def as_id(self) -> DatapointsId:
        return DatapointsId(start=self.start, end=self.end, **_selector_fields(self))


class DatapointsQueryDefaults(BaseModelObject):
    """Defaults applied to every query item in a retrieve request when the item omits the field."""

    start: Timestamp | None = None
    end: Timestamp | None = None
    limit: int | None = None
    aggregates: list[DatapointAggregate] | None = Field(default=None, min_length=1)
    granularity: str | None = None
    include_outside_points: bool | None = None
    time_zone: str | None = None
    ignore_unknown_ids: bool = False


class LatestDatapointRequest(TimeSeriesDatapointSelector, _UnitTarget, RequestResource):
    """Query for the latest data point of one time series.

    See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getLatest>`_.
    """

    before: Timestamp | None = None
    include_status: bool | None = None
    ignore_bad_datapoints: bool | None = None
    treat_uncertain_as_bad: bool | None = None

    def as_id(self) -> DatapointsId:
        return DatapointsId(before=self.before, **_selector_fields(self))


class DatapointsDeleteRequest(TimeSeriesDatapointSelector, RequestResource):
    """Data points to delete from one time series.

    ``inclusive_begin`` is required. It is the first timestamp to delete.
    ``exclusive_end`` is the first timestamp to keep. When it is omitted, only the data point at
    ``inclusive_begin`` is deleted. Both are serialized as epoch milliseconds.

    See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/deleteDatapoints>`_.
    """

    inclusive_begin: Timestamp
    exclusive_end: Timestamp | None = None

    def as_id(self) -> DatapointsId:
        return DatapointsId(
            inclusive_begin=self.inclusive_begin,
            exclusive_end=self.exclusive_end,
            **_selector_fields(self),
        )


class _DatapointsSeries(TimeSeriesIdentifiers, ResponseResource[DatapointsRequest]):
    """Data points returned for one time series.

    ``id``, ``is_string``, and ``type`` are always present. ``is_string`` is false for numeric series
    and for aggregates, which CDF only returns for numeric series.
    """

    id: int
    is_string: bool
    type: str
    unit: str | None = None
    unit_external_id: str | None = None
    next_cursor: str | None = None

    @classmethod
    def request_cls(cls) -> builtins.type[DatapointsRequest]:
        return DatapointsRequest


class NumericDatapointsResponse(_DatapointsSeries):
    """Raw data points from a numeric time series."""

    is_string: Literal[False]
    is_step: bool | None = None
    datapoints: list[NumericDatapointResponse]

    def as_request_resource(self) -> DatapointsRequest:
        return _as_insert_request(self, self.datapoints)


class StringDatapointsResponse(_DatapointsSeries):
    """Raw data points from a string time series."""

    is_string: Literal[True]
    datapoints: list[StringDatapointResponse]

    def as_request_resource(self) -> DatapointsRequest:
        return _as_insert_request(self, self.datapoints)


class AggregateDatapointsResponse(_DatapointsSeries):
    """Aggregated data points from a numeric time series."""

    is_string: Literal[False]
    is_step: bool
    datapoints: list[AggregateDatapointResponse]

    def as_request_resource(self) -> DatapointsRequest:
        raise ValueError("Aggregate datapoints cannot be converted to an insert request.")


_AGGREGATE_POINT_FIELDS = frozenset(
    {
        "count",
        "countGood",
        "countUncertain",
        "countBad",
        "durationGood",
        "durationUncertain",
        "durationBad",
        "average",
        "max",
        "maxDatapoint",
        "min",
        "minDatapoint",
        "sum",
        "interpolation",
        "stepInterpolation",
        "continuousVariance",
        "discreteVariance",
        "totalVariation",
    }
)


def parse_datapoints_series(
    item: dict[str, Any],
) -> NumericDatapointsResponse | StringDatapointsResponse | AggregateDatapointsResponse:
    """Parse one retrieve or latest item into the series type that matches its points."""
    points = item.get("datapoints") or []
    if any(isinstance(point, dict) and _AGGREGATE_POINT_FIELDS.intersection(point) for point in points):
        return AggregateDatapointsResponse.model_validate(item)
    if item.get("isString") is True:
        return StringDatapointsResponse.model_validate(item)
    return NumericDatapointsResponse.model_validate(item)


def _coerce_datapoints_series(
    value: Any,
) -> NumericDatapointsResponse | StringDatapointsResponse | AggregateDatapointsResponse:
    if isinstance(value, (NumericDatapointsResponse, StringDatapointsResponse, AggregateDatapointsResponse)):
        return value
    if isinstance(value, dict):
        return parse_datapoints_series(value)
    raise TypeError(f"Expected a datapoints series object, got {type(value).__name__}.")


DatapointsSeriesResponse: TypeAlias = Annotated[
    NumericDatapointsResponse | StringDatapointsResponse | AggregateDatapointsResponse,
    BeforeValidator(_coerce_datapoints_series),
]


def _as_insert_request(
    response: NumericDatapointsResponse | StringDatapointsResponse,
    points: list[NumericDatapointResponse] | list[StringDatapointResponse],
) -> DatapointsRequest:
    datapoints = _insert_datapoints(points)
    if response.external_id is not None:
        return DatapointsRequest(external_id=response.external_id, datapoints=datapoints)
    if response.instance_id is not None:
        return DatapointsRequest(instance_id=response.instance_id, datapoints=datapoints)
    return DatapointsRequest(id=response.id, datapoints=datapoints)


def _insert_datapoints(
    points: list[NumericDatapointResponse] | list[StringDatapointResponse],
) -> list[Datapoint]:
    converted: list[Datapoint] = []
    for point in points:
        value = point.value
        if value is None:
            raise ValueError("A data point without a value cannot be converted to an insert request.")
        if point.status is None:
            converted.append(Datapoint(timestamp=point.timestamp, value=value))
        else:
            converted.append(Datapoint(timestamp=point.timestamp, value=value, status=point.status))
    return converted
