from typing import Any, ClassVar, Literal, TypeAlias

from pydantic import Field, JsonValue, field_serializer, model_serializer, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import ViewId

from ._query import QueryTargetUnit


class _MetricAggregate(BaseModelObject):
    """An aggregate computed over one property.

    Accepts both the Python form ``CountAggregate(property="externalId")`` and the API form
    ``{"count": {"property": "externalId"}}``.
    """

    aggregate_name: ClassVar[str] = ""
    property: str

    @model_validator(mode="before")
    @classmethod
    def unwrap_api_form(cls, data: Any) -> Any:
        name = cls.aggregate_name
        if isinstance(data, dict) and "property" not in data and isinstance(data.get(name), dict):
            return data[name]
        return data

    def aggregate_body(self) -> dict[str, Any]:
        return {"property": self.property}

    @model_serializer(mode="plain", when_used="always")
    def serialize_aggregate(self) -> dict[str, Any]:
        return {self.aggregate_name: self.aggregate_body()}


class AvgAggregate(_MetricAggregate):
    """Average of a numeric property."""

    aggregate_name: ClassVar[str] = "avg"


class CountAggregate(_MetricAggregate):
    """Count of values for a property."""

    aggregate_name: ClassVar[str] = "count"


class MinAggregate(_MetricAggregate):
    """Minimum value of a property."""

    aggregate_name: ClassVar[str] = "min"


class MaxAggregate(_MetricAggregate):
    """Maximum value of a property."""

    aggregate_name: ClassVar[str] = "max"


class SumAggregate(_MetricAggregate):
    """Sum of a numeric property."""

    aggregate_name: ClassVar[str] = "sum"


class HistogramAggregate(_MetricAggregate):
    """Histogram of a numeric property.

    ``interval`` is the width of each bucket.
    """

    aggregate_name: ClassVar[str] = "histogram"
    interval: float

    def aggregate_body(self) -> dict[str, Any]:
        return {"property": self.property, "interval": self.interval}


InstanceAggregateDefinition: TypeAlias = (
    AvgAggregate | CountAggregate | MinAggregate | MaxAggregate | SumAggregate | HistogramAggregate
)


class InstanceAggregateRequest(BaseModelObject):
    """Request body for ``POST /models/instances/aggregate``.

    See `API docs <https://api-docs.cognite.com/20230101/tag/Instances/operation/aggregateInstances>`_.
    """

    view: ViewId
    query: str | None = None
    properties: list[str] | None = Field(default=None, min_length=1, max_length=200)
    limit: int | None = Field(default=None, ge=1, le=1000)
    aggregates: list[InstanceAggregateDefinition] | None = Field(default=None, max_length=5)
    group_by: list[str] | None = Field(default=None, min_length=1, max_length=5)
    filter: dict[str, JsonValue] | None = None
    operator: Literal["AND", "OR"] | None = None
    instance_type: Literal["node", "edge"] | None = None
    target_units: list[QueryTargetUnit] | None = Field(default=None, min_length=1, max_length=10)
    include_typing: bool | None = None

    @field_serializer("view")
    def serialize_view(self, view: ViewId) -> dict[str, Any]:
        return view.dump(include_type=True)


class HistogramBucket(BaseModelObject):
    """One bucket in a histogram aggregate."""

    start: float
    count: int


class InstanceAggregateValue(BaseModelObject):
    """One computed aggregate in an aggregation result."""

    aggregate: Literal["avg", "count", "min", "max", "sum", "histogram"]
    property: str
    value: float | None = None
    interval: float | None = None
    buckets: list[HistogramBucket] | None = None


class InstanceAggregateResult(BaseModelObject):
    """One group of aggregate values."""

    instance_type: Literal["node", "edge"]
    aggregates: list[InstanceAggregateValue]
    group: dict[str, JsonValue] | None = None


class InstanceAggregateResponse(BaseModelObject):
    """Response from ``POST /models/instances/aggregate``."""

    items: list[InstanceAggregateResult]
    typing: dict[str, JsonValue] | None = None
