from typing import Any, Literal, Protocol

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.cdf_client import ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.classic_aggregate import (
    ClassicAggregateCountItem,
    ClassicAggregateUniqueBucket,
)

_AggregateName = Literal[
    "count",
    "cardinalityValues",
    "cardinalityProperties",
    "uniqueValues",
    "uniqueProperties",
]
_METADATA_PATH: tuple[str, ...] = ("metadata",)


class _AggregatePoster(Protocol):
    def _post_aggregate(self, body: dict[str, Any]) -> SuccessResponse: ...


def aggregate_count(
    api: _AggregatePoster,
    *,
    filter: ClassicFilter | dict[str, Any] | None = None,
    advanced_filter: dict[str, JsonValue] | None = None,
    property: tuple[str, ...] | None = None,
) -> int:
    body = _aggregate_body(
        "count",
        filter=filter,
        advanced_filter=advanced_filter,
        property=property,
    )
    return _first_count(api._post_aggregate(body))


def aggregate_cardinality(
    api: _AggregatePoster,
    property: tuple[str, ...],
    *,
    filter: ClassicFilter | dict[str, Any] | None = None,
    advanced_filter: dict[str, JsonValue] | None = None,
    aggregate_filter: dict[str, JsonValue] | None = None,
) -> int:
    aggregate: Literal["cardinalityProperties", "cardinalityValues"] = (
        "cardinalityProperties" if property == _METADATA_PATH else "cardinalityValues"
    )
    body = _aggregate_body(
        aggregate,
        filter=filter,
        advanced_filter=advanced_filter,
        aggregate_filter=aggregate_filter,
        property=property,
    )
    return _first_count(api._post_aggregate(body))


def aggregate_unique(
    api: _AggregatePoster,
    property: tuple[str, ...],
    *,
    filter: ClassicFilter | dict[str, Any] | None = None,
    advanced_filter: dict[str, JsonValue] | None = None,
    aggregate_filter: dict[str, JsonValue] | None = None,
) -> list[ClassicAggregateUniqueBucket]:
    aggregate: Literal["uniqueProperties", "uniqueValues"] = (
        "uniqueProperties" if property == _METADATA_PATH else "uniqueValues"
    )
    body = _aggregate_body(
        aggregate,
        filter=filter,
        advanced_filter=advanced_filter,
        aggregate_filter=aggregate_filter,
        property=property,
    )
    response = api._post_aggregate(body)
    return ResponseItems[ClassicAggregateUniqueBucket].model_validate_json(response.body).items


def files_aggregate_count(
    api: _AggregatePoster,
    *,
    filter: ClassicFilter | dict[str, Any] | None = None,
) -> int:
    body: dict[str, Any] = {}
    dumped_filter = _dump_filter(filter)
    if dumped_filter is not None:
        body["filter"] = dumped_filter
    return _first_count(api._post_aggregate(body))


def _aggregate_body(
    aggregate: _AggregateName,
    *,
    filter: ClassicFilter | dict[str, Any] | None,
    advanced_filter: dict[str, JsonValue] | None,
    aggregate_filter: dict[str, JsonValue] | None = None,
    property: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"aggregate": aggregate}
    dumped_filter = _dump_filter(filter)
    if dumped_filter is not None:
        body["filter"] = dumped_filter
    if advanced_filter:
        body["advancedFilter"] = advanced_filter
    if aggregate_filter:
        body["aggregateFilter"] = aggregate_filter
    if property is not None:
        if aggregate in {"cardinalityProperties", "uniqueProperties"}:
            body["path"] = list(property)
        else:
            body["properties"] = [{"property": list(property)}]
    return body


def _dump_filter(filter: ClassicFilter | dict[str, Any] | None) -> dict[str, Any] | None:
    if filter is None:
        return None
    dumped = filter.dump() if isinstance(filter, ClassicFilter) else filter
    return dumped or None


def _first_count(response: SuccessResponse) -> int:
    items = ResponseItems[ClassicAggregateCountItem].model_validate_json(response.body).items
    return items[0].count if items else 0
