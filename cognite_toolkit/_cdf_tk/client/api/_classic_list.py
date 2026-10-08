"""Request bodies for classic asset, event, time series, sequence, and file list calls."""

from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    ClassicFilter,
    EpochTimestampRange,
    GeoLocationFilter,
    LabelFilter,
)

AggregatedAssetProperty = Literal["childCount", "path", "depth", "child_count"]
ObjectId = int | str | InternalId | ExternalId
ObjectIds = ObjectId | Sequence[ObjectId]
TimeRange = EpochTimestampRange | dict[str, Any]
Sort = dict[str, Any] | Sequence[dict[str, Any]]
_AGGREGATED_PROPERTIES = {"childCount": "childCount", "child_count": "childCount", "path": "path", "depth": "depth"}


def classic_list_body(
    *,
    filter: ClassicFilter | dict[str, Any] | None,
    fields: dict[str, Any],
    advanced_filter: dict[str, JsonValue] | None = None,
    sort: Sort | None = None,
    partition: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {}
    if extra:
        body.update(extra)
    dumped_filter = _merge_filter(filter, fields)
    if dumped_filter:
        body["filter"] = dumped_filter
    if advanced_filter:
        body["advancedFilter"] = advanced_filter
    dumped_sort = _sort(sort)
    if dumped_sort is not None:
        body["sort"] = dumped_sort
    if partition is not None:
        body["partition"] = partition
    return body


def _merge_filter(filter: ClassicFilter | dict[str, Any] | None, fields: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(filter, ClassicFilter):
        body = filter.dump()
    elif isinstance(filter, Mapping):
        body = dict(filter)
    else:
        body = {}
    for key, value in fields.items():
        if value is not None:
            body[key] = value
    return body or None


def _aggregated_properties(value: bool | Sequence[AggregatedAssetProperty]) -> list[str] | None:
    if isinstance(value, bool):
        if not value:
            return None
        return ["childCount", "path", "depth"]
    properties = [value] if isinstance(value, str) else list(value)
    normalized: list[str] = []
    for item in properties:
        canonical = _AGGREGATED_PROPERTIES.get(item)
        if canonical is None:
            allowed = "childCount, path, depth"
            raise ValueError(f"Unknown aggregated property {item!r}. Expected one of {allowed}.")
        normalized.append(canonical)
    return normalized


def _sort(value: Sort | None) -> list[dict[str, Any]] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return [dict(value)]
    return [dict(item) for item in value]


def _time_range(value: TimeRange | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, EpochTimestampRange):
        return value.dump() or None
    return dict(value)


def _dump_model(value: LabelFilter | GeoLocationFilter | dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return dict(value)
    return value.dump() or None


def _int_ids(value: int | Sequence[int] | None) -> list[int] | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("Expected an int or sequence of ints, got bool.")
    if isinstance(value, int):
        return [value]
    if isinstance(value, (str, Mapping)):
        raise TypeError(f"Expected an int or sequence of ints, got {type(value).__name__}.")
    return list(value)


def _str_ids(value: str | Sequence[str] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    return list(value)


def _object_ids(
    ids: ObjectIds | None = None,
    external_ids: str | Sequence[str] | None = None,
) -> list[dict[str, Any]] | None:
    if ids is None and external_ids is None:
        return None
    dumped: list[dict[str, Any]] = []
    if ids is not None:
        dumped.extend(_dump_object_id(item) for item in _as_object_ids(ids))
    if external_ids is not None:
        dumped.extend({"externalId": external_id} for external_id in _as_strings(external_ids))
    return dumped


def _as_object_ids(value: ObjectIds) -> list[ObjectId]:
    if isinstance(value, bool):
        raise TypeError("Expected an id, got bool.")
    if isinstance(value, (int, str, InternalId, ExternalId)):
        return [value]
    if isinstance(value, Mapping):
        raise TypeError(f"Expected an id or sequence of ids, got {type(value).__name__}.")
    return list(value)


def _as_strings(value: str | Sequence[str]) -> list[str]:
    if isinstance(value, str):
        return [value]
    return list(value)


def _dump_object_id(value: ObjectId) -> dict[str, Any]:
    if isinstance(value, bool) or not isinstance(value, (int, str, InternalId, ExternalId)):
        raise TypeError(f"Expected an id, got {type(value).__name__}.")
    if isinstance(value, int):
        return {"id": value}
    if isinstance(value, str):
        return {"externalId": value}
    return value.dump()
