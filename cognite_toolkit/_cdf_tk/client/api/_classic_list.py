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


def asset_list_body(
    *,
    filter: ClassicFilter | dict[str, Any] | None = None,
    name: str | None = None,
    parent_ids: int | Sequence[int] | None = None,
    parent_external_ids: str | Sequence[str] | None = None,
    root_ids: ObjectIds | None = None,
    asset_subtree_ids: ObjectIds | None = None,
    asset_subtree_external_ids: str | Sequence[str] | None = None,
    data_set_ids: ObjectIds | None = None,
    data_set_external_ids: str | Sequence[str] | None = None,
    metadata: dict[str, str] | None = None,
    source: str | None = None,
    created_time: TimeRange | None = None,
    last_updated_time: TimeRange | None = None,
    root: bool | None = None,
    external_id_prefix: str | None = None,
    labels: LabelFilter | dict[str, Any] | None = None,
    geo_location: GeoLocationFilter | dict[str, Any] | None = None,
    aggregated_properties: bool | Sequence[AggregatedAssetProperty] = False,
    advanced_filter: dict[str, JsonValue] | None = None,
    sort: Sort | None = None,
    partition: str | None = None,
) -> dict[str, Any]:
    aggregated = _aggregated_properties(aggregated_properties)
    return _list_body(
        filter=filter,
        fields={
            "name": name,
            "parentIds": _int_ids(parent_ids),
            "parentExternalIds": _str_ids(parent_external_ids),
            "rootIds": _object_ids(root_ids),
            "assetSubtreeIds": _object_ids(asset_subtree_ids, asset_subtree_external_ids),
            "dataSetIds": _object_ids(data_set_ids, data_set_external_ids),
            "metadata": metadata,
            "source": source,
            "createdTime": _time_range(created_time),
            "lastUpdatedTime": _time_range(last_updated_time),
            "root": root,
            "externalIdPrefix": external_id_prefix,
            "labels": _dump_model(labels),
            "geoLocation": _dump_model(geo_location),
        },
        advanced_filter=advanced_filter,
        sort=sort,
        partition=partition,
        extra={"aggregatedProperties": aggregated} if aggregated is not None else None,
    )


def event_list_body(
    *,
    filter: ClassicFilter | dict[str, Any] | None = None,
    start_time: TimeRange | None = None,
    end_time: TimeRange | None = None,
    active_at_time: TimeRange | None = None,
    type: str | None = None,
    subtype: str | None = None,
    metadata: dict[str, str] | None = None,
    asset_ids: int | Sequence[int] | None = None,
    asset_external_ids: str | Sequence[str] | None = None,
    asset_subtree_ids: ObjectIds | None = None,
    asset_subtree_external_ids: str | Sequence[str] | None = None,
    data_set_ids: ObjectIds | None = None,
    data_set_external_ids: str | Sequence[str] | None = None,
    source: str | None = None,
    created_time: TimeRange | None = None,
    last_updated_time: TimeRange | None = None,
    external_id_prefix: str | None = None,
    advanced_filter: dict[str, JsonValue] | None = None,
    sort: Sort | None = None,
    partition: str | None = None,
) -> dict[str, Any]:
    return _list_body(
        filter=filter,
        fields={
            "startTime": _time_range(start_time),
            "endTime": _time_range(end_time),
            "activeAtTime": _time_range(active_at_time),
            "metadata": metadata,
            "assetIds": _int_ids(asset_ids),
            "assetExternalIds": _str_ids(asset_external_ids),
            "assetSubtreeIds": _object_ids(asset_subtree_ids, asset_subtree_external_ids),
            "dataSetIds": _object_ids(data_set_ids, data_set_external_ids),
            "source": source,
            "type": type,
            "subtype": subtype,
            "createdTime": _time_range(created_time),
            "lastUpdatedTime": _time_range(last_updated_time),
            "externalIdPrefix": external_id_prefix,
        },
        advanced_filter=advanced_filter,
        sort=sort,
        partition=partition,
    )


def timeseries_list_body(
    *,
    filter: ClassicFilter | dict[str, Any] | None = None,
    name: str | None = None,
    unit: str | None = None,
    unit_external_id: str | None = None,
    unit_quantity: str | None = None,
    is_string: bool | None = None,
    is_step: bool | None = None,
    metadata: dict[str, str] | None = None,
    asset_ids: int | Sequence[int] | None = None,
    asset_external_ids: str | Sequence[str] | None = None,
    root_asset_ids: int | Sequence[int] | None = None,
    asset_subtree_ids: ObjectIds | None = None,
    asset_subtree_external_ids: str | Sequence[str] | None = None,
    data_set_ids: ObjectIds | None = None,
    data_set_external_ids: str | Sequence[str] | None = None,
    external_id_prefix: str | None = None,
    created_time: TimeRange | None = None,
    last_updated_time: TimeRange | None = None,
    advanced_filter: dict[str, JsonValue] | None = None,
    sort: Sort | None = None,
    partition: str | None = None,
) -> dict[str, Any]:
    return _list_body(
        filter=filter,
        fields={
            "name": name,
            "unit": unit,
            "unitExternalId": unit_external_id,
            "unitQuantity": unit_quantity,
            "isString": is_string,
            "isStep": is_step,
            "metadata": metadata,
            "assetIds": _int_ids(asset_ids),
            "assetExternalIds": _str_ids(asset_external_ids),
            "rootAssetIds": _int_ids(root_asset_ids),
            "assetSubtreeIds": _object_ids(asset_subtree_ids, asset_subtree_external_ids),
            "dataSetIds": _object_ids(data_set_ids, data_set_external_ids),
            "externalIdPrefix": external_id_prefix,
            "createdTime": _time_range(created_time),
            "lastUpdatedTime": _time_range(last_updated_time),
        },
        advanced_filter=advanced_filter,
        sort=sort,
        partition=partition,
    )


def sequence_list_body(
    *,
    filter: ClassicFilter | dict[str, Any] | None = None,
    name: str | None = None,
    external_id_prefix: str | None = None,
    metadata: dict[str, str] | None = None,
    asset_ids: int | Sequence[int] | None = None,
    root_asset_ids: int | Sequence[int] | None = None,
    asset_subtree_ids: ObjectIds | None = None,
    asset_subtree_external_ids: str | Sequence[str] | None = None,
    data_set_ids: ObjectIds | None = None,
    data_set_external_ids: str | Sequence[str] | None = None,
    created_time: TimeRange | None = None,
    last_updated_time: TimeRange | None = None,
    advanced_filter: dict[str, JsonValue] | None = None,
    sort: Sort | None = None,
    partition: str | None = None,
) -> dict[str, Any]:
    return _list_body(
        filter=filter,
        fields={
            "name": name,
            "externalIdPrefix": external_id_prefix,
            "metadata": metadata,
            "assetIds": _int_ids(asset_ids),
            "rootAssetIds": _int_ids(root_asset_ids),
            "assetSubtreeIds": _object_ids(asset_subtree_ids, asset_subtree_external_ids),
            "createdTime": _time_range(created_time),
            "lastUpdatedTime": _time_range(last_updated_time),
            "dataSetIds": _object_ids(data_set_ids, data_set_external_ids),
        },
        advanced_filter=advanced_filter,
        sort=sort,
        partition=partition,
    )


def file_list_body(
    *,
    filter: ClassicFilter | dict[str, Any] | None = None,
    name: str | None = None,
    directory_prefix: str | None = None,
    mime_type: str | None = None,
    metadata: dict[str, str] | None = None,
    asset_ids: int | Sequence[int] | None = None,
    asset_external_ids: str | Sequence[str] | None = None,
    root_asset_ids: ObjectIds | None = None,
    root_asset_external_ids: str | Sequence[str] | None = None,
    data_set_ids: ObjectIds | None = None,
    data_set_external_ids: str | Sequence[str] | None = None,
    asset_subtree_ids: ObjectIds | None = None,
    asset_subtree_external_ids: str | Sequence[str] | None = None,
    source: str | None = None,
    created_time: TimeRange | None = None,
    last_updated_time: TimeRange | None = None,
    uploaded_time: TimeRange | None = None,
    source_created_time: TimeRange | None = None,
    source_modified_time: TimeRange | None = None,
    external_id_prefix: str | None = None,
    uploaded: bool | None = None,
    labels: LabelFilter | dict[str, Any] | None = None,
    geo_location: GeoLocationFilter | dict[str, Any] | None = None,
    partition: str | None = None,
) -> dict[str, Any]:
    return _list_body(
        filter=filter,
        fields={
            "name": name,
            "directoryPrefix": directory_prefix,
            "mimeType": mime_type,
            "metadata": metadata,
            "assetIds": _int_ids(asset_ids),
            "assetExternalIds": _str_ids(asset_external_ids),
            "rootAssetIds": _object_ids(root_asset_ids, root_asset_external_ids),
            "dataSetIds": _object_ids(data_set_ids, data_set_external_ids),
            "assetSubtreeIds": _object_ids(asset_subtree_ids, asset_subtree_external_ids),
            "source": source,
            "createdTime": _time_range(created_time),
            "lastUpdatedTime": _time_range(last_updated_time),
            "uploadedTime": _time_range(uploaded_time),
            "sourceCreatedTime": _time_range(source_created_time),
            "sourceModifiedTime": _time_range(source_modified_time),
            "externalIdPrefix": external_id_prefix,
            "uploaded": uploaded,
            "labels": _dump_model(labels),
            "geoLocation": _dump_model(geo_location),
        },
        partition=partition,
    )


def _list_body(
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
    elif isinstance(filter, dict):
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
    if isinstance(value, dict):
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
