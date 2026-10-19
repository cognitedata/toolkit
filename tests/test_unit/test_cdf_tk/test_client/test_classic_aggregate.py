import gzip
import json
from typing import Any

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.assets import AssetsAPI
from cognite_toolkit._cdf_tk.client.api.events import EventsAPI
from cognite_toolkit._cdf_tk.client.api.filemetadata import FileMetadataAPI
from cognite_toolkit._cdf_tk.client.api.sequences import SequencesAPI
from cognite_toolkit._cdf_tk.client.api.timeseries import TimeSeriesAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.classic_aggregate import ClassicAggregateUniqueBucket


def _request_json(request: httpx2.Request) -> dict[str, Any]:
    raw = request.content
    if len(raw) >= 2 and raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    loaded = json.loads(raw)
    if not isinstance(loaded, dict):
        raise AssertionError(f"Expected a JSON object, got {type(loaded)}")
    return loaded


def _aggregate_response(request: httpx2.Request) -> httpx2.Response:
    payload = _request_json(request)
    aggregate = payload.get("aggregate")
    if aggregate in (None, "count", "cardinalityValues", "cardinalityProperties"):
        return httpx2.Response(status_code=200, json={"items": [{"count": 11}]})
    if aggregate == "uniqueValues":
        return httpx2.Response(status_code=200, json={"items": [{"count": 2, "values": ["pump"]}]})
    if aggregate == "uniqueProperties":
        return httpx2.Response(
            status_code=200,
            json={"items": [{"count": 3, "values": [{"property": ["metadata", "tag"]}]}]},
        )
    raise AssertionError(f"unexpected aggregate {aggregate!r}")


@pytest.mark.parametrize(
    "api_cls, value_property",
    [
        (AssetsAPI, ("name",)),
        (EventsAPI, ("source",)),
        (TimeSeriesAPI, ("unit",)),
        (SequencesAPI, ("name",)),
    ],
)
def test_classic_aggregate_payloads(
    api_cls: type[CDFResourceAPI[Any]],
    value_property: tuple[str, ...],
    toolkit_config: ToolkitClientConfig,
    respx_mock: respx.MockRouter,
) -> None:
    api = api_cls(HTTPClient(toolkit_config))
    aggregate_url = toolkit_config.create_api_url(api._method_endpoint_map["aggregate"].path)
    captured: list[dict[str, Any]] = []

    def capture(request: httpx2.Request) -> httpx2.Response:
        captured.append(
            {
                "body": _request_json(request),
                "cdf_version": request.headers.get("cdf-version"),
            }
        )
        return _aggregate_response(request)

    respx_mock.post(aggregate_url).mock(side_effect=capture)
    strict_filter = ClassicFilter(data_set_ids=[InternalId(id=7)])
    advanced_filter = {"prefix": {"property": ["name"], "value": "pump"}}
    aggregate_filter = {"prefix": {"value": "t"}}

    count = api.count()  # type: ignore[attr-defined]
    filtered_count = api.count(filter=strict_filter, advanced_filter=advanced_filter)  # type: ignore[attr-defined]
    value_cardinality = api.cardinality(value_property, aggregate_filter=aggregate_filter)  # type: ignore[attr-defined]
    metadata_cardinality = api.cardinality(("metadata",))  # type: ignore[attr-defined]
    value_unique = api.unique(value_property)  # type: ignore[attr-defined]
    metadata_unique = api.unique(("metadata",), aggregate_filter=aggregate_filter)  # type: ignore[attr-defined]

    assert {
        "results": {
            "count": count,
            "filtered_count": filtered_count,
            "value_cardinality": value_cardinality,
            "metadata_cardinality": metadata_cardinality,
            "value_unique": [(bucket.count, bucket.value) for bucket in value_unique],
            "metadata_unique": [(bucket.count, bucket.value) for bucket in metadata_unique],
        },
        "bodies": [item["body"] for item in captured],
        "versions": {item["cdf_version"] for item in captured},
    } == {
        "results": {
            "count": 11,
            "filtered_count": 11,
            "value_cardinality": 11,
            "metadata_cardinality": 11,
            "value_unique": [(2, "pump")],
            "metadata_unique": [(3, {"property": ["metadata", "tag"]})],
        },
        "bodies": [
            {"aggregate": "count"},
            {
                "aggregate": "count",
                "filter": {"dataSetIds": [{"id": 7}]},
                "advancedFilter": advanced_filter,
            },
            {
                "aggregate": "cardinalityValues",
                "properties": [{"property": list(value_property)}],
                "aggregateFilter": aggregate_filter,
            },
            {"aggregate": "cardinalityProperties", "path": ["metadata"]},
            {"aggregate": "uniqueValues", "properties": [{"property": list(value_property)}]},
            {
                "aggregate": "uniqueProperties",
                "path": ["metadata"],
                "aggregateFilter": aggregate_filter,
            },
        ],
        "versions": {"20230101"},
    }


def test_assets_count_with_property(toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
    api = AssetsAPI(HTTPClient(toolkit_config))
    aggregate_url = toolkit_config.create_api_url("/assets/aggregate")
    captured: list[dict[str, Any]] = []

    def capture(request: httpx2.Request) -> httpx2.Response:
        captured.append(_request_json(request))
        return httpx2.Response(status_code=200, json={"items": [{"count": 4}]})

    respx_mock.post(aggregate_url).mock(side_effect=capture)
    count = api.count(property=("metadata", "timezone"))

    assert {"count": count, "body": captured[0]} == {
        "count": 4,
        "body": {"aggregate": "count", "properties": [{"property": ["metadata", "timezone"]}]},
    }


def test_files_aggregate_count(toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
    api = FileMetadataAPI(HTTPClient(toolkit_config))
    aggregate_url = toolkit_config.create_api_url("/files/aggregate")
    captured: list[dict[str, Any]] = []

    def capture(request: httpx2.Request) -> httpx2.Response:
        captured.append(
            {
                "body": _request_json(request),
                "cdf_version": request.headers.get("cdf-version"),
            }
        )
        return httpx2.Response(status_code=200, json={"items": [{"count": 9}]})

    respx_mock.post(aggregate_url).mock(side_effect=capture)
    total = api.count()
    filtered = api.count(filter={"uploaded": True, "directoryPrefix": "/drawings"})

    assert {
        "total": total,
        "filtered": filtered,
        "bodies": [item["body"] for item in captured],
        "versions": {item["cdf_version"] for item in captured},
    } == {
        "total": 9,
        "filtered": 9,
        "bodies": [{}, {"filter": {"uploaded": True, "directoryPrefix": "/drawings"}}],
        "versions": {"20230101"},
    }


def test_unique_bucket_normalizes_singular_value() -> None:
    bucket = ClassicAggregateUniqueBucket.model_validate({"count": 1, "value": "pump"})
    assert {"count": bucket.count, "values": bucket.values, "value": bucket.value} == {
        "count": 1,
        "values": ["pump"],
        "value": "pump",
    }
