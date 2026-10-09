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
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    ClassicFilter,
    EpochTimestampRange,
    GeoLocationFilter,
    LabelFilter,
)


def _request_json(request: httpx2.Request) -> dict[str, Any]:
    raw = request.content
    if len(raw) >= 2 and raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    loaded = json.loads(raw)
    assert isinstance(loaded, dict)
    return loaded


def _capture_list(
    api: CDFResourceAPI[Any],
    toolkit_config: ToolkitClientConfig,
    respx_mock: respx.MockRouter,
) -> list[dict[str, Any]]:
    captured: list[dict[str, Any]] = []

    def capture(request: httpx2.Request) -> httpx2.Response:
        captured.append(_request_json(request))
        return httpx2.Response(status_code=200, json={"items": []})

    list_url = toolkit_config.create_api_url(api._method_endpoint_map["list"].path)
    respx_mock.post(list_url).mock(side_effect=capture)
    return captured


def test_asset_list_iterate_paginate_send_every_filter(
    toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
) -> None:
    api = AssetsAPI(HTTPClient(toolkit_config))
    captured = _capture_list(api, toolkit_config, respx_mock)
    kwargs: dict[str, Any] = {
        "aggregated_properties": ["child_count", "path"],
        "filter": ClassicFilter(asset_subtree_ids=[ExternalId(external_id="root")]),
        "name": "pump",
        "parent_ids": 2,
        "parent_external_ids": ["parent"],
        "root_ids": InternalId(id=3),
        "data_set_ids": [4],
        "data_set_external_ids": "set",
        "metadata": {"k": "v"},
        "source": "pi",
        "created_time": EpochTimestampRange(min_=1, max_=2),
        "last_updated_time": {"min": 3},
        "root": False,
        "external_id_prefix": "pre",
        "labels": LabelFilter(contains_any=["PUMP"]),
        "geo_location": GeoLocationFilter(relation="INTERSECTS", shape={"type": "Point", "coordinates": [0, 0]}),
        "advanced_filter": {"equals": {"property": ["name"], "value": "pump"}},
        "sort": {"property": ["name"], "order": "asc"},
        "partition": "1/2",
    }
    expected_filter = {
        "assetSubtreeIds": [{"externalId": "root"}],
        "name": "pump",
        "parentIds": [2],
        "parentExternalIds": ["parent"],
        "rootIds": [{"id": 3}],
        "dataSetIds": [{"id": 4}, {"externalId": "set"}],
        "metadata": {"k": "v"},
        "source": "pi",
        "createdTime": {"min": 1, "max": 2},
        "lastUpdatedTime": {"min": 3},
        "root": False,
        "externalIdPrefix": "pre",
        "labels": {"containsAny": [{"externalId": "PUMP"}]},
        "geoLocation": {"relation": "INTERSECTS", "shape": {"type": "Point", "coordinates": [0, 0]}},
    }
    expected_common = {
        "aggregatedProperties": ["childCount", "path"],
        "advancedFilter": kwargs["advanced_filter"],
        "sort": [{"property": ["name"], "order": "asc"}],
        "partition": "1/2",
        "filter": expected_filter,
        "limit": 10,
    }

    api.paginate(limit=10, cursor="cursor", **kwargs)
    assert captured[0] == {**expected_common, "cursor": "cursor"}
    api.list(limit=10, **kwargs)
    assert captured[1] == expected_common
    list(api.iterate(limit=10, **kwargs))
    assert captured[2] == expected_common


def test_event_list_merges_dict_filter_with_arguments(
    toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
) -> None:
    api = EventsAPI(HTTPClient(toolkit_config))
    captured = _capture_list(api, toolkit_config, respx_mock)
    api.list(
        filter={"source": "kept"},
        limit=5,
        type="failure",
        subtype="electrical",
        asset_ids=[1],
        asset_external_ids="asset",
        start_time={"max": 10},
        end_time=EpochTimestampRange(min_=4),
        active_at_time={"min": 1, "max": 9},
        external_id_prefix="evt",
        advanced_filter={"exists": {"property": ["description"]}},
        sort=[{"property": ["createdTime"], "order": "desc", "nulls": "last"}],
        partition="2/3",
    )
    assert captured[0] == {
        "limit": 5,
        "advancedFilter": {"exists": {"property": ["description"]}},
        "sort": [{"property": ["createdTime"], "order": "desc", "nulls": "last"}],
        "partition": "2/3",
        "filter": {
            "source": "kept",
            "type": "failure",
            "subtype": "electrical",
            "assetIds": [1],
            "assetExternalIds": ["asset"],
            "startTime": {"max": 10},
            "endTime": {"min": 4},
            "activeAtTime": {"min": 1, "max": 9},
            "externalIdPrefix": "evt",
        },
    }


def test_timeseries_and_sequence_filters(toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
    config = toolkit_config
    timeseries = TimeSeriesAPI(HTTPClient(config))
    sequences = SequencesAPI(HTTPClient(config))
    captured = _capture_list(timeseries, config, respx_mock)
    timeseries.list(
        limit=3,
        name="temp",
        unit="C",
        unit_external_id="temperature",
        unit_quantity="Temperature",
        is_string=False,
        is_step=True,
        asset_ids=[9],
        root_asset_ids=8,
        external_id_prefix="ts",
        partition="1/10",
    )
    assert captured[0]["filter"] == {
        "name": "temp",
        "unit": "C",
        "unitExternalId": "temperature",
        "unitQuantity": "Temperature",
        "isString": False,
        "isStep": True,
        "assetIds": [9],
        "rootAssetIds": [8],
        "externalIdPrefix": "ts",
    }
    assert captured[0]["partition"] == "1/10"

    sequence_captured = _capture_list(sequences, config, respx_mock)
    sequences.paginate(
        limit=4,
        cursor="next",
        name="depth",
        root_asset_ids=[8, 9],
        metadata={"extracted-by": "cognite"},
        advanced_filter={"prefix": {"property": ["name"], "value": "dep"}},
    )
    assert sequence_captured[0] == {
        "limit": 4,
        "cursor": "next",
        "advancedFilter": {"prefix": {"property": ["name"], "value": "dep"}},
        "filter": {
            "name": "depth",
            "rootAssetIds": [8, 9],
            "metadata": {"extracted-by": "cognite"},
        },
    }


def test_file_list_filters(toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
    api = FileMetadataAPI(HTTPClient(toolkit_config))
    captured = _capture_list(api, toolkit_config, respx_mock)
    list(
        api.iterate(
            filter=ClassicFilter(data_set_ids=[InternalId(id=1)]),
            directory_prefix="/docs",
            uploaded=False,
            limit=2,
            name="manual",
            mime_type="application/pdf",
            asset_external_ids="asset",
            root_asset_ids="root-asset",
            source="sap",
            external_id_prefix="file",
            labels=LabelFilter(contains_all=["VERIFIED"]),
            partition="1/3",
        )
    )
    assert captured[0] == {
        "limit": 2,
        "partition": "1/3",
        "filter": {
            "dataSetIds": [{"id": 1}],
            "directoryPrefix": "/docs",
            "uploaded": False,
            "name": "manual",
            "mimeType": "application/pdf",
            "assetExternalIds": ["asset"],
            "rootAssetIds": [{"externalId": "root-asset"}],
            "source": "sap",
            "externalIdPrefix": "file",
            "labels": {"containsAll": [{"externalId": "VERIFIED"}]},
        },
    }
    assert "advancedFilter" not in captured[0]
    assert "sort" not in captured[0]


def test_explicit_filter_argument_replaces_same_field_from_filter_object(
    toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
) -> None:
    api = AssetsAPI(HTTPClient(toolkit_config))
    captured = _capture_list(api, toolkit_config, respx_mock)
    api.list(
        limit=1,
        filter=ClassicFilter(data_set_ids=[InternalId(id=1)], asset_subtree_ids=[InternalId(id=2)]),
        data_set_external_ids="replacement",
    )
    assert captured[0]["filter"] == {
        "assetSubtreeIds": [{"id": 2}],
        "dataSetIds": [{"externalId": "replacement"}],
    }


def test_unknown_aggregated_property_is_rejected(
    toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
) -> None:
    api = AssetsAPI(HTTPClient(toolkit_config))
    _capture_list(api, toolkit_config, respx_mock)
    with pytest.raises(ValueError, match="Unknown aggregated property"):
        api.list(limit=1, aggregated_properties=["not-a-property"])  # type: ignore[list-item]
