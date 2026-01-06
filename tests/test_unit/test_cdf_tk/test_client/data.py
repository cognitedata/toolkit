from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.client.api.assets import AssetsAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI
from cognite_toolkit._cdf_tk.client.data_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.data_classes.base import RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.data_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.data_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse


@dataclass
class CDFResource:
    response_cls: type[ResponseResource]
    request_cls: type[RequestResource]
    example_data: dict[str, Any]
    api_class: type[CDFResourceAPI] | None = None


def get_example_response(resource_cls: type[ResponseResource]) -> dict[str, Any]:
    responses: dict[type[ResponseResource], dict[str, Any]] = {
        AssetResponse: {
            "id": 123,
            "externalId": "asset_001",
            "name": "Example Asset",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "rootId": 1,
        },
        TimeSeriesResponse: {
            "id": 456,
            "externalId": "ts_001",
            "name": "Example Time Series",
            "isSting": False,
            "isStep": False,
            "type": "numeric",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        EventResponse: {
            "id": 789,
            "externalId": "event_001",
            "type": "Example Event Type",
            "startTime": 1622547800000,
            "endTime": 1622547900000,
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
    }
    try:
        return responses[resource_cls]
    except KeyError:
        raise ValueError(f"No example response defined for {resource_cls}")


def iterate_cdf_resources() -> Iterable[tuple]:
    yield pytest.param(
        CDFResource(
            response_cls=AssetResponse,
            request_cls=AssetRequest,
            example_data=get_example_response(AssetResponse),
            api_class=AssetsAPI,
        ),
        id="Asset",
    )
    yield pytest.param(
        CDFResource(
            response_cls=TimeSeriesResponse,
            request_cls=TimeSeriesRequest,
            example_data=get_example_response(TimeSeriesResponse),
        ),
        id="TimeSeries",
    )
    yield pytest.param(
        CDFResource(
            response_cls=EventResponse,
            request_cls=EventRequest,
            example_data=get_example_response(EventResponse),
        ),
        id="Event",
    )
