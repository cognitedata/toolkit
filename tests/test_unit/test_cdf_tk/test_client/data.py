from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.client.api.assets import AssetsAPI
from cognite_toolkit._cdf_tk.client.api.events import EventsAPI
from cognite_toolkit._cdf_tk.client.api.timeseries import TimeSeriesAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI
from cognite_toolkit._cdf_tk.client.data_classes.agent import AgentRequest, AgentResponse
from cognite_toolkit._cdf_tk.client.data_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.data_classes.base import Identifier, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.data_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.data_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.data_classes.raw import (
    RAWDatabase,
    DatabaseResponse,
    RAWTable,
    TableResponse,
)
from cognite_toolkit._cdf_tk.client.data_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse


@dataclass
class CDFResource:
    response_cls: type[ResponseResource]
    request_cls: type[RequestResource]
    example_data: dict[str, Any]
    api_class: type[CDFResourceAPI] | None = None

    @cached_property
    def response_instance(self) -> ResponseResource:
        return self.response_cls.model_validate(self.example_data)

    @cached_property
    def request_instance(self) -> RequestResource:
        return self.response_instance.as_request_resource()

    @cached_property
    def resource_id(self) -> Identifier:
        return self.request_instance.as_id()


def get_example_minimum_responses(resource_cls: type[ResponseResource]) -> dict[str, Any]:
    """Return an example with the only required and identifier fields for the given resource class."""
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
            "isSting": False,
            "isStep": False,
            "type": "numeric",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        EventResponse: {
            "id": 789,
            "externalId": "event_001",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        FileMetadataResponse: {
            "id": 101,
            "externalId": "file_001",
            "name": "example.pdf",
            "uploaded": True,
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        AgentResponse: {
            "id": 202,
            "externalId": "agent_001",
            "name": "Example Agent",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        DatabaseResponse: {
            "dbName": "example_db",
        },
        TableResponse: {
            "dbName": "example_db",
            "tableName": "example_table",
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
            example_data=get_example_minimum_responses(AssetResponse),
            api_class=AssetsAPI,
        ),
        id="Asset",
    )
    yield pytest.param(
        CDFResource(
            response_cls=TimeSeriesResponse,
            request_cls=TimeSeriesRequest,
            example_data=get_example_minimum_responses(TimeSeriesResponse),
            api_class=TimeSeriesAPI,
        ),
        id="TimeSeries",
    )
    yield pytest.param(
        CDFResource(
            response_cls=EventResponse,
            request_cls=EventRequest,
            example_data=get_example_minimum_responses(EventResponse),
            api_class=EventsAPI,
        ),
        id="Event",
    )
    yield pytest.param(
        CDFResource(
            response_cls=FileMetadataResponse,
            request_cls=FileMetadataRequest,
            example_data=get_example_minimum_responses(FileMetadataResponse),
        ),
        id="FileMetadata",
    )
    yield pytest.param(
        CDFResource(
            response_cls=AgentResponse,
            request_cls=AgentRequest,
            example_data=get_example_minimum_responses(AgentResponse),
        ),
        id="Agent",
    )
    yield pytest.param(
        CDFResource(
            response_cls=DatabaseResponse,
            request_cls=RAWDatabase,
            example_data=get_example_minimum_responses(DatabaseResponse),
        ),
        id="Database",
    )
    yield pytest.param(
        CDFResource(
            response_cls=TableResponse,
            request_cls=RAWTable,
            example_data=get_example_minimum_responses(TableResponse),
        ),
        id="Table",
    )
