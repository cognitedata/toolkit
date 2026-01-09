from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.client.api.assets import AssetsAPI
from cognite_toolkit._cdf_tk.client.api.events import EventsAPI
from cognite_toolkit._cdf_tk.client.api.filemetadata import FileMetadataAPI
from cognite_toolkit._cdf_tk.client.api.timeseries import TimeSeriesAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI
from cognite_toolkit._cdf_tk.client.data_classes.agent import AgentRequest, AgentResponse
from cognite_toolkit._cdf_tk.client.data_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.data_classes.base import Identifier, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.data_classes.data_modeling import (
    ContainerRequest,
    ContainerResponse,
    DataModelRequest,
    DataModelResponse,
    SpaceRequest,
    SpaceResponse,
    ViewRequest,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.data_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.data_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.data_classes.raw import RAWDatabase, RAWTable
from cognite_toolkit._cdf_tk.client.data_classes.simulator_model import SimulatorModelRequest, SimulatorModelResponse
from cognite_toolkit._cdf_tk.client.data_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse


@dataclass
class CDFResource:
    response_cls: type[ResponseResource]
    request_cls: type[RequestResource]
    example_data: dict[str, Any]
    api_class: type[CDFResourceAPI] | None = None
    is_dump_equal_to_example: bool = True

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
            "isString": False,
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
            "externalId": "agent_001",
            "name": "Example Agent",
            "ownerId": "user@example.com",
            "runtimeVersion": "v1",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        RAWDatabase: {
            "name": "example_db",
        },
        RAWTable: {
            "dbName": "example_db",
            "name": "example_table",
        },
        SimulatorModelResponse: {
            "id": 111,
            "externalId": "simulator_model_001",
            "simulatorExternalId": "simulator_001",
            "name": "Example Simulator Model",
            "dataSetId": 123456,
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            # 'type' is not required in the response, but is required in the request. Likely a bug in the CDF API docs.
            "type": "default",
        },
        SpaceResponse: {
            "space": "my_space",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "isGlobal": False,
        },
        ContainerResponse: {
            "space": "my_space",
            "externalId": "my_container",
            "properties": {
                "name": {
                    "type": {"type": "text", "list": False, "collation": "ucs_basic"},
                    "nullable": True,
                },
            },
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "isGlobal": False,
        },
        DataModelResponse: {
            "space": "my_space",
            "externalId": "my_data_model",
            "version": "1",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "isGlobal": False,
        },
        ViewResponse: {
            "space": "my_space",
            "externalId": "my_view",
            "version": "1",
            "filter": None,
            "properties": {},
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "isGlobal": False,
            "mappedContainers": [],
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
            api_class=FileMetadataAPI,
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
            response_cls=RAWDatabase,
            request_cls=RAWDatabase,
            example_data=get_example_minimum_responses(RAWDatabase),
        ),
        id="RAWDatabase",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RAWTable,
            request_cls=RAWTable,
            example_data=get_example_minimum_responses(RAWTable),
            is_dump_equal_to_example=False,
        ),
        id="RAWTable",
    )
    yield pytest.param(
        CDFResource(
            response_cls=SimulatorModelResponse,
            request_cls=SimulatorModelRequest,
            example_data=get_example_minimum_responses(SimulatorModelResponse),
        ),
        id="SimulatorModel",
    )
    yield pytest.param(
        CDFResource(
            response_cls=SpaceResponse,
            request_cls=SpaceRequest,
            example_data=get_example_minimum_responses(SpaceResponse),
        ),
        id="Space",
    )
    yield pytest.param(
        CDFResource(
            response_cls=ContainerResponse,
            request_cls=ContainerRequest,
            example_data=get_example_minimum_responses(ContainerResponse),
        ),
        id="Container",
    )
    yield pytest.param(
        CDFResource(
            response_cls=DataModelResponse,
            request_cls=DataModelRequest,
            example_data=get_example_minimum_responses(DataModelResponse),
        ),
        id="DataModel",
    )
    yield pytest.param(
        CDFResource(
            response_cls=ViewResponse,
            request_cls=ViewRequest,
            example_data=get_example_minimum_responses(ViewResponse),
        ),
        id="View",
    )
