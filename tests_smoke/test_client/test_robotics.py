from contextlib import suppress
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Generic, TypeAlias

import pytest
from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import (
    T_Identifier,
    T_RequestResource,
    T_ResponseResource,
)
from cognite_toolkit._cdf_tk.client.api.robotics_capabilities import CapabilitiesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_data_postprocessing import DataPostProcessingAPI
from cognite_toolkit._cdf_tk.client.api.robotics_frames import FramesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_locations import LocationsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_maps import MapsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_robots import RobotsAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.robotics import (
    RobotCapabilityRequest,
    RobotCapabilityResponse,
    RobotDataPostProcessingRequest,
    RobotDataPostProcessingResponse,
    RobotFrameRequest,
    RobotFrameResponse,
    RobotLocationRequest,
    RobotLocationResponse,
    RobotMapRequest,
    RobotMapResponse,
    RobotRequest,
    RobotResponse,
)
from tests_smoke.exceptions import EndpointAssertionError

DESCRIPTIONS = ["Initial description", "Updated description for testing"]

RoboticsAPIType: TypeAlias = CapabilitiesAPI | DataPostProcessingAPI | FramesAPI | LocationsAPI | MapsAPI | RobotsAPI


@dataclass
class CDFResource(Generic[T_Identifier, T_RequestResource, T_ResponseResource]):
    response_cls: type[T_ResponseResource]
    request_cls: type[T_RequestResource]
    example_request: dict[str, Any]
    example_update: dict[str, Any]
    api_class: type[CDFResourceAPI[T_Identifier, T_RequestResource, T_ResponseResource]]

    @cached_property
    def example_request_instance(self) -> T_RequestResource:
        return self.request_cls.model_validate(self.example_request)

    @property
    def identifier(self) -> T_Identifier:
        return self.example_request_instance.as_id()  # type: ignore[return-value]


ROOT_FRAME_EXTERNAL_ID = "rootCoordinateFrame"


@pytest.fixture(scope="session")
def root_frame(toolkit_client: ToolkitClient) -> RobotFrameResponse:
    root = RobotFrameRequest(
        name="Root coordinate frame",
        external_id=ROOT_FRAME_EXTERNAL_ID,
    )
    try:
        return toolkit_client.tool.robotics.frames.retrieve([root.as_id()])[0]
    except ToolkitAPIError:
        return toolkit_client.tool.robotics.frames.create([root])[0]


@pytest.fixture(scope="session")
def persistent_robots_data_set(toolkit_client: ToolkitClient) -> DataSetResponse:
    data_set = DataSetRequest(
        external_id="ds_robotics_api_tests_persistent",
        name="Robotics API Tests Persistent",
        description="Data set for testing the Robotics API with persistent data",
    )
    retrieved = toolkit_client.tool.datasets.retrieve([data_set.as_id()], ignore_unknown_ids=True)
    if retrieved:
        return retrieved[0]
    else:
        return toolkit_client.tool.datasets.create([data_set])[0]


def robotic_api_resource_definitions() -> dict[str, CDFResource]:
    return {
        "capabilities": CDFResource(
            response_cls=RobotCapabilityResponse,
            request_cls=RobotCapabilityRequest,
            example_request={
                "name": "ptz",
                "external_id": "ptz",
                "method": "ptz",
                "input_schema": INPUT_SCHEMA_CAPABILITY,
                "data_handling_schema": DATA_HANDLING_SCHEMA_CAPABILITY,
                "description": "Pan, tilt, zoom camera for visual image capture",
            },
            example_update={
                "description": "Updated description",
            },
            api_class=CapabilitiesAPI,
        ),
        "data_postprocessing": CDFResource(
            response_cls=RobotDataPostProcessingResponse,
            request_cls=RobotDataPostProcessingRequest,
            example_request={
                "name": "Read dial gauge",
                "external_id": "read_dial_gauge",
                "method": "read_dial_gauge",
                "input_schema": INPUT_SCHEMA_DATA_PROCESSING,
                "description": "Read dial gauge from an image using Cognite Vision gauge reader",
            },
            example_update={
                "description": "Updated description",
            },
            api_class=DataPostProcessingAPI,
        ),
        "maps": CDFResource(
            response_cls=RobotMapResponse,
            request_cls=RobotMapRequest,
            example_request={
                "name": "Robot navigation map",
                "external_id": "robotMap",
                "map_type": "POINTCLOUD",
                "description": "Robot navigation map",
                "scale": 1.0,
            },
            example_update={
                "description": "Updated description",
            },
            api_class=MapsAPI,
        ),
        "locations": CDFResource(
            response_cls=RobotLocationResponse,
            request_cls=RobotLocationRequest,
            example_request={
                "name": "Water treatment plant",
                "external_id": "waterTreatmentPlant1",
                "description": "Water treatment plant location",
            },
            example_update={
                "description": "Updated description",
            },
            api_class=LocationsAPI,
        ),
        "frames": CDFResource(
            response_cls=RobotFrameResponse,
            request_cls=RobotFrameRequest,
            example_request={
                "name": "Root coordinate frame of a location",
                "external_id": "rootCoordinateFrameLocation1",
                "transform": {
                    "parentFrameExternalId": ROOT_FRAME_EXTERNAL_ID,
                    "translation": {"x": 0, "y": 0, "z": 0},
                    "orientation": {"x": 0, "y": 0, "z": 0, "w": 1},
                },
            },
            example_update={
                "name": "Updated name",
            },
            api_class=FramesAPI,
        ),
        "robots": CDFResource(
            response_cls=RobotResponse,
            request_cls=RobotRequest,
            example_request={
                "name": "wall-e",
                "capabilities": [],
                "robot_type": "DJI_DRONE",
                "description": "Test robot",
                "metadata": {},
            },
            example_update={
                "description": "Updated description",
            },
            api_class=RobotsAPI,
        ),
    }


DATA_HANDLING_SCHEMA_CAPABILITY: dict[str, JsonValue] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "id": "robotics/schemas/0.1.0/data_handling/ptz",
    "type": "object",
    "properties": {
        "uploadInstructions": {
            "type": "object",
            "properties": {
                "image": {
                    "type": "object",
                    "properties": {
                        "method": {"const": "uploadFile"},
                        "parameters": {
                            "type": "object",
                            "properties": {"filenamePrefix": {"type": "string"}},
                            "required": ["filenamePrefix"],
                        },
                    },
                    "required": ["method", "parameters"],
                    "additionalProperties": False,
                }
            },
            "additionalProperties": False,
        }
    },
    "required": ["uploadInstructions"],
}

INPUT_SCHEMA_CAPABILITY: dict[str, JsonValue] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "id": "robotics/schemas/0.1.0/capabilities/ptz",
    "title": "PTZ camera capability input",
    "type": "object",
    "properties": {
        "method": {"type": "string"},
        "parameters": {
            "type": "object",
            "properties": {
                "tilt": {"type": "number", "minimum": -90, "maximum": 90},
                "pan": {"type": "number", "minimum": -180, "maximum": 180},
                "zoom": {"type": "number", "minimum": 0, "maximum": 100},
            },
            "required": ["tilt", "pan", "zoom"],
        },
    },
    "required": ["method", "parameters"],
    "additionalProperties": False,
}

INPUT_SCHEMA_DATA_PROCESSING: dict[str, JsonValue] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "id": "robotics/schemas/0.1.0/data_postprocessing/read_dial_gauge",
    "title": "Read dial gauge postprocessing input",
    "type": "object",
    "properties": {
        "image": {
            "type": "object",
            "properties": {
                "method": {"type": "string"},
                "parameters": {
                    "type": "object",
                    "properties": {
                        "unit": {"type": "string"},
                        "deadAngle": {"type": "number"},
                        "minLevel": {"type": "number"},
                        "maxLevel": {"type": "number"},
                    },
                },
            },
            "required": ["method", "parameters"],
            "additionalProperties": False,
        }
    },
    "additionalProperties": False,
}


class TestRoboticsAPI:
    @pytest.mark.usefixtures("root_frame")
    @pytest.mark.parametrize(
        "resource_def", [pytest.param(value, id=key) for key, value in robotic_api_resource_definitions().items()]
    )
    def test_crud_and_list(
        self,
        persistent_robots_data_set: DataSetResponse,
        toolkit_client: ToolkitClient,
        resource_def: CDFResource,
    ) -> None:
        api: RoboticsAPIType = resource_def.api_class(toolkit_client.http_client)  # type: ignore[call-arg,assignment]

        if not issubclass(resource_def.request_cls, RobotRequest):
            request = resource_def.example_request_instance
        else:
            example = resource_def.example_request.copy()
            example["dataSetId"] = persistent_robots_data_set.id
            # example["capabilities"] = [existing_capability.external_id]
            request = RobotRequest.model_validate(example)

        identifier = request.as_id()

        # Ensure clean state
        with suppress(ToolkitAPIError):
            api.delete([identifier])

        try:
            # Create
            created = api.create([request])
            create_endpoint = api._method_endpoint_map["create"].path
            if len(created) != 1:
                raise EndpointAssertionError(create_endpoint, "Expected exactly one created item")
            if not isinstance(created[0], resource_def.response_cls):
                raise EndpointAssertionError(
                    create_endpoint, f"Expected item of type {resource_def.response_cls.__name__}"
                )

            # Retrieve
            retrieved = api.retrieve([identifier])
            retrieve_endpoint = api._method_endpoint_map["retrieve"].path
            if len(retrieved) != 1:
                raise EndpointAssertionError(retrieve_endpoint, "Expected exactly one retrieved item")
            if not isinstance(retrieved[0], resource_def.response_cls):
                raise EndpointAssertionError(
                    retrieve_endpoint, f"Expected item of type {resource_def.response_cls.__name__}"
                )
            retrieved_dumped = retrieved[0].as_request_resource().model_dump(by_alias=True, exclude_none=True)
            request_dumped = request.model_dump(by_alias=True, exclude_none=True)
            if retrieved_dumped != request_dumped:
                raise EndpointAssertionError(retrieve_endpoint, "Retrieved item does not match the created item")

            listed = api.list(limit=None)
            list_endpoint = api._method_endpoint_map["list"].path
            if not any(item.as_id() == identifier for item in listed):
                raise EndpointAssertionError(list_endpoint, "Created item not found in list")

            # Update
            update_instance = request.model_copy(update=resource_def.example_update)
            updated = api.update([update_instance])
            update_endpoint = api._method_endpoint_map["update"].path
            if len(updated) != 1:
                raise EndpointAssertionError(update_endpoint, "Expected exactly one updated item")
            if not isinstance(updated[0], resource_def.response_cls):
                raise EndpointAssertionError(
                    update_endpoint, f"Expected item of type {resource_def.response_cls.__name__}"
                )
            if updated[0].as_request_resource().model_dump() != update_instance.model_dump():
                raise EndpointAssertionError(update_endpoint, "Updated item does not match the update data")
        finally:
            # Delete
            api.delete([identifier])

            with pytest.raises(ToolkitAPIError):
                api.retrieve([identifier])
