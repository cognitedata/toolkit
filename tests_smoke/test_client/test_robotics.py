from contextlib import suppress
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Generic, TypeAlias

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.api.robotics_capabilities import CapabilitiesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_data_postprocessing import DataPostProcessingAPI
from cognite_toolkit._cdf_tk.client.api.robotics_frames import FramesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_locations import LocationsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_maps import MapsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_robots import RobotsAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.base import (
    T_Identifier,
    T_RequestResource,
    T_ResponseResource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.robotics import (
    Point3D,
    Quaternion,
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
    Transform,
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
    def example_update_instance(self) -> T_RequestResource:
        return self.example_request_instance.model_copy(update=self.example_update)

    @property
    def identifier(self) -> T_Identifier:
        return self.example_request_instance.as_id()  # type: ignore[return-value]


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
                "name": "Root coordinate frame",
                "external_id": "rootCoordinateFrame",
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
            },
            example_update={
                "description": "Updated description",
            },
            api_class=RobotsAPI,
        ),
    }


DATA_HANDLING_SCHEMA_CAPABILITY = {
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

INPUT_SCHEMA_CAPABILITY = {
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

INPUT_SCHEMA_DATA_PROCESSING = {
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
    @pytest.mark.parametrize(
        "resource_def", [pytest.param(value, id=key) for key, value in robotic_api_resource_definitions().items()]
    )
    def test_crud_list(self, toolkit_client: ToolkitClient, resource_def: CDFResource) -> None:
        api: RoboticsAPIType = resource_def.api_class(toolkit_client.http_client)  # type: ignore[call-arg,assignment]

        request = resource_def.example_request_instance

        # Ensure clean state
        with suppress(ToolkitAPIError):
            api.delete([resource_def.identifier])

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
            retrieved = api.retrieve([resource_def.identifier])
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
            if not any(item.external_id == created[0].external_id for item in listed):
                raise EndpointAssertionError(list_endpoint, "Created item not found in list")

            # Update
            update_instance = resource_def.example_update_instance
            updated = api.update([update_instance])
            update_endpoint = api._method_endpoint_map["update"].path
            if len(updated) != 1:
                raise EndpointAssertionError(update_endpoint, "Expected exactly one updated item")
            if not isinstance(updated[0], resource_def.response_cls):
                raise EndpointAssertionError(
                    update_endpoint, f"Expected item of type {resource_def.response_cls.__name__}"
                )
            if updated[0].as_write().model_dump() != update_instance.model_dump():
                raise EndpointAssertionError(update_endpoint, "Updated item does not match the update data")
        finally:
            # Delete
            api.delete([resource_def.identifier])

            with pytest.raises(ToolkitAPIError):
                api.retrieve([resource_def.identifier])


@pytest.fixture(scope="session")
def root_frame(toolkit_client: ToolkitClient) -> RobotFrameResponse:
    root = RobotFrameRequest(
        name="Root coordinate frame",
        external_id="rootCoordinateFrame",
    )
    try:
        return toolkit_client.tool.robotics.frames.retrieve(root.external_id)
    except ToolkitAPIError:
        return toolkit_client.tool.robotics.frames.create(root)


FRAME_NAMES = ["Root coordinate frame of a location", "Updated name"]


@pytest.fixture(scope="session")
def existing_frame(toolkit_client: ToolkitClient, root_frame: RobotFrameResponse) -> RobotFrameResponse:
    location = RobotFrameRequest(
        name=FRAME_NAMES[0],
        external_id="rootCoordinateFrame",
        transform=Transform(
            parent_frame_external_id=root_frame.external_id,
            translation=Point3D(x=0, y=0, z=0),
            orientation=Quaternion(x=0, y=0, z=0, w=1),
        ),
    )
    try:
        return toolkit_client.tool.robotics.frames.retrieve(location.external_id)
    except ToolkitAPIError:
        return toolkit_client.tool.robotics.frames.create(location)


@pytest.fixture(scope="session")
def existing_robots_data_set(toolkit_client: ToolkitClient) -> DataSetResponse:
    data_set = DataSetRequest(
        external_id="ds_robotics_api_tests",
        name="Robotics API Tests",
        description="Data set for testing the Robotics API",
    )
    retrieved = toolkit_client.data_sets.retrieve(external_id=data_set.external_id)
    if retrieved:
        return retrieved
    else:
        return toolkit_client.data_sets.create(data_set)


@pytest.fixture(scope="session")
def persistent_robots_data_set(toolkit_client: ToolkitClient) -> DataSetResponse:
    data_set = DataSetRequest(
        external_id="ds_robotics_api_tests_persistent",
        name="Robotics API Tests Persistent",
        description="Data set for testing the Robotics API with persistent data",
    )
    retrieved = toolkit_client.data_sets.retrieve(external_id=data_set.external_id)
    if retrieved:
        return retrieved
    else:
        return toolkit_client.data_sets.create(data_set)


@pytest.fixture(scope="session")
def existing_robot(
    toolkit_client: ToolkitClient,
    persistent_robots_data_set: DataSetResponse,
    existing_capability: RobotCapabilityResponse,
) -> RobotResponse:
    robot = RobotRequest(
        name="wall-e",
        capabilities=[existing_capability.external_id],
        robot_type="DJI_DRONE",
        data_set_id=persistent_robots_data_set.id,
        description=DESCRIPTIONS[0],
    )
    try:
        found = toolkit_client.tool.robotics.robots.list()
        return next(r for r in found if r.name == robot.name)
    except (ToolkitAPIError, StopIteration):
        return toolkit_client.tool.robotics.robots.create(robot)


@pytest.mark.skip("Robot API seems to fail if you have two robots. This causes every other test run to fail.")
class TestRobotsAPI:
    def test_create_retrieve_delete(
        self,
        toolkit_client: ToolkitClient,
        existing_robots_data_set: DataSetResponse,
        existing_capability: RobotCapabilityResponse,
    ) -> None:
        robot = RobotRequest(
            name="test_robot",
            capabilities=[],
            robot_type="SPOT",
            data_set_id=existing_robots_data_set.id,
            description="test_description",
        )
        retrieved: RobotResponse | None = None
        try:
            created = toolkit_client.tool.robotics.robots.create(robot)
            assert isinstance(created, RobotResponse)
            assert created.as_write().model_dump() == robot.model_dump()

            all_retrieved = toolkit_client.tool.robotics.robots.retrieve(robot.data_set_id)
            assert isinstance(all_retrieved, list)
            retrieved = next((r for r in all_retrieved if r.name == robot.name), None)
            assert isinstance(retrieved, RobotResponse)
            assert retrieved.as_write().model_dump() == robot.model_dump()
        finally:
            if retrieved:
                toolkit_client.tool.robotics.robots.delete(retrieved.data_set_id)

        with pytest.raises(ToolkitAPIError):
            toolkit_client.tool.robotics.robots.retrieve(robot.data_set_id)

    @pytest.mark.usefixtures("existing_robot")
    def test_list_robots(self, toolkit_client: ToolkitClient) -> None:
        robots = toolkit_client.tool.robotics.robots.list()
        assert isinstance(robots, list)
        assert len(robots) > 0
        assert all(isinstance(r, RobotResponse) for r in robots)

    @pytest.mark.usefixtures("existing_robot")
    def test_iterate_robots(self, toolkit_client: ToolkitClient) -> None:
        for robot in toolkit_client.tool.robotics.robots:
            assert isinstance(robot, RobotResponse)
            break
        else:
            pytest.fail("No robots found")

    def test_update_robot(self, toolkit_client: ToolkitClient, existing_robot: RobotResponse) -> None:
        update = existing_robot.as_write()
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_robot.description)
        updated = toolkit_client.tool.robotics.robots.update(update)
        assert updated.description == update.description


class TestFrameAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient, root_frame: RobotFrameResponse) -> None:
        frame = RobotFrameRequest(
            name="test_create_retrieve_delete",
            external_id="test_create_retrieve_delete",
            transform=Transform(
                parent_frame_external_id=root_frame.external_id,
                translation=Point3D(x=0, y=0, z=0),
                orientation=Quaternion(x=0, y=0, z=0, w=1),
            ),
        )
        try:
            created = toolkit_client.tool.robotics.frames.create(frame)
            assert isinstance(created, RobotFrameResponse)
            assert created.as_write().model_dump() == frame.model_dump()

            retrieved = toolkit_client.tool.robotics.frames.retrieve(frame.external_id)

            assert isinstance(retrieved, RobotFrameResponse)
            assert retrieved.as_write().model_dump() == frame.model_dump()
        finally:
            toolkit_client.tool.robotics.frames.delete(frame.external_id)

        with pytest.raises(ToolkitAPIError):
            toolkit_client.tool.robotics.frames.retrieve(frame.external_id)

    @pytest.mark.usefixtures("existing_frame")
    def test_list_frames(self, toolkit_client: ToolkitClient) -> None:
        frames = toolkit_client.tool.robotics.frames.list()
        assert isinstance(frames, list)
        assert len(frames) > 0
        assert all(isinstance(f, RobotFrameResponse) for f in frames)

    @pytest.mark.usefixtures("existing_frame")
    def test_iterate_frames(self, toolkit_client: ToolkitClient) -> None:
        for frame in toolkit_client.tool.robotics.frames:
            assert isinstance(frame, RobotFrameResponse)
            break
        else:
            pytest.fail("No frames found")

    def test_update_frame(self, toolkit_client: ToolkitClient, existing_frame: RobotFrameResponse) -> None:
        update = existing_frame.as_write()
        update.name = next(name for name in FRAME_NAMES if name != existing_frame.name)
        updated = toolkit_client.tool.robotics.frames.update(update)
        assert updated.name == update.name
