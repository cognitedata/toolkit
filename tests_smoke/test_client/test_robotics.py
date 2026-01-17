import contextlib

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
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

DESCRIPTIONS = ["Original Description", "Updated Description"]


@pytest.fixture(scope="session")
def existing_capability(toolkit_client: ToolkitClient) -> RobotCapabilityResponse:
    capability = RobotCapabilityRequest(
        name="ptz",
        external_id="ptz",
        method="ptz",
        input_schema=INPUT_SCHEMA_CAPABILITY,
        data_handling_schema=DATA_HANDLING_SCHEMA_CAPABILITY,
        description=DESCRIPTIONS[0],
    )
    try:
        retrieved = toolkit_client.tool.robotics.capabilities.retrieve(capability.external_id)
        return retrieved
    except ToolkitAPIError:
        created = toolkit_client.tool.robotics.capabilities.create(capability)
        return created


@pytest.fixture(scope="session")
def existing_data_processing(toolkit_client: ToolkitClient) -> RobotDataPostProcessingResponse:
    data_processing = RobotDataPostProcessingRequest(
        name="Read dial gauge",
        external_id="read_dial_gauge",
        method="read_dial_gauge",
        input_schema=INPUT_SCHEMA_CAPABILITY,
        description=DESCRIPTIONS[0],
    )
    try:
        return toolkit_client.tool.robotics.data_postprocessing.retrieve(data_processing.external_id)
    except ToolkitAPIError:
        return toolkit_client.tool.robotics.data_postprocessing.create(data_processing)


@pytest.fixture(scope="session")
def existing_map(toolkit_client: ToolkitClient) -> RobotMapResponse:
    map_ = RobotMapRequest(
        name="Robot navigation map",
        external_id="robotMap",
        map_type="POINTCLOUD",
        description=DESCRIPTIONS[0],
    )
    try:
        return toolkit_client.tool.robotics.maps.retrieve(map_.external_id)
    except ToolkitAPIError:
        return toolkit_client.tool.robotics.maps.create(map_)


@pytest.fixture(scope="session")
def existing_location(toolkit_client: ToolkitClient) -> RobotLocationResponse:
    location = RobotLocationRequest(
        name="Water treatment plant",
        external_id="waterTreatmentPlant1",
        description=DESCRIPTIONS[0],
    )
    try:
        return toolkit_client.tool.robotics.locations.retrieve(location.external_id)
    except ToolkitAPIError:
        return toolkit_client.tool.robotics.locations.create(location)


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
def existing_robots_data_set(toolkit_client: ToolkitClient) -> DataSet:
    data_set = DataSetWrite(
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
def persistent_robots_data_set(toolkit_client: ToolkitClient) -> DataSet:
    data_set = DataSetWrite(
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
    toolkit_client: ToolkitClient, persistent_robots_data_set: DataSet, existing_capability: RobotCapabilityResponse
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


class TestRobotCapabilityAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        capability = RobotCapabilityRequest(
            name="test_create_retrieve_delete",
            external_id="test_create_retrieve_delete",
            method="ptz",
            input_schema=INPUT_SCHEMA_CAPABILITY,
            data_handling_schema=DATA_HANDLING_SCHEMA_CAPABILITY,
            description="Pan, tilt, zoom camera for visual image capture",
        )
        try:
            with contextlib.suppress(CogniteDuplicatedError):
                created = toolkit_client.tool.robotics.capabilities.create(capability)
                assert isinstance(created, RobotCapabilityResponse)
                assert created.as_write().model_dump() == capability.model_dump()

            retrieved = toolkit_client.tool.robotics.capabilities.retrieve(capability.external_id)

            assert isinstance(retrieved, RobotCapabilityResponse)
            assert retrieved.as_write().model_dump() == capability.model_dump()
        finally:
            toolkit_client.tool.robotics.capabilities.delete(capability.external_id)

        with pytest.raises(ToolkitAPIError):
            toolkit_client.tool.robotics.capabilities.retrieve(capability.external_id)

    @pytest.mark.usefixtures("existing_capability")
    def test_list_capabilities(self, toolkit_client: ToolkitClient) -> None:
        capabilities = toolkit_client.tool.robotics.capabilities.list()
        assert isinstance(capabilities, list)
        assert len(capabilities) > 0
        assert all(isinstance(cap, RobotCapabilityResponse) for cap in capabilities)

    @pytest.mark.usefixtures("existing_capability")
    def test_iterate_capabilities(self, toolkit_client: ToolkitClient) -> None:
        for capability in toolkit_client.tool.robotics.capabilities:
            assert isinstance(capability, RobotCapabilityResponse)
            break
        else:
            pytest.fail("No capabilities found")

    def test_update_capability(
        self, toolkit_client: ToolkitClient, existing_capability: RobotCapabilityResponse
    ) -> None:
        update = existing_capability.as_write()
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_capability.description)
        updated = toolkit_client.tool.robotics.capabilities.update(update)
        assert updated.description == update.description


@pytest.mark.skip("Robot API seems to fail if you have two robots. This causes every other test run to fail.")
class TestRobotsAPI:
    def test_create_retrieve_delete(
        self,
        toolkit_client: ToolkitClient,
        existing_robots_data_set: DataSet,
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
            with contextlib.suppress(CogniteDuplicatedError):
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


class TestDataProcessingAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        data_processing = RobotDataPostProcessingRequest(
            name="test_create_retrieve_delete",
            external_id="test_create_retrieve_delete",
            method="read_dial_gauge",
            input_schema=INPUT_SCHEMA_DATA_PROCESSING,
            description="Read dial gauge from an image using Cognite Vision gauge reader",
        )
        try:
            with contextlib.suppress(CogniteDuplicatedError):
                created = toolkit_client.tool.robotics.data_postprocessing.create(data_processing)
                assert isinstance(created, RobotDataPostProcessingResponse)
                assert created.as_write().model_dump() == data_processing.model_dump()

            retrieved = toolkit_client.tool.robotics.data_postprocessing.retrieve(data_processing.external_id)

            assert isinstance(retrieved, RobotDataPostProcessingResponse)
            assert retrieved.as_write().model_dump() == data_processing.model_dump()
        finally:
            toolkit_client.tool.robotics.data_postprocessing.delete(data_processing.external_id)

        with pytest.raises(ToolkitAPIError):
            toolkit_client.tool.robotics.data_postprocessing.retrieve(data_processing.external_id)

    @pytest.mark.usefixtures("existing_data_processing")
    def test_list_data_postprocessing(self, toolkit_client: ToolkitClient) -> None:
        data_postprocessing = toolkit_client.tool.robotics.data_postprocessing.list()
        assert isinstance(data_postprocessing, list)
        assert len(data_postprocessing) > 0
        assert all(isinstance(dp, RobotDataPostProcessingResponse) for dp in data_postprocessing)

    @pytest.mark.usefixtures("existing_data_processing")
    def test_iterate_data_postprocessing(self, toolkit_client: ToolkitClient) -> None:
        for data_postprocessing in toolkit_client.tool.robotics.data_postprocessing:
            assert isinstance(data_postprocessing, RobotDataPostProcessingResponse)
            break
        else:
            pytest.fail("No data processing found")

    def test_update_capability(
        self, toolkit_client: ToolkitClient, existing_data_processing: RobotDataPostProcessingResponse
    ) -> None:
        update = existing_data_processing.as_write()
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_data_processing.description)
        updated = toolkit_client.tool.robotics.data_postprocessing.update(update)
        assert updated.description == update.description


class TestMapAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        map_ = RobotMapRequest(
            name="test_create_retrieve_delete",
            external_id="test_create_retrieve_delete",
            map_type="TWODMAP",
            description="2D map of the robotics lab",
            scale=1.0,
        )
        try:
            with contextlib.suppress(CogniteDuplicatedError):
                created = toolkit_client.tool.robotics.maps.create(map_)
                assert isinstance(created, RobotMapResponse)
                assert created.as_write().model_dump() == map_.model_dump()

            retrieved = toolkit_client.tool.robotics.maps.retrieve(map_.external_id)

            assert isinstance(retrieved, RobotMapResponse)
            assert retrieved.as_write().model_dump() == map_.model_dump()
        finally:
            toolkit_client.tool.robotics.maps.delete(map_.external_id)

        with pytest.raises(ToolkitAPIError):
            toolkit_client.tool.robotics.maps.retrieve(map_.external_id)

    @pytest.mark.usefixtures("existing_map")
    def test_list_maps(self, toolkit_client: ToolkitClient) -> None:
        maps = toolkit_client.tool.robotics.maps.list()
        assert isinstance(maps, list)
        assert len(maps) > 0
        assert all(isinstance(m, RobotMapResponse) for m in maps)

    @pytest.mark.usefixtures("existing_map")
    def test_iterate_maps(self, toolkit_client: ToolkitClient) -> None:
        for map_ in toolkit_client.tool.robotics.maps:
            assert isinstance(map_, RobotMapResponse)
            break
        else:
            pytest.fail("No maps found")

    def test_update_map(self, toolkit_client: ToolkitClient, existing_map: RobotMapResponse) -> None:
        update = existing_map.as_write()
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_map.description)
        updated = toolkit_client.tool.robotics.maps.update(update)
        assert updated.description == update.description


class TestLocationAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        location = RobotLocationRequest(
            name="test_create_retrieve_delete",
            external_id="test_create_retrieve_delete",
        )
        try:
            with contextlib.suppress(CogniteDuplicatedError):
                created = toolkit_client.tool.robotics.locations.create(location)
                assert isinstance(created, RobotLocationResponse)
                assert created.as_write().model_dump() == location.model_dump()

            retrieved = toolkit_client.tool.robotics.locations.retrieve(location.external_id)

            assert isinstance(retrieved, RobotLocationResponse)
            assert retrieved.as_write().model_dump() == location.model_dump()
        finally:
            toolkit_client.tool.robotics.locations.delete(location.external_id)

        with pytest.raises(ToolkitAPIError):
            toolkit_client.tool.robotics.locations.retrieve(location.external_id)

    @pytest.mark.usefixtures("existing_location")
    def test_list_locations(self, toolkit_client: ToolkitClient) -> None:
        locations = toolkit_client.tool.robotics.locations.list()
        assert isinstance(locations, list)
        assert len(locations) > 0
        assert all(isinstance(loc, RobotLocationResponse) for loc in locations)

    @pytest.mark.usefixtures("existing_location")
    def test_iterate_locations(self, toolkit_client: ToolkitClient) -> None:
        for location in toolkit_client.tool.robotics.locations:
            assert isinstance(location, RobotLocationResponse)
            break
        else:
            pytest.fail("No locations found")

    def test_update_location(self, toolkit_client: ToolkitClient, existing_location: RobotLocationResponse) -> None:
        update = existing_location.as_write()
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_location.description)
        updated = toolkit_client.tool.robotics.locations.update(update)
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
            with contextlib.suppress(CogniteDuplicatedError):
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
