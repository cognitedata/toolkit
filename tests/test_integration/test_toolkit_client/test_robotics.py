import contextlib

import pytest
from cognite.client.data_classes import DataSet, DataSetWrite
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
    DataPostProcessing,
    DataPostProcessingList,
    DataPostProcessingWrite,
    Frame,
    FrameList,
    FrameWrite,
    Location,
    LocationList,
    LocationWrite,
    Map,
    MapList,
    MapWrite,
    Point3D,
    Quaternion,
    Robot,
    RobotCapability,
    RobotCapabilityList,
    RobotCapabilityWrite,
    RobotList,
    RobotWrite,
    Transform,
)
from tests.test_integration.constants import RUN_UNIQUE_ID

DESCRIPTIONS = ["Original Description", "Updated Description"]


@pytest.fixture(scope="session")
def existing_capability(toolkit_client: ToolkitClient) -> RobotCapability:
    capability = RobotCapabilityWrite(
        name="ptz",
        external_id=f"ptz_{RUN_UNIQUE_ID}",
        method="ptz",
        input_schema=INPUT_SCHEMA_CAPABILITY,
        data_handling_schema=DATA_HANDLING_SCHEMA_CAPABILITY,
        description=DESCRIPTIONS[0],
    )
    try:
        retrieved = toolkit_client.robotics.capabilities.retrieve(capability.external_id)
        return retrieved
    except CogniteAPIError:
        created = toolkit_client.robotics.capabilities.create(capability)
        return created


@pytest.fixture(scope="session")
def existing_data_processing(toolkit_client: ToolkitClient) -> RobotCapability:
    data_processing = RobotCapabilityWrite(
        name="Read dial gauge",
        external_id=f"read_dial_gauge_{RUN_UNIQUE_ID}",
        method="read_dial_gauge",
        input_schema=INPUT_SCHEMA_CAPABILITY,
        description=DESCRIPTIONS[0],
    )
    try:
        return toolkit_client.robotics.data_postprocessing.retrieve(data_processing.external_id)
    except CogniteAPIError:
        return toolkit_client.robotics.data_postprocessing.create(data_processing)


@pytest.fixture(scope="session")
def existing_map(toolkit_client: ToolkitClient) -> Map:
    map_ = MapWrite(
        name="Robot navigation map",
        external_id=f"robotMap_{RUN_UNIQUE_ID}",
        map_type="POINTCLOUD",
        description=DESCRIPTIONS[0],
    )
    try:
        return toolkit_client.robotics.maps.retrieve(map_.external_id)
    except CogniteAPIError:
        return toolkit_client.robotics.maps.create(map_)


@pytest.fixture(scope="session")
def existing_location(toolkit_client: ToolkitClient) -> Map:
    location = LocationWrite(
        name="Water treatment plant",
        external_id=f"waterTreatmentPlant1_{RUN_UNIQUE_ID}",
        description=DESCRIPTIONS[0],
    )
    try:
        return toolkit_client.robotics.locations.retrieve(location.external_id)
    except CogniteAPIError:
        return toolkit_client.robotics.locations.create(location)


@pytest.fixture(scope="session")
def root_frame(toolkit_client: ToolkitClient) -> Map:
    root = FrameWrite(
        name="Root coordinate frame",
        external_id="rootCoordinateFrame",
    )
    try:
        return toolkit_client.robotics.frames.retrieve(root.external_id)
    except CogniteAPIError:
        return toolkit_client.robotics.frames.create(root)


FRAME_NAMES = ["Root coordinate frame of a location", "Updated name"]


@pytest.fixture(scope="session")
def existing_frame(toolkit_client: ToolkitClient, root_frame: Frame) -> Map:
    location = FrameWrite(
        name=FRAME_NAMES[0],
        external_id=f"rootCoordinateFrame_{RUN_UNIQUE_ID}",
        transform=Transform(
            parent_frame_external_id=root_frame.external_id,
            translation=Point3D(0, 0, 0),
            orientation=Quaternion(0, 0, 0, 1),
        ),
    )
    try:
        return toolkit_client.robotics.frames.retrieve(location.external_id)
    except CogniteAPIError:
        return toolkit_client.robotics.frames.create(location)


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
    toolkit_client: ToolkitClient, persistent_robots_data_set: DataSet, existing_capability: RobotCapability
) -> Robot:
    robot = RobotWrite(
        name="wall-e",
        capabilities=[existing_capability.external_id],
        robot_type="DJI_DRONE",
        data_set_id=persistent_robots_data_set.id,
        description=DESCRIPTIONS[0],
    )
    try:
        found = toolkit_client.robotics.robots.list()
        return found.get_robot_by_name(robot.name)
    except (CogniteAPIError, ValueError):
        return toolkit_client.robotics.robots.create(robot)


class TestRobotCapabilityAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        capability = RobotCapabilityWrite(
            name="test_create_retrieve_delete",
            external_id=f"test_create_retrieve_delete_{RUN_UNIQUE_ID}",
            method="ptz",
            input_schema=INPUT_SCHEMA_CAPABILITY,
            data_handling_schema=DATA_HANDLING_SCHEMA_CAPABILITY,
            description="Pan, tilt, zoom camera for visual image capture",
        )
        try:
            with contextlib.suppress(CogniteDuplicatedError):
                created = toolkit_client.robotics.capabilities.create(capability)
                assert isinstance(created, RobotCapability)
                assert created.as_write().dump() == capability.dump()

            retrieved = toolkit_client.robotics.capabilities.retrieve(capability.external_id)

            assert isinstance(retrieved, RobotCapability)
            assert retrieved.as_write().dump() == capability.dump()
        finally:
            toolkit_client.robotics.capabilities.delete(capability.external_id)

        with pytest.raises(CogniteAPIError):
            toolkit_client.robotics.capabilities.retrieve(capability.external_id)

    @pytest.mark.usefixtures("existing_capability")
    def test_list_capabilities(self, toolkit_client: ToolkitClient) -> None:
        capabilities = toolkit_client.robotics.capabilities.list()
        assert isinstance(capabilities, RobotCapabilityList)
        assert len(capabilities) > 0

    @pytest.mark.usefixtures("existing_capability")
    def test_iterate_capabilities(self, toolkit_client: ToolkitClient) -> None:
        for capability in toolkit_client.robotics.capabilities:
            assert isinstance(capability, RobotCapability)
            break
        else:
            pytest.fail("No capabilities found")

    def test_update_capability(self, toolkit_client: ToolkitClient, existing_capability: RobotCapability) -> None:
        update = existing_capability
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_capability.description)
        updated = toolkit_client.robotics.capabilities.update(update)
        assert updated.description == update.description


@pytest.mark.skip("Robot API seems to fail if you have two robots. This causes every other test run to fail.")
class TestRobotsAPI:
    def test_create_retrieve_delete(
        self, toolkit_client: ToolkitClient, existing_robots_data_set: DataSet, existing_capability: RobotCapability
    ) -> None:
        robot = RobotWrite(
            name="test_robot",
            capabilities=[],
            robot_type="SPOT",
            data_set_id=existing_robots_data_set.id,
            description="test_description",
        )
        retrieved: Robot | None = None
        try:
            with contextlib.suppress(CogniteDuplicatedError):
                created = toolkit_client.robotics.robots.create(robot)
                assert isinstance(created, Robot)
                assert created.as_write().dump() == robot.dump()

            all_retrieved = toolkit_client.robotics.robots.retrieve(robot.data_set_id)
            assert isinstance(all_retrieved, RobotList)
            retrieved = all_retrieved.get_robot_by_name(robot.name)
            assert isinstance(retrieved, Robot)
            assert retrieved.as_write().dump() == robot.dump()
        finally:
            if retrieved:
                toolkit_client.robotics.robots.delete(retrieved.data_set_id)

        with pytest.raises(CogniteAPIError):
            toolkit_client.robotics.robots.retrieve(robot.data_set_id)

    @pytest.mark.usefixtures("existing_robot")
    def test_list_robots(self, toolkit_client: ToolkitClient) -> None:
        robots = toolkit_client.robotics.robots.list()
        assert isinstance(robots, RobotList)
        assert len(robots) > 0

    @pytest.mark.usefixtures("existing_robot")
    def test_iterate_robots(self, toolkit_client: ToolkitClient) -> None:
        for robot in toolkit_client.robotics.robots:
            assert isinstance(robot, Robot)
            break
        else:
            pytest.fail("No robots found")

    def test_update_robot(self, toolkit_client: ToolkitClient, existing_robot: Robot) -> None:
        update = existing_robot.as_write()
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_robot.description)
        updated = toolkit_client.robotics.robots.update(update)
        assert updated.description == update.description


class TestDataProcessingAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        data_processing = DataPostProcessingWrite(
            name="test_create_retrieve_delete",
            external_id=f"test_create_retrieve_delete_{RUN_UNIQUE_ID}",
            method="read_dial_gauge",
            input_schema=INPUT_SCHEMA_DATA_PROCESSING,
            description="Read dial gauge from an image using Cognite Vision gauge reader",
        )
        try:
            with contextlib.suppress(CogniteDuplicatedError):
                created = toolkit_client.robotics.data_postprocessing.create(data_processing)
                assert isinstance(created, DataPostProcessing)
                assert created.as_write().dump() == data_processing.dump()

            retrieved = toolkit_client.robotics.data_postprocessing.retrieve(data_processing.external_id)

            assert isinstance(retrieved, DataPostProcessing)
            assert retrieved.as_write().dump() == data_processing.dump()
        finally:
            toolkit_client.robotics.data_postprocessing.delete(data_processing.external_id)

        with pytest.raises(CogniteAPIError):
            toolkit_client.robotics.data_postprocessing.retrieve(data_processing.external_id)

    @pytest.mark.usefixtures("existing_data_processing")
    def test_list_data_postprocessing(self, toolkit_client: ToolkitClient) -> None:
        data_postprocessing = toolkit_client.robotics.data_postprocessing.list()
        assert isinstance(data_postprocessing, DataPostProcessingList)
        assert len(data_postprocessing) > 0

    @pytest.mark.usefixtures("existing_data_processing")
    def test_iterate_data_postprocessing(self, toolkit_client: ToolkitClient) -> None:
        for data_postprocessing in toolkit_client.robotics.data_postprocessing:
            assert isinstance(data_postprocessing, DataPostProcessing)
            break
        else:
            pytest.fail("No data processing found")

    def test_update_capability(
        self, toolkit_client: ToolkitClient, existing_data_processing: DataPostProcessing
    ) -> None:
        update = existing_data_processing.as_write()
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_data_processing.description)
        updated = toolkit_client.robotics.data_postprocessing.update(update)
        assert updated.description == update.description


class TestMapAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        map_ = MapWrite(
            name="test_create_retrieve_delete",
            external_id=f"test_create_retrieve_delete_{RUN_UNIQUE_ID}",
            map_type="TWODMAP",
            description="2D map of the robotics lab",
            scale=1.0,
        )
        try:
            with contextlib.suppress(CogniteDuplicatedError):
                created = toolkit_client.robotics.maps.create(map_)
                assert isinstance(created, Map)
                assert created.as_write().dump() == map_.dump()

            retrieved = toolkit_client.robotics.maps.retrieve(map_.external_id)

            assert isinstance(retrieved, Map)
            assert retrieved.as_write().dump() == map_.dump()
        finally:
            toolkit_client.robotics.maps.delete(map_.external_id)

        with pytest.raises(CogniteAPIError):
            toolkit_client.robotics.maps.retrieve(map_.external_id)

    @pytest.mark.usefixtures("existing_map")
    def test_list_maps(self, toolkit_client: ToolkitClient) -> None:
        maps = toolkit_client.robotics.maps.list()
        assert isinstance(maps, MapList)
        assert len(maps) > 0

    @pytest.mark.usefixtures("existing_map")
    def test_iterate_maps(self, toolkit_client: ToolkitClient) -> None:
        for map_ in toolkit_client.robotics.maps:
            assert isinstance(map_, Map)
            break
        else:
            pytest.fail("No maps found")

    def test_update_map(self, toolkit_client: ToolkitClient, existing_map: Map) -> None:
        update = existing_map.as_write()
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_map.description)
        updated = toolkit_client.robotics.maps.update(update)
        assert updated.description == update.description


class TestLocationAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        location = LocationWrite(
            name="test_create_retrieve_delete",
            external_id=f"test_create_retrieve_delete_{RUN_UNIQUE_ID}",
        )
        try:
            with contextlib.suppress(CogniteDuplicatedError):
                created = toolkit_client.robotics.locations.create(location)
                assert isinstance(created, Location)
                assert created.as_write().dump() == location.dump()

            retrieved = toolkit_client.robotics.locations.retrieve(location.external_id)

            assert isinstance(retrieved, Location)
            assert retrieved.as_write().dump() == location.dump()
        finally:
            toolkit_client.robotics.locations.delete(location.external_id)

        with pytest.raises(CogniteAPIError):
            toolkit_client.robotics.locations.retrieve(location.external_id)

    @pytest.mark.usefixtures("existing_location")
    def test_list_locations(self, toolkit_client: ToolkitClient) -> None:
        locations = toolkit_client.robotics.locations.list()
        assert isinstance(locations, LocationList)
        assert len(locations) > 0

    @pytest.mark.usefixtures("existing_location")
    def test_iterate_locations(self, toolkit_client: ToolkitClient) -> None:
        for location in toolkit_client.robotics.locations:
            assert isinstance(location, Location)
            break
        else:
            pytest.fail("No locations found")

    def test_update_location(self, toolkit_client: ToolkitClient, existing_location: Location) -> None:
        update = existing_location.as_write()
        update.description = next(desc for desc in DESCRIPTIONS if desc != existing_location.description)
        updated = toolkit_client.robotics.locations.update(update)
        assert updated.description == update.description


class TestFrameAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient, root_frame: Frame) -> None:
        frame = FrameWrite(
            name="test_create_retrieve_delete",
            external_id=f"test_create_retrieve_delete_{RUN_UNIQUE_ID}",
            transform=Transform(
                parent_frame_external_id=root_frame.external_id,
                translation=Point3D(0, 0, 0),
                orientation=Quaternion(0, 0, 0, 1),
            ),
        )
        try:
            with contextlib.suppress(CogniteDuplicatedError):
                created = toolkit_client.robotics.frames.create(frame)
                assert isinstance(created, Frame)
                assert created.as_write().dump() == frame.dump()

            retrieved = toolkit_client.robotics.frames.retrieve(frame.external_id)

            assert isinstance(retrieved, Frame)
            assert retrieved.as_write().dump() == frame.dump()
        finally:
            toolkit_client.robotics.frames.delete(frame.external_id)

        with pytest.raises(CogniteAPIError):
            toolkit_client.robotics.frames.retrieve(frame.external_id)

    @pytest.mark.usefixtures("existing_frame")
    def test_list_frames(self, toolkit_client: ToolkitClient) -> None:
        frames = toolkit_client.robotics.frames.list()
        assert isinstance(frames, FrameList)
        assert len(frames) > 0

    @pytest.mark.usefixtures("existing_frame")
    def test_iterate_frames(self, toolkit_client: ToolkitClient) -> None:
        for frame in toolkit_client.robotics.frames:
            assert isinstance(frame, Frame)
            break
        else:
            pytest.fail("No frames found")

    def test_update_frame(self, toolkit_client: ToolkitClient, existing_frame: Frame) -> None:
        update = existing_frame.as_write()
        update.name = next(name for name in FRAME_NAMES if name != existing_frame.name)
        updated = toolkit_client.robotics.frames.update(update)
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
