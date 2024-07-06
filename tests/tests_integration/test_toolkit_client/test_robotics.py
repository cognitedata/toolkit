import contextlib

import pytest
from cognite.client.data_classes import DataSet, DataSetWrite
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
    Robot,
    RobotCapability,
    RobotCapabilityList,
    RobotCapabilityWrite,
    RobotList,
    RobotWrite,
)

DATA_HANDLING_SCHEMA = {
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

INPUT_SCHEMA = {
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

DESCRIPTIONS = ["Original Description", "Updated Description"]


@pytest.fixture(scope="session")
def existing_capability(toolkit_client: ToolkitClient) -> RobotCapability:
    capability = RobotCapabilityWrite(
        name="ptz",
        external_id="ptz",
        method="ptz",
        input_schema=INPUT_SCHEMA,
        data_handling_schema=DATA_HANDLING_SCHEMA,
        description=DESCRIPTIONS[0],
    )
    try:
        retrieved = toolkit_client.robotics.capabilities.retrieve(capability.external_id)
        return retrieved
    except CogniteAPIError:
        created = toolkit_client.robotics.capabilities.create(capability)
        return created


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
            external_id="test_create_retrieve_delete",
            method="ptz",
            input_schema=INPUT_SCHEMA,
            data_handling_schema=DATA_HANDLING_SCHEMA,
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
