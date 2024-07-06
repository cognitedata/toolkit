import contextlib

import pytest
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
    Robot,
    RobotCapability,
    RobotCapabilityList,
    RobotCapabilityWrite,
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


class TestRobotCapabilityAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        capability = RobotCapabilityWrite(
            name="ptz",
            external_id="ptz",
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

    def test_list_capabilities(self, toolkit_client: ToolkitClient) -> None:
        capabilities = toolkit_client.robotics.capabilities.list()
        assert isinstance(capabilities, RobotCapabilityList)
        assert len(capabilities) > 0


class TestRobotsAPI:
    @pytest.mark.skip("Skip until Robotics API is available")
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        robot = RobotWrite(
            name="test_robot",
            capabilities=["test_capability"],
            robot_type="SPOT",
            data_set_id=1,
            description="test_description",
            metadata={"test_key": "test_value"},
        )
        try:
            created = toolkit_client.robotics.robots.create(robot)
            assert isinstance(created, Robot)
            assert created.as_write() == robot

            retrieved = toolkit_client.robotics.robots.retrieve(created.name)

            assert isinstance(retrieved, Robot)
            assert retrieved.dump() == created.dump()
        finally:
            toolkit_client.robotics.robots.delete(robot.name)

        retrieved = toolkit_client.robotics.robots.retrieve(robot.name)
        assert retrieved is None
