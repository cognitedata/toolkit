import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.robotics import Robot, RobotCapability, RobotWrite


class TestRobotCapabilityAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        capability = RobotCapability(
            name="test_capability",
            external_id="test_capability",
            method="test_method",
            input_schema={"test_key": "test_value"},
            data_handling_schema={"test_key": "test_value"},
            description="test_description",
        )
        try:
            created = toolkit_client.robotics.capabilities.create(capability)
            assert isinstance(created, RobotCapability)
            assert created.dump() == capability.dump()

            retrieved = toolkit_client.robotics.capabilities.retrieve(created.external_id)

            assert isinstance(retrieved, RobotCapability)
            assert retrieved.dump() == created.dump()
        finally:
            toolkit_client.robotics.capabilities.delete(capability.name)

        retrieved = toolkit_client.robotics.capabilities.retrieve(capability.name)
        assert retrieved is None


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
