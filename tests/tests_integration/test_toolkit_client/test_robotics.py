import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.robotics import Robot, RobotWrite


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
