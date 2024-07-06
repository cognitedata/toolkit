from __future__ import annotations

from typing import Literal

from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from typing_extensions import TypeAlias

RobotType: TypeAlias = Literal["SPOT", "ANYMAL", "DJI_DRONE", "TAUROB", "UWNKNOWN"]


class RobotCore(WriteableCogniteResource["RobotWrite"]):
    """
    Robot contains information for a single robot, including capabilities, type, video streaming setup, etc.
    These fields are updated every time a robot registers with Robotics Services and do not require manual changes.

    Args:
        name: Robot name
        capabilities: List of external ids for the capabilities the robot can perform.
        robot_type: Type of robot.
        data_set_id: The id of the data set this asset belongs to.
        description: A brief description of the robot.
        metadata: Custom, application specific metadata. String key -> String value.
        location_external_id: External id of the location.

    """

    def __init__(
        self,
        name: str,
        capabilities: list[str],
        robot_type: RobotType,
        data_set_id: int,
        description: str | None = None,
        metadata: dict | None = None,
        location_external_id: str | None = None,
    ) -> None:
        self.name = name
        self.capabilities = capabilities
        self.robot_type = robot_type
        self.data_set_id = data_set_id
        self.description = description
        self.metadata = metadata
        self.location_external_id = location_external_id

    def as_write(self) -> RobotWrite:
        return RobotWrite(
            name=self.name,
            capabilities=self.capabilities,
            robot_type=self.robot_type,
            data_set_id=self.data_set_id,
            description=self.description,
            metadata=self.metadata,
            location_external_id=self.location_external_id,
        )


class RobotWrite(RobotCore): ...


class Robot(RobotCore):
    """
    Robot contains information for a single robot, including capabilities, type, video streaming setup, etc.
    These fields are updated every time a robot registers with Robotics Services and do not require manual changes.

    Args:
        name: Robot name
        capabilities: List of external ids for the capabilities the robot can perform.
        robot_type: Type of robot.
        data_set_id: The id of the data set this asset belongs to.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description: A brief description of the robot.
        metadata: Custom, application-specific metadata. String key -> String value.
        location_external_id: External id of the location.

    """

    def __init__(
        self,
        name: str,
        capabilities: list[str],
        robot_type: RobotType,
        data_set_id: int,
        created_time: int,
        updated_time: int,
        description: str | None = None,
        metadata: dict | None = None,
        location_external_id: str | None = None,
    ) -> None:
        super().__init__(name, capabilities, robot_type, data_set_id, description, metadata, location_external_id)
        self.created_time = created_time
        self.updated_time = updated_time


class RobotWriteList(CogniteResourceList):
    _RESOURCE = RobotWrite


class RobotList(WriteableCogniteResourceList[RobotWrite, Robot]):
    _RESOURCE = Robot

    def as_write(self) -> RobotWriteList:
        return RobotWriteList([robot.as_write() for robot in self])
