from typing import Any, ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import DataSetId

from ._common import RobotType


class Robot(BaseModelObject):
    """Robot contains information for a single robot, including capabilities, type, video streaming setup, etc.
    These fields are updated every time a robot registers with Robotics Services and do not require manual changes.

    Args:
        name: Robot name.
        capabilities: List of external ids for the capabilities the robot can perform.
        robot_type: Type of robot.
        data_set_id: The id of the data set this asset belongs to.
        description: A brief description of the robot.
        metadata: Custom, application-specific metadata. String key -> String value.
        location_external_id: External id of the location.
    """

    name: str
    capabilities: list[str]
    robot_type: RobotType
    data_set_id: int
    description: str | None = None
    metadata: dict[str, str] | None = None
    location_external_id: str | None = None

    def as_id(self) -> DataSetId:
        return DataSetId(data_set_id=self.data_set_id)


class RobotRequest(Robot, UpdatableRequestResource):
    """Request resource for creating or updating a Robot."""

    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata"})
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"location_external_id", "description"})

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update = super().as_update(mode)
        update["update"].pop("capabilities", None)
        return update


class RobotResponse(Robot, ResponseResource[RobotRequest]):
    """Response resource for a Robot."""

    created_time: int
    updated_time: int

    def as_request_resource(self) -> RobotRequest:
        return RobotRequest.model_validate(self.dump(), extra="ignore")
