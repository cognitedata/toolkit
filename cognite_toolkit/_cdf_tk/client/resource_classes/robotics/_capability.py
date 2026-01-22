from typing import ClassVar

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId


class RobotCapability(BaseModelObject):
    """Robot capabilities define what actions that robots can execute, including data capture (PTZ, PTZ-IR, 360)
    and behaviors (e.g., docking).

    Args:
        name: RobotCapability name.
        external_id: RobotCapability external id. Must be unique for the resource type.
        method: RobotCapability method. The method is used to call the right functionality on the robot.
        input_schema: Schema that defines what inputs are needed for the action.
            The input are values that configure the action, e.g pan, tilt and zoom values.
        data_handling_schema: Schema that defines how the data from a RobotCapability should be handled,
            including upload instructions.
        description: Description of RobotCapability. Textual description of the RobotCapability.
    """

    external_id: str
    name: str
    method: str
    description: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class RobotCapabilityRequest(RobotCapability, UpdatableRequestResource):
    """Request resource for creating or updating a RobotCapability."""

    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"input_schema", "data_handling_schema"})
    input_schema: dict[str, JsonValue] | None = None
    data_handling_schema: dict[str, JsonValue] | None = None


class RobotCapabilityResponse(RobotCapability, ResponseResource[RobotCapabilityRequest]):
    """Response resource for a RobotCapability."""

    # The response always has input_schema and data_handling_schema
    input_schema: dict[str, JsonValue]
    data_handling_schema: dict[str, JsonValue]

    def as_request_resource(self) -> RobotCapabilityRequest:
        return RobotCapabilityRequest.model_validate(self.dump(), extra="ignore")
