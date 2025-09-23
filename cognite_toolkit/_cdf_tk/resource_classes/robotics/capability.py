from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource


class RobotCapabilityYAML(ToolkitResource):
    name: str = Field(description="Robot capability name.")
    external_id: str = Field(description="RobotCapability external id.", max_length=256)
    method: str = Field(description="RobotCapability method, used to call the right functionality on the robot.")
    input_schema: dict[str, JsonValue] | None = Field(
        default=None, description="Schema that defines what inputs are needed for configuring the action."
    )
    data_handling_schema: dict[str, JsonValue] | None = Field(
        default=None, description="Schema that defines how the data from a RobotCapability should be handled."
    )
    description: str | None = Field(
        default=None,
        description="Textual description of the RobotCapability.",
    )
