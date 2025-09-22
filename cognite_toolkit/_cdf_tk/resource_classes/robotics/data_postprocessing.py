from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource


class RobotDataPostProcessingYAML(ToolkitResource):
    name: str = Field(description="DataProcessing name.")
    external_id: str = Field(
        description="DataProcessing external id. Must be unique for the resource type.",
    )
    method: str = Field(description="DataProcessing method, to call the right functionality on the robot.")
    input_schema: dict[str, JsonValue] | None = Field(
        default=None, description="Schema that defines what inputs are needed for the data postprocessing."
    )
    description: str | None = Field(default=None, description="Textual description of the DataProcessing.")
