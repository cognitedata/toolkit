from pydantic import Field

from cognite_toolkit._cdf_tk.resource_classes.base import BaseModelResource, ToolkitResource


class Translation(BaseModelResource):
    x: float = Field(description="X coordinate")
    y: float = Field(description="Y coordinate")
    z: float = Field(description="Z coordinate")


class Orientation(Translation):
    w: float = Field(description="W coordinate")


class Transform(BaseModelResource):
    parent_frame_external_id: str = Field(description="Parent frame external id.")
    translation: Translation = Field(description="Translation")
    orientation: Orientation = Field(description="Orientation")


class RobotFrameYAML(ToolkitResource):
    name: str = Field(description="Robot frame name.")
    external_id: str = Field(description="RobotFrame external id.", max_length=256)
    transform: Transform | None = Field(default=None, description="Transform")
