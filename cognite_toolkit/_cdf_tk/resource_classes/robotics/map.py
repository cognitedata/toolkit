from typing import Literal

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource


class RobotMapYAML(ToolkitResource):
    name: str = Field(description="Map name.")
    external_id: str = Field(description="Map external id.", max_length=256)
    map_type: Literal["WAYPOINTMAP", "THREEDMODEL", "TWODMAP", "POINTCLOUD"] = Field(description="Map type.")
    description: str | None = Field(default=None, description="Description of Map. Textual description of the Map.")
    frame_external_id: str | None = Field(default=None, description="External id of the map's reference frame.")
    data: dict[str, JsonValue] | None = Field(default=None, description="Map-specific data.")
    location_external_id: str | None = Field(default=None, description="External id of the location.")
    scale: float | None = Field(default=None, description="Uniform scaling factor.", ge=0.0, le=1.0)
