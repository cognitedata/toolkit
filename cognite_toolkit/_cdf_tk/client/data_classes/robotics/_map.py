from typing import ClassVar

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestUpdateable,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import ExternalId

from ._common import MapType


class RobotMap(BaseModelObject):
    """The map resource allows defining both context maps and robot maps of a specific location. A context map is a
    visual representation of a location, for example, a 3D model, a 2D floor plan, or a point cloud model.
    A robot map is a representation of where a robot is able to navigate. Maps need to be aligned with respect
    to each other using coordinate frames.

    Args:
        name: Map name.
        external_id: Map external id. Must be unique for the resource type.
        map_type: Map type.
        description: Description of Map. Textual description of the Map.
        frame_external_id: External id of the map's reference frame.
        data: Map-specific data.
        location_external_id: External id of the location.
        scale: Uniform scaling factor, for example, for map unit conversion (centimeter to meter).
    """

    external_id: str
    name: str
    map_type: MapType
    description: str | None = None
    frame_external_id: str | None = None
    data: JsonValue | None = None
    location_external_id: str | None = None
    scale: float | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class RobotMapRequest(RobotMap, RequestUpdateable):
    """Request resource for creating or updating a RobotMap."""

    non_nullable_fields: ClassVar[frozenset[str]] = frozenset(
        {"description", "data", "frame_external_id", "location_external_id", "scale"}
    )


class RobotMapResponse(RobotMap, ResponseResource[RobotMapRequest]):
    """Response resource for a RobotMap."""

    created_time: int
    updated_time: int

    def as_request_resource(self) -> RobotMapRequest:
        return RobotMapRequest.model_validate(self.dump(), extra="ignore")
