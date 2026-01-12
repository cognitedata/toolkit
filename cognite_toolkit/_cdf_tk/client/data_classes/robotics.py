from typing import Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId

RobotType = Literal["SPOT", "ANYMAL", "DJI_DRONE", "TAUROB", "UNKNOWN"]
MapType = Literal["WAYPOINTMAP", "THREEDMODEL", "TWODMAP", "POINTCLOUD"]


# ==================== Frame ====================


class Point3D(BaseModelObject):
    """A point in 3D space."""

    x: float
    y: float
    z: float


class Quaternion(BaseModelObject):
    """A quaternion representing orientation."""

    x: float
    y: float
    z: float
    w: float


class Transform(BaseModelObject):
    """Transform of the parent frame to the current frame.

    Args:
        parent_frame_external_id: The external id of the parent frame.
        translation: Transform translation (Point3D).
        orientation: Transform orientation as quaternion (Quaternion).
    """

    parent_frame_external_id: str
    translation: Point3D
    orientation: Quaternion


class Frame(BaseModelObject):
    """The frames resource represents coordinate frames, which are used to describe how maps are aligned with
    respect to each other. For example, frames are used to describe the relative position of a context map
    (e.g., a 3D model of a location) and a robot's navigation map. Frames are aligned with each other through
    transforms, which consist of a translation (in meters) and rotation (quaternion).

    Args:
        name: Frame name.
        external_id: Frame external id. Must be unique for the resource type.
        transform: Transform of the parent frame to the current frame.
    """

    external_id: str
    name: str
    transform: Transform | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class FrameRequest(Frame, RequestResource):
    """Request resource for creating or updating a Frame."""

    pass


class FrameResponse(Frame, ResponseResource[FrameRequest]):
    """Response resource for a Frame."""

    created_time: int
    updated_time: int

    def as_request_resource(self) -> FrameRequest:
        return FrameRequest.model_validate(self.dump(), extra="ignore")


# ==================== RobotCapability ====================


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
    input_schema: JsonValue | None = None
    data_handling_schema: JsonValue | None = None
    description: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class RobotCapabilityRequest(RobotCapability, RequestResource):
    """Request resource for creating or updating a RobotCapability."""

    pass


class RobotCapabilityResponse(RobotCapability, ResponseResource[RobotCapabilityRequest]):
    """Response resource for a RobotCapability."""

    # The response always has input_schema and data_handling_schema
    input_schema: JsonValue
    data_handling_schema: JsonValue

    def as_request_resource(self) -> RobotCapabilityRequest:
        return RobotCapabilityRequest.model_validate(self.dump(), extra="ignore")


# ==================== Location ====================


class Location(BaseModelObject):
    """The Locations resource is used to specify the physical location of a robot. Robot missions are defined
    for a specific location. In addition, the location is used to group Missions and Map resources.

    Args:
        name: Location name.
        external_id: Location external id. Must be unique for the resource type.
        description: Description of Location. Textual description of the Location.
    """

    external_id: str
    name: str
    description: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class LocationRequest(Location, RequestResource):
    """Request resource for creating or updating a Location."""

    pass


class LocationResponse(Location, ResponseResource[LocationRequest]):
    """Response resource for a Location."""

    created_time: int
    updated_time: int

    def as_request_resource(self) -> LocationRequest:
        return LocationRequest.model_validate(self.dump(), extra="ignore")


# ==================== Robot ====================


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

    def as_id(self) -> ExternalId:
        # Robot does not have external_id in the legacy definition, using name as identifier
        return ExternalId(external_id=self.name)


class RobotRequest(Robot, RequestResource):
    """Request resource for creating or updating a Robot."""

    pass


class RobotResponse(Robot, ResponseResource[RobotRequest]):
    """Response resource for a Robot."""

    created_time: int
    updated_time: int

    def as_request_resource(self) -> RobotRequest:
        return RobotRequest.model_validate(self.dump(), extra="ignore")


# ==================== DataPostProcessing ====================


class DataPostProcessing(BaseModelObject):
    """DataPostprocessing define types of data processing on data captured by the robot.
    DataPostprocessing enables you to automatically process data captured by the robot.

    Args:
        name: DataPostProcessing name.
        external_id: DataPostProcessing external id. Must be unique for the resource type.
        method: DataPostProcessing method. The method is used to call the right functionality on the robot.
        input_schema: Schema that defines what inputs are needed for the data postprocessing.
            The input are values that configure the data postprocessing, e.g max and min values for a gauge.
        description: Description of DataPostProcessing. Textual description of the DataPostProcessing.
    """

    external_id: str
    name: str
    method: str
    input_schema: JsonValue | None = None
    description: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class DataPostProcessingRequest(DataPostProcessing, RequestResource):
    """Request resource for creating or updating a DataPostProcessing."""

    pass


class DataPostProcessingResponse(DataPostProcessing, ResponseResource[DataPostProcessingRequest]):
    """Response resource for a DataPostProcessing."""

    # The response always has input_schema
    input_schema: JsonValue

    def as_request_resource(self) -> DataPostProcessingRequest:
        return DataPostProcessingRequest.model_validate(self.dump(), extra="ignore")


# ==================== Map ====================


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


class RobotMapRequest(RobotMap, RequestResource):
    """Request resource for creating or updating a RobotMap."""

    pass


class RobotMapResponse(RobotMap, ResponseResource[RobotMapRequest]):
    """Response resource for a RobotMap."""

    created_time: int
    updated_time: int

    def as_request_resource(self) -> RobotMapRequest:
        return RobotMapRequest.model_validate(self.dump(), extra="ignore")
