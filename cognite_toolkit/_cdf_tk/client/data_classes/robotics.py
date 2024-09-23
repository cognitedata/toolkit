from __future__ import annotations

from abc import ABC
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from typing_extensions import Self, TypeAlias

MapType: TypeAlias = Literal["WAYPOINTMAP", "THREEDMODEL", "TWODMAP", "POINTCLOUD"]


class RobotCapabilityCore(WriteableCogniteResource["RobotCapabilityWrite"], ABC):
    """Robot capabilities define what actions that robots can execute, including data capture (PTZ, PTZ-IR, 360)
    and behaviors (e.g., docking)

    Args:
        name: RobotCapability name.
        external_id: RobotCapability external id. Must be unique for the resource type.
        method: RobotCapability method. The method is used to call the right functionality on the robot.
        description: Description of RobotCapability. Textual description of the RobotCapability.

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        method: str,
        description: str | None = None,
    ) -> None:
        self.name = name
        self.external_id = external_id
        self.method = method
        self.description = description


class RobotCapabilityWrite(RobotCapabilityCore):
    """Robot capabilities define what actions that robots can execute, including data capture (PTZ, PTZ-IR, 360)
    and behaviors (e.g., docking)

    Args:
        name: RobotCapability name.
        external_id: RobotCapability external id. Must be unique for the resource type.
        method: RobotCapability method. The method is used to call the right functionality on the robot.
        input_schema: Schema that defines what inputs are needed for the action. The input are values that
        configure the action, e.g pan, tilt and zoom values.
        data_handling_schema: Schema that defines how the data from a RobotCapability should be handled,
            including upload instructions.
        description: Description of RobotCapability. Textual description of the RobotCapability.

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        method: str,
        input_schema: dict | None = None,
        data_handling_schema: dict | None = None,
        description: str | None = None,
    ) -> None:
        super().__init__(name, external_id, method, description)
        self.input_schema = input_schema
        self.data_handling_schema = data_handling_schema

    def as_write(self) -> RobotCapabilityWrite:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource["externalId"],
            method=resource["method"],
            input_schema=resource.get("inputSchema"),
            data_handling_schema=resource.get("dataHandlingSchema"),
            description=resource.get("description"),
        )


class RobotCapability(RobotCapabilityCore):
    """Robot capabilities define what actions that robots can execute, including data capture (PTZ, PTZ-IR, 360)
    and behaviors (e.g., docking)

    Args:
        name: RobotCapability name.
        external_id: RobotCapability external id. Must be unique for the resource type.
        method: RobotCapability method. The method is used to call the right functionality on the robot.
        input_schema: Schema that defines what inputs are needed for the action. The input are values that
        configure the action, e.g pan, tilt and zoom values.
        data_handling_schema: Schema that defines how the data from a RobotCapability should be handled,
            including upload instructions.
        description: Description of RobotCapability. Textual description of the RobotCapability.

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        method: str,
        input_schema: dict,
        data_handling_schema: dict,
        description: str | None = None,
    ) -> None:
        super().__init__(name, external_id, method, description)
        self.input_schema = input_schema
        self.data_handling_schema = data_handling_schema

    def as_write(self) -> RobotCapabilityWrite:
        return RobotCapabilityWrite(
            name=self.name,
            external_id=self.external_id,
            method=self.method,
            input_schema=self.input_schema,
            data_handling_schema=self.data_handling_schema,
            description=self.description,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource["externalId"],
            method=resource["method"],
            input_schema=resource["inputSchema"],
            data_handling_schema=resource["dataHandlingSchema"],
            description=resource.get("description"),
        )


class RobotCapabilityWriteList(CogniteResourceList, ExternalIDTransformerMixin):
    _RESOURCE = RobotCapabilityWrite


class RobotCapabilityList(
    WriteableCogniteResourceList[RobotCapabilityWrite, RobotCapability], ExternalIDTransformerMixin
):
    _RESOURCE = RobotCapability

    def as_write(self) -> RobotCapabilityWriteList:
        return RobotCapabilityWriteList([capability.as_write() for capability in self])


class _RobotCapabilityUpdate(CogniteUpdate):
    """This is not fully implemented as the Toolkit only needs it for the
    _get_update_properties in the .update method of the RobotCapability class.

    All updates are done through the RobotCapabilityWrite class instead.
    """

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name"),
            PropertySpec("description", is_nullable=False),
            PropertySpec("method", is_nullable=False),
            PropertySpec("input_schema", is_nullable=False),
            PropertySpec("data_handling_schema", is_nullable=False),
        ]


RobotType: TypeAlias = Literal["SPOT", "ANYMAL", "DJI_DRONE", "TAUROB", "UWNKNOWN"]


class RobotCore(WriteableCogniteResource["RobotWrite"], ABC):
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


class RobotWrite(RobotCore):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            capabilities=resource["capabilities"],
            robot_type=resource["robotType"],
            data_set_id=resource["dataSetId"],
            description=resource.get("description"),
            metadata=resource.get("metadata"),
            location_external_id=resource.get("locationExternalId"),
        )


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

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            capabilities=resource["capabilities"],
            robot_type=resource["robotType"],
            data_set_id=resource["dataSetId"],
            created_time=resource["createdTime"],
            updated_time=resource["updatedTime"],
            description=resource.get("description"),
            metadata=resource.get("metadata"),
            location_external_id=resource.get("locationExternalId"),
        )


class RobotWriteList(CogniteResourceList):
    _RESOURCE = RobotWrite


class RobotList(WriteableCogniteResourceList[RobotWrite, Robot]):
    _RESOURCE = Robot

    def as_write(self) -> RobotWriteList:
        return RobotWriteList([robot.as_write() for robot in self])

    def get_robot_by_name(self, name: str) -> Robot:
        try:
            return next(robot for robot in self if robot.name == name)
        except StopIteration:
            raise ValueError(f"No robot with name {name} found in list")


class _RobotUpdate(CogniteUpdate):
    """This is not fully implemented as the Toolkit only needs it for the
    _get_update_properties in the .update method of the Robot class.

    All updates are done through the RobotWrite
    """

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=False),
            PropertySpec("metadata", is_object=True, is_nullable=False),
            PropertySpec("robot_type", is_nullable=False),
            PropertySpec("location_external_id", is_nullable=False),
        ]


class DataPostProcessingCore(WriteableCogniteResource["DataPostProcessingWrite"], ABC):
    """Robot capabilities define what actions that robots can execute, including data capture (PTZ, PTZ-IR, 360)
    and behaviors (e.g., docking)

    Args:
        name: DataProcessing name.
        external_id: DataProcessing external id. Must be unique for the resource type.
        method: DataProcessing method. The method is used to call the right functionality on the robot.
        description: Description of DataProcessing. Textual description of the DataProcessing.

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        method: str,
        description: str | None = None,
    ) -> None:
        self.name = name
        self.external_id = external_id
        self.method = method
        self.description = description


class DataPostProcessingWrite(DataPostProcessingCore):
    """DataPostprocessing define types of data processing on data captured by the robot.
    DataPostprocessing enables you to automatically process data captured by the robot.

    Args:
        name: DataProcessing name.
        external_id: DataProcessing external id. Must be unique for the resource type.
        method: DataProcessing method. The method is used to call the right functionality on the robot.
        input_schema: Schema that defines what inputs are needed for the data postprocessing. The input are values
            that configure the data postprocessing, e.g max and min values for a gauge.
        configure the action, e.g pan, tilt and zoom values.
        description: Description of DataProcessing. Textual description of the DataProcessing.

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        method: str,
        input_schema: dict | None = None,
        description: str | None = None,
    ) -> None:
        super().__init__(name, external_id, method, description)
        self.input_schema = input_schema

    def as_write(self) -> DataPostProcessingWrite:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource["externalId"],
            method=resource["method"],
            input_schema=resource.get("inputSchema"),
            description=resource.get("description"),
        )


class DataPostProcessing(DataPostProcessingCore):
    """DataPostprocessing define types of data processing on data captured by the robot.
    DataPostprocessing enables you to automatically process data captured by the robot.

    Args:
        name: DataProcessing name.
        external_id: DataProcessing external id. Must be unique for the resource type.
        method: DataProcessing method. The method is used to call the right functionality on the robot.
        input_schema: Schema that defines what inputs are needed for the data postprocessing. The input are values
            that configure the data postprocessing, e.g max and min values for a gauge.
        configure the action, e.g pan, tilt and zoom values.
        description: Description of DataProcessing. Textual description of the DataProcessing.

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        method: str,
        input_schema: dict,
        description: str | None = None,
    ) -> None:
        super().__init__(name, external_id, method, description)
        self.input_schema = input_schema

    def as_write(self) -> DataPostProcessingWrite:
        return DataPostProcessingWrite(
            name=self.name,
            external_id=self.external_id,
            method=self.method,
            input_schema=self.input_schema,
            description=self.description,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource["externalId"],
            method=resource["method"],
            input_schema=resource["inputSchema"],
            description=resource.get("description"),
        )


class DataPostProcessingWriteList(CogniteResourceList, ExternalIDTransformerMixin):
    _RESOURCE = DataPostProcessingWrite


class DataPostProcessingList(
    WriteableCogniteResourceList[DataPostProcessingWrite, DataPostProcessing], ExternalIDTransformerMixin
):
    _RESOURCE = DataPostProcessing

    def as_write(self) -> DataPostProcessingWriteList:
        return DataPostProcessingWriteList([capability.as_write() for capability in self])


class _DataProcessingUpdate(CogniteUpdate):
    """This is not fully implemented as the Toolkit only needs it for the
    _get_update_properties in the .update method of the DataProcessing class.

    All updates are done through the DataProcessingWrite class instead.
    """

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=False),
            PropertySpec("method", is_nullable=False),
            PropertySpec("input_schema", is_nullable=False),
        ]


class LocationCore(WriteableCogniteResource["LocationWrite"], ABC):
    """Robot capabilities define what actions that robots can execute, including data capture (PTZ, PTZ-IR, 360)
    and behaviors (e.g., docking)

    Args:
        name: Location name.
        external_id: Location external id. Must be unique for the resource type.
        description: Description of Location. Textual description of the Location.

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        description: str | None = None,
    ) -> None:
        self.name = name
        self.external_id = external_id
        self.description = description


class LocationWrite(LocationCore):
    """The Locations resource is used to specify the physical location of a robot. Robot missions are defined
    for a specific location. In addition, the location is used to group Missions and Map resources.

    Args:
        name: Location name.
        external_id: Location external id. Must be unique for the resource type.
        description: Description of Location. Textual description of the Location.

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        description: str | None = None,
    ) -> None:
        super().__init__(name, external_id, description)

    def as_write(self) -> LocationWrite:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource["externalId"],
            description=resource.get("description"),
        )


class Location(LocationCore):
    """DataPostprocessing define types of data processing on data captured by the robot.
    DataPostprocessing enables you to automatically process data captured by the robot.

    Args:
        name: Location name.
        external_id: Location external id. Must be unique for the resource type.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description: Description of Location. Textual description of the Location.

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        created_time: int,
        updated_time: int,
        description: str | None = None,
    ) -> None:
        super().__init__(name, external_id, description)
        self.created_time = created_time
        self.updated_time = updated_time

    def as_write(self) -> LocationWrite:
        return LocationWrite(
            name=self.name,
            external_id=self.external_id,
            description=self.description,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            updated_time=resource["updatedTime"],
            description=resource.get("description"),
        )


class LocationWriteList(CogniteResourceList, ExternalIDTransformerMixin):
    _RESOURCE = LocationWrite


class LocationList(WriteableCogniteResourceList[LocationWrite, Location], ExternalIDTransformerMixin):
    _RESOURCE = Location

    def as_write(self) -> LocationWriteList:
        return LocationWriteList([capability.as_write() for capability in self])


class _LocationUpdate(CogniteUpdate):
    """This is not fully implemented as the Toolkit only needs it for the
    _get_update_properties in the .update method of the Location class.

    All updates are done through the LocationWrite class instead.
    """

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=False),
        ]


class Point3D(CogniteObject):
    """A point in 3D space."""

    def __init__(self, x: float, y: float, z: float) -> None:
        self.x = x
        self.y = y
        self.z = z

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            x=resource["x"],
            y=resource["y"],
            z=resource["z"],
        )


class Quaternion(Point3D):
    """A quaternion."""

    def __init__(self, x: float, y: float, z: float, w: float) -> None:
        super().__init__(x, y, z)
        self.w = w

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            x=resource["x"],
            y=resource["y"],
            z=resource["z"],
            w=resource["w"],
        )


class Transform(CogniteObject):
    """Transform of the parent frame to the current frame.

    Args:
        parent_frame_external_id: The external id of the parent frame.
        translation: Transform translation (Point3)
        orientation: Transform orientation as quaternion (Quaternion).

    """

    def __init__(
        self,
        parent_frame_external_id: str,
        translation: Point3D,
        orientation: Quaternion,
    ) -> None:
        self.parent_frame_external_id = parent_frame_external_id
        self.translation = translation
        self.orientation = orientation

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            parent_frame_external_id=resource["parentFrameExternalId"],
            translation=Point3D._load(resource["translation"]),
            orientation=Quaternion._load(resource["orientation"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "parentFrameExternalId" if camel_case else "parent_frame_external_id": self.parent_frame_external_id,
            "translation": self.translation.dump(camel_case),
            "orientation": self.orientation.dump(camel_case),
        }


class FrameCore(WriteableCogniteResource["FrameWrite"], ABC):
    """The frames resource represents coordinate frames, which are used to describe how maps are aligned with
    respect to each other. For example, frames are used to describe the relative position of a context map
    (e.g., a 3D model of a location) and a robot's navigation map. Frames are aligned with each other through
    transforms, which consist of a translation (in meters) and rotation (quaternion).

    Args:
        name: Frame name.
        external_id: Frame external id. Must be unique for the resource type.
        transform: Transform of the parent frame to the current frame.
    """

    def __init__(
        self,
        name: str,
        external_id: str,
        transform: Transform | None = None,
    ) -> None:
        self.name = name
        self.external_id = external_id
        self.transform = transform

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "name": self.name,
            "externalId" if camel_case else "external_id": self.external_id,
        }
        if self.transform:
            output["transform"] = self.transform.dump(camel_case)
        return output


class FrameWrite(FrameCore):
    """The frames resource represents coordinate frames, which are used to describe how maps are aligned with
    respect to each other. For example, frames are used to describe the relative position of a context map
    (e.g., a 3D model of a location) and a robot's navigation map. Frames are aligned with each other through
    transforms, which consist of a translation (in meters) and rotation (quaternion).

    Args:
        name: Frame name.
        external_id: Frame external id. Must be unique for the resource type.
        transform: Transform of the parent frame to the current frame.
    """

    def __init__(
        self,
        name: str,
        external_id: str,
        transform: Transform | None = None,
    ) -> None:
        super().__init__(name, external_id, transform)

    def as_write(self) -> FrameWrite:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource["externalId"],
            transform=Transform._load(resource["transform"], cognite_client) if resource.get("transform") else None,
        )


class Frame(FrameCore):
    """DataPostprocessing define types of data processing on data captured by the robot.
    DataPostprocessing enables you to automatically process data captured by the robot.

    Args:
        name: Frame name.
        external_id: Frame external id. Must be unique for the resource type.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        created_time: int,
        updated_time: int,
        transform: Transform | None = None,
    ) -> None:
        super().__init__(name, external_id, transform)
        self.created_time = created_time
        self.updated_time = updated_time

    def as_write(self) -> FrameWrite:
        return FrameWrite(
            name=self.name,
            external_id=self.external_id,
            transform=self.transform,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            updated_time=resource["updatedTime"],
            transform=Transform._load(resource["transform"]) if resource.get("transform") else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["createdTime" if camel_case else "created_time"] = self.created_time
        output["updatedTime" if camel_case else "updated_time"] = self.updated_time
        return output


class FrameWriteList(CogniteResourceList, ExternalIDTransformerMixin):
    _RESOURCE = FrameWrite


class FrameList(WriteableCogniteResourceList[FrameWrite, Frame], ExternalIDTransformerMixin):
    _RESOURCE = Frame

    def as_write(self) -> FrameWriteList:
        return FrameWriteList([capability.as_write() for capability in self])


class _FrameUpdate(CogniteUpdate):
    """This is not fully implemented as the Toolkit only needs it for the
    _get_update_properties in the .update method of the Frame class.

    All updates are done through the FrameWrite class instead.
    """

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("transform", is_nullable=False),
        ]


class MapCore(WriteableCogniteResource["MapWrite"], ABC):
    """The map resource allows defining both context maps and robot maps of a specific location. A context map is a
    visual representation of a location, for example, a 3D model, a 2D floor plan, or a point cloud model.
    A robot map is a representation of where a robot is able to navigate. Maps need to be aligned with respect
    to each other using coordinate frames.

    Args:
        name: Map name.
        external_id: Map external id. Must be unique for the resource type.
        map_type: Map type
        description: Description of Map. Textual description of the Map.
        frame_external_id: External id of the map's reference frame.
        data: Map-specific data.
        location_external_id: External id of the location.
        scale: Uniform scaling factor, for example, for map unit conversion (centimeter to meter).

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        map_type: MapType,
        description: str | None = None,
        frame_external_id: str | None = None,
        data: dict | None = None,
        location_external_id: str | None = None,
        scale: float | None = None,
    ) -> None:
        self.name = name
        self.external_id = external_id
        self.map_type = map_type
        self.description = description
        self.frame_external_id = frame_external_id
        self.data = data
        self.location_external_id = location_external_id
        self.scale = scale


class MapWrite(MapCore):
    """The map resource allows defining both context maps and robot maps of a specific location. A context map is a
    visual representation of a location, for example, a 3D model, a 2D floor plan, or a point cloud model.
    A robot map is a representation of where a robot is able to navigate. Maps need to be aligned with respect
    to each other using coordinate frames.

    Args:
        name: Map name.
        external_id: Map external id. Must be unique for the resource type.
        map_type: Map type
        description: Description of Map. Textual description of the Map.
        frame_external_id: External id of the map's reference frame.
        data: Map-specific data.
        location_external_id: External id of the location.
        scale: Uniform scaling factor, for example, for map unit conversion (centimeter to meter).

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        map_type: MapType,
        description: str | None = None,
        frame_external_id: str | None = None,
        data: dict | None = None,
        location_external_id: str | None = None,
        scale: float | None = None,
    ) -> None:
        super().__init__(name, external_id, map_type, description, frame_external_id, data, location_external_id, scale)

    def as_write(self) -> MapWrite:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource["externalId"],
            map_type=resource["mapType"],
            description=resource.get("description"),
            frame_external_id=resource.get("frameExternalId"),
            data=resource.get("data"),
            location_external_id=resource.get("locationExternalId"),
            scale=resource.get("scale"),
        )


class Map(MapCore):
    """The map resource allows defining both context maps and robot maps of a specific location. A context map is a
    visual representation of a location, for example, a 3D model, a 2D floor plan, or a point cloud model.
    A robot map is a representation of where a robot is able to navigate. Maps need to be aligned with respect
    to each other using coordinate frames.

    Args:
        name: Map name.
        external_id: Map external id. Must be unique for the resource type.
        map_type: Map type
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description: Description of Map. Textual description of the Map.
        frame_external_id: External id of the map's reference frame.
        data: Map-specific data.
        location_external_id: External id of the location.
        scale: Uniform scaling factor, for example, for map unit conversion (centimeter to meter).

    """

    def __init__(
        self,
        name: str,
        external_id: str,
        map_type: MapType,
        created_time: int,
        updated_time: int,
        description: str | None = None,
        frame_external_id: str | None = None,
        data: dict | None = None,
        location_external_id: str | None = None,
        scale: float | None = None,
    ) -> None:
        super().__init__(name, external_id, map_type, description, frame_external_id, data, location_external_id, scale)
        self.created_time = created_time
        self.updated_time = updated_time

    def as_write(self) -> MapWrite:
        return MapWrite(
            name=self.name,
            external_id=self.external_id,
            map_type=self.map_type,
            description=self.description,
            frame_external_id=self.frame_external_id,
            data=self.data,
            location_external_id=self.location_external_id,
            scale=self.scale,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource["externalId"],
            map_type=resource["mapType"],
            created_time=resource["createdTime"],
            updated_time=resource["updatedTime"],
            description=resource.get("description"),
            frame_external_id=resource.get("frameExternalId"),
            data=resource.get("data"),
            location_external_id=resource.get("locationExternalId"),
            scale=resource.get("scale"),
        )


class MapWriteList(CogniteResourceList, ExternalIDTransformerMixin):
    _RESOURCE = MapWrite


class MapList(WriteableCogniteResourceList[MapWrite, Map], ExternalIDTransformerMixin):
    _RESOURCE = Map

    def as_write(self) -> MapWriteList:
        return MapWriteList([capability.as_write() for capability in self])


class _MapUpdate(CogniteUpdate):
    """This is not fully implemented as the Toolkit only needs it for the
    _get_update_properties in the .update method of the Map class.

    All updates are done through the MapWrite class instead.
    """

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=False),
            PropertySpec("frame_external_id", is_nullable=False),
            PropertySpec("data", is_nullable=False),
            PropertySpec("location_external_id", is_nullable=False),
            PropertySpec("scale", is_nullable=False),
        ]
