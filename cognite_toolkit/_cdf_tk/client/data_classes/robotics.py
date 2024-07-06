from __future__ import annotations

from abc import ABC
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from typing_extensions import Self, TypeAlias


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


class RobotCapabilityWriteList(CogniteResourceList):
    _RESOURCE = RobotCapabilityWrite


class RobotCapabilityList(WriteableCogniteResourceList[RobotCapabilityWrite, RobotCapability]):
    _RESOURCE = RobotCapability

    def as_write(self) -> RobotCapabilityWriteList:
        return RobotCapabilityWriteList([capability.as_write() for capability in self])


class _RobotCapabilityUpdate(CogniteUpdate):
    """This is not fully implemented as the Toolkit only needs it for the
    _get_update_properties in the .update method of the RobotCapability class.

    All updates are done through the RobotCapabilityWrite class instead.
    """

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
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
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=False),
            PropertySpec("metadata", is_container=True, is_nullable=False),
            PropertySpec("robot_type", is_nullable=False),
            PropertySpec("location_external_id", is_nullable=False),
        ]


class DataProcessingCore(WriteableCogniteResource["DataProcessingWrite"], ABC):
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


class DataProcessingWrite(DataProcessingCore):
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

    def as_write(self) -> DataProcessingWrite:
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


class DataProcessing(DataProcessingCore):
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

    def as_write(self) -> DataProcessingWrite:
        return DataProcessingWrite(
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


class DataProcessingWriteList(CogniteResourceList):
    _RESOURCE = DataProcessingWrite


class DataProcessingList(WriteableCogniteResourceList[DataProcessingWrite, DataProcessing]):
    _RESOURCE = DataProcessing

    def as_write(self) -> DataProcessingWriteList:
        return DataProcessingWriteList([capability.as_write() for capability in self])


class _DataProcessingUpdate(CogniteUpdate):
    """This is not fully implemented as the Toolkit only needs it for the
    _get_update_properties in the .update method of the DataProcessing class.

    All updates are done through the DataProcessingWrite class instead.
    """

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
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


class LocationWriteList(CogniteResourceList):
    _RESOURCE = LocationWrite


class LocationList(WriteableCogniteResourceList[LocationWrite, Location]):
    _RESOURCE = Location

    def as_write(self) -> LocationWriteList:
        return LocationWriteList([capability.as_write() for capability in self])


class _LocationUpdate(CogniteUpdate):
    """This is not fully implemented as the Toolkit only needs it for the
    _get_update_properties in the .update method of the Location class.

    All updates are done through the LocationWrite class instead.
    """

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=False),
        ]
