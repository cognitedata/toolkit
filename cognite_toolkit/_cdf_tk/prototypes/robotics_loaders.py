from __future__ import annotations

from collections.abc import Iterable

from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
    DataPostProcessing,
    DataPostProcessingList,
    DataPostProcessingWrite,
    DataPostProcessingWriteList,
    Frame,
    FrameList,
    FrameWrite,
    FrameWriteList,
    Location,
    LocationList,
    LocationWrite,
    LocationWriteList,
    Map,
    MapList,
    MapWrite,
    MapWriteList,
    RobotCapability,
    RobotCapabilityList,
    RobotCapabilityWrite,
    RobotCapabilityWriteList,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader


class RoboticMapLoader(ResourceLoader[str, MapWrite, Map, MapWriteList, MapList]):
    folder_name = "robotics"
    filename_pattern = r"^.*\.Map$"  # Matches all yaml files whose stem ends with '.Map'.
    resource_cls = Map
    resource_write_cls = MapWrite
    list_cls = MapList
    list_write_cls = MapWriteList
    kind = "Map"
    _doc_url = "Maps/operation/createMaps"

    @property
    def display_name(self) -> str:
        return "robotics.map"

    @classmethod
    def get_id(cls, item: Map | MapWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Map must have external_id")
        return item.external_id

    @classmethod
    def get_required_capability(cls, items: MapWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        return capabilities.RoboticsAcl(
            [
                capabilities.RoboticsAcl.Action.Read,
                capabilities.RoboticsAcl.Action.Create,
                capabilities.RoboticsAcl.Action.Delete,
                capabilities.RoboticsAcl.Action.Update,
                capabilities.RoboticsAcl.Action.Delete,
            ],
            capabilities.RoboticsAcl.Scope.All(),
        )

    def create(self, items: MapWriteList) -> MapList:
        return self.client.robotics.maps.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> MapList:
        try:
            return self.client.robotics.maps.retrieve(ids)
        except CogniteAPIError:
            return MapList([])

    def update(self, items: MapWriteList) -> MapList:
        return self.client.robotics.maps.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.robotics.maps.delete(ids)
        except CogniteAPIError as e:
            return len(e.successful)
        return len(ids)

    def iterate(self) -> Iterable[Map]:
        return iter(self.client.robotics.maps)


class RoboticFrameLoader(ResourceLoader[str, FrameWrite, Frame, FrameWriteList, FrameList]):
    folder_name = "robotics"
    filename_pattern = r"^.*\.Frame$"  # Matches all yaml files whose stem ends with '.Frame'.
    resource_cls = Frame
    resource_write_cls = FrameWrite
    list_cls = FrameList
    list_write_cls = FrameWriteList
    kind = "Frame"
    _doc_url = "Frames/operation/createFrames"

    @property
    def display_name(self) -> str:
        return "robotics.frame"

    @classmethod
    def get_id(cls, item: Frame | FrameWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Frame must have external_id")
        return item.external_id

    @classmethod
    def get_required_capability(cls, items: FrameWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        return capabilities.RoboticsAcl(
            [
                capabilities.RoboticsAcl.Action.Read,
                capabilities.RoboticsAcl.Action.Create,
                capabilities.RoboticsAcl.Action.Delete,
                capabilities.RoboticsAcl.Action.Update,
                capabilities.RoboticsAcl.Action.Delete,
            ],
            capabilities.RoboticsAcl.Scope.All(),
        )

    def create(self, items: FrameWriteList) -> FrameList:
        return self.client.robotics.frames.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> FrameList:
        return self.client.robotics.frames.retrieve(ids)

    def update(self, items: FrameWriteList) -> FrameList:
        return self.client.robotics.frames.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.robotics.frames.delete(ids)
        except CogniteAPIError as e:
            return len(e.successful)
        return len(ids)

    def iterate(self) -> Iterable[Frame]:
        return iter(self.client.robotics.frames)


class RoboticLocationLoader(ResourceLoader[str, LocationWrite, Location, LocationWriteList, LocationList]):
    folder_name = "robotics"
    filename_pattern = r"^.*\.Location$"  # Matches all yaml files whose stem ends with '.Location'.
    resource_cls = Location
    resource_write_cls = LocationWrite
    list_cls = LocationList
    list_write_cls = LocationWriteList
    kind = "Location"
    _doc_url = "Locations/operation/createLocations"

    @property
    def display_name(self) -> str:
        return "robotics.location"

    @classmethod
    def get_id(cls, item: Location | LocationWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Location must have external_id")
        return item.external_id

    @classmethod
    def get_required_capability(cls, items: LocationWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        return capabilities.RoboticsAcl(
            [
                capabilities.RoboticsAcl.Action.Read,
                capabilities.RoboticsAcl.Action.Create,
                capabilities.RoboticsAcl.Action.Delete,
                capabilities.RoboticsAcl.Action.Update,
                capabilities.RoboticsAcl.Action.Delete,
            ],
            capabilities.RoboticsAcl.Scope.All(),
        )

    def create(self, items: LocationWriteList) -> LocationList:
        return self.client.robotics.locations.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> LocationList:
        return self.client.robotics.locations.retrieve(ids)

    def update(self, items: LocationWriteList) -> LocationList:
        return self.client.robotics.locations.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.robotics.locations.delete(ids)
        except CogniteAPIError as e:
            return len(e.successful)
        return len(ids)

    def iterate(self) -> Iterable[Location]:
        return iter(self.client.robotics.locations)


class RoboticsDataPostProcessingLoader(
    ResourceLoader[
        str, DataPostProcessingWrite, DataPostProcessing, DataPostProcessingWriteList, DataPostProcessingList
    ]
):
    folder_name = "robotics"
    filename_pattern = r"^.*\.DataPostProcessing$"  # Matches all yaml files whose stem ends with '.DataPostProcessing'.
    resource_cls = DataPostProcessing
    resource_write_cls = DataPostProcessingWrite
    list_cls = DataPostProcessingList
    list_write_cls = DataPostProcessingWriteList
    kind = "DataPostProcessing"
    _doc_url = "DataPostProcessing/operation/createDataPostProcessing"

    @property
    def display_name(self) -> str:
        return "robotics.data_postprocessing"

    @classmethod
    def get_id(cls, item: DataPostProcessing | DataPostProcessingWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("DataPostProcessing must have external_id")
        return item.external_id

    @classmethod
    def get_required_capability(cls, items: DataPostProcessingWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        return capabilities.RoboticsAcl(
            [
                capabilities.RoboticsAcl.Action.Read,
                capabilities.RoboticsAcl.Action.Create,
                capabilities.RoboticsAcl.Action.Delete,
                capabilities.RoboticsAcl.Action.Update,
                capabilities.RoboticsAcl.Action.Delete,
            ],
            capabilities.RoboticsAcl.Scope.All(),
        )

    def create(self, items: DataPostProcessingWriteList) -> DataPostProcessingList:
        return self.client.robotics.data_postprocessing.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> DataPostProcessingList:
        try:
            return self.client.robotics.data_postprocessing.retrieve(ids)
        except CogniteAPIError:
            return DataPostProcessingList([])

    def update(self, items: DataPostProcessingWriteList) -> DataPostProcessingList:
        return self.client.robotics.data_postprocessing.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.robotics.data_postprocessing.delete(ids)
        except CogniteAPIError as e:
            return len(e.successful)
        return len(ids)

    def iterate(self) -> Iterable[DataPostProcessing]:
        return iter(self.client.robotics.data_postprocessing)


class RobotCapabilityLoader(
    ResourceLoader[str, RobotCapabilityWrite, RobotCapability, RobotCapabilityWriteList, RobotCapabilityList]
):
    folder_name = "robotics"
    filename_pattern = r"^.*\.RobotCapability$"  # Matches all yaml files whose stem ends with '.RobotCapability'.
    resource_cls = RobotCapability
    resource_write_cls = RobotCapabilityWrite
    list_cls = RobotCapabilityList
    list_write_cls = RobotCapabilityWriteList
    kind = "RobotCapability"
    _doc_url = "RobotCapabilities/operation/createRobotCapabilities"

    @property
    def display_name(self) -> str:
        return "robotics.robot_capability"

    @classmethod
    def get_id(cls, item: RobotCapability | RobotCapabilityWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("RobotCapability must have external_id")
        return item.external_id

    @classmethod
    def get_required_capability(cls, items: RobotCapabilityWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        return capabilities.RoboticsAcl(
            [
                capabilities.RoboticsAcl.Action.Read,
                capabilities.RoboticsAcl.Action.Create,
                capabilities.RoboticsAcl.Action.Delete,
                capabilities.RoboticsAcl.Action.Update,
                capabilities.RoboticsAcl.Action.Delete,
            ],
            capabilities.RoboticsAcl.Scope.All(),
        )

    def create(self, items: RobotCapabilityWriteList) -> RobotCapabilityList:
        return self.client.robotics.capabilities.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> RobotCapabilityList:
        return self.client.robotics.capabilities.retrieve(ids)

    def update(self, items: RobotCapabilityWriteList) -> RobotCapabilityList:
        return self.client.robotics.capabilities.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.robotics.capabilities.delete(ids)
        except CogniteAPIError as e:
            return len(e.successful)
        return len(ids)

    def iterate(self) -> Iterable[RobotCapability]:
        return iter(self.client.robotics.capabilities)
