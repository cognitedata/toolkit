from __future__ import annotations

from collections.abc import Iterable

from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.robotics import (
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
        return self.client.robotics.maps.retrieve(ids)

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
