from __future__ import annotations

from collections.abc import Iterable

from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.robotics import Map, MapList, MapWrite, MapWriteList
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
