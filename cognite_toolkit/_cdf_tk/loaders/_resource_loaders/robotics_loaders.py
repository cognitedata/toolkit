from __future__ import annotations

import json
from collections.abc import Callable, Hashable, Iterable, Sequence
from contextlib import suppress
from typing import Any

from cognite.client.data_classes import capabilities
from cognite.client.data_classes._base import T_CogniteResourceList
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
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable


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
        return "robotics frames"

    @classmethod
    def get_id(cls, item: Frame | FrameWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Frame must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[FrameWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
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
        return _fallback_to_one_by_one(self.client.robotics.frames.retrieve, ids, FrameList)

    def update(self, items: FrameWriteList) -> FrameList:
        return self.client.robotics.frames.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.robotics.frames.delete(ids)
        except CogniteAPIError as e:
            return len(e.successful)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Frame]:
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
        return "robotics locations"

    @classmethod
    def get_id(cls, item: Location | LocationWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Location must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[LocationWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        actions = (
            [
                capabilities.RoboticsAcl.Action.Read,
            ]
            if read_only
            else [
                capabilities.RoboticsAcl.Action.Read,
                capabilities.RoboticsAcl.Action.Create,
                capabilities.RoboticsAcl.Action.Delete,
                capabilities.RoboticsAcl.Action.Update,
                capabilities.RoboticsAcl.Action.Delete,
            ]
        )

        return capabilities.RoboticsAcl(actions, capabilities.RoboticsAcl.Scope.All())

    def create(self, items: LocationWriteList) -> LocationList:
        return self.client.robotics.locations.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> LocationList:
        return _fallback_to_one_by_one(self.client.robotics.locations.retrieve, ids, LocationList)

    def update(self, items: LocationWriteList) -> LocationList:
        return self.client.robotics.locations.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.robotics.locations.delete(ids)
        except CogniteAPIError as e:
            return len(e.successful)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Location]:
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
        return "robotics data postprocessing"

    @classmethod
    def get_id(cls, item: DataPostProcessing | DataPostProcessingWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("DataPostProcessing must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[DataPostProcessingWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        actions = (
            [
                capabilities.RoboticsAcl.Action.Read,
            ]
            if read_only
            else [
                capabilities.RoboticsAcl.Action.Read,
                capabilities.RoboticsAcl.Action.Create,
                capabilities.RoboticsAcl.Action.Delete,
                capabilities.RoboticsAcl.Action.Update,
                capabilities.RoboticsAcl.Action.Delete,
            ]
        )

        return capabilities.RoboticsAcl(actions, capabilities.RoboticsAcl.Scope.All())

    def create(self, items: DataPostProcessingWriteList) -> DataPostProcessingList:
        return self.client.robotics.data_postprocessing.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> DataPostProcessingList:
        return _fallback_to_one_by_one(self.client.robotics.data_postprocessing.retrieve, ids, DataPostProcessingList)

    def update(self, items: DataPostProcessingWriteList) -> DataPostProcessingList:
        # There is a bug in the /update endpoint that requires the input_schema to be a string
        # and not an object. This is a workaround until the bug is fixed.
        # We do the serialization to avoid modifying the original object.
        to_update = []
        for item in items:
            if isinstance(item.input_schema, dict):
                update = DataPostProcessingWrite.load(item.dump())
                update.input_schema = json.dumps(item.input_schema)  # type: ignore[assignment]
                to_update.append(update)
            else:
                to_update.append(item)

        return self.client.robotics.data_postprocessing.update(to_update)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.robotics.data_postprocessing.delete(ids)
        except CogniteAPIError as e:
            return len(e.successful)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[DataPostProcessing]:
        return iter(self.client.robotics.data_postprocessing)

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path[0] == "inputSchema":
            return diff_list_hashable(local, cdf)
        return super().diff_list(local, cdf, json_path)


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
        return "robotics robot capabilities"

    @classmethod
    def get_id(cls, item: RobotCapability | RobotCapabilityWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("RobotCapability must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RobotCapabilityWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        actions = (
            [
                capabilities.RoboticsAcl.Action.Read,
            ]
            if read_only
            else [
                capabilities.RoboticsAcl.Action.Read,
                capabilities.RoboticsAcl.Action.Create,
                capabilities.RoboticsAcl.Action.Delete,
                capabilities.RoboticsAcl.Action.Update,
                capabilities.RoboticsAcl.Action.Delete,
            ]
        )

        return capabilities.RoboticsAcl(actions, capabilities.RoboticsAcl.Scope.All())

    def create(self, items: RobotCapabilityWriteList) -> RobotCapabilityList:
        return self.client.robotics.capabilities.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> RobotCapabilityList:
        return _fallback_to_one_by_one(self.client.robotics.capabilities.retrieve, ids, RobotCapabilityList)

    def update(self, items: RobotCapabilityWriteList) -> RobotCapabilityList:
        # There is a bug in the /update endpoint that requires the input_schema to be a string
        # and not an object. This is a workaround until the bug is fixed.
        # We do the serialization to avoid modifying the original object.
        to_update = []
        for item in items:
            if isinstance(item.input_schema, dict) or isinstance(item.data_handling_schema, dict):
                update = RobotCapabilityWrite.load(item.dump())
                if isinstance(item.data_handling_schema, dict):
                    update.data_handling_schema = json.dumps(item.data_handling_schema)  # type: ignore[assignment]
                if isinstance(item.input_schema, dict):
                    update.input_schema = json.dumps(item.input_schema)  # type: ignore[assignment]
                to_update.append(update)
            else:
                to_update.append(item)

        return self.client.robotics.capabilities.update(to_update)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.robotics.capabilities.delete(ids)
        except CogniteAPIError as e:
            return len(e.successful)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[RobotCapability]:
        return iter(self.client.robotics.capabilities)

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path[0] in {"inputSchema", "dataHandlingSchema"}:
            return diff_list_hashable(local, cdf)
        return super().diff_list(local, cdf, json_path)


class RoboticMapLoader(ResourceLoader[str, MapWrite, Map, MapWriteList, MapList]):
    folder_name = "robotics"
    filename_pattern = r"^.*\.Map$"  # Matches all yaml files whose stem ends with '.Map'.
    resource_cls = Map
    resource_write_cls = MapWrite
    list_cls = MapList
    list_write_cls = MapWriteList
    kind = "Map"
    dependencies = frozenset({RoboticFrameLoader, RoboticLocationLoader})
    _doc_url = "Maps/operation/createMaps"

    @property
    def display_name(self) -> str:
        return "robotics maps"

    @classmethod
    def get_id(cls, item: Map | MapWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Map must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[MapWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [
                capabilities.RoboticsAcl.Action.Read,
            ]
            if read_only
            else [
                capabilities.RoboticsAcl.Action.Read,
                capabilities.RoboticsAcl.Action.Create,
                capabilities.RoboticsAcl.Action.Delete,
                capabilities.RoboticsAcl.Action.Update,
                capabilities.RoboticsAcl.Action.Delete,
            ]
        )

        return capabilities.RoboticsAcl(actions, capabilities.RoboticsAcl.Scope.All())

    def dump_resource(self, resource: Map, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dump = resource.as_write().dump()
        local = local or {}
        if dump.get("scale") == 1.0 and "scale" not in local:
            # Default value set on the server side.
            del dump["scale"]
        return dump

    def create(self, items: MapWriteList) -> MapList:
        return self.client.robotics.maps.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> MapList:
        return _fallback_to_one_by_one(self.client.robotics.maps.retrieve, ids, MapList)

    def update(self, items: MapWriteList) -> MapList:
        return self.client.robotics.maps.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.robotics.maps.delete(ids)
        except CogniteAPIError as e:
            return len(e.successful)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Map]:
        return iter(self.client.robotics.maps)


def _fallback_to_one_by_one(
    api_call: Callable, items: Sequence | SequenceNotStr, return_cls: type[T_CogniteResourceList]
) -> T_CogniteResourceList:
    try:
        return api_call(items)
    except CogniteAPIError:
        return_items = return_cls([])
        if len(items) > 1:
            # The API does not give any information about which items were not found/failed.
            # so we need to apply them one by one to find out which ones failed.
            for item in items:
                with suppress(CogniteAPIError):
                    return_items.append(api_call(item))
        return return_items
