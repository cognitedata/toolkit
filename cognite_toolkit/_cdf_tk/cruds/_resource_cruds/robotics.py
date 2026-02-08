import json
from collections.abc import Hashable, Iterable, Sequence
from typing import Any

from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.robotics import (
    RobotCapabilityRequest,
    RobotCapabilityResponse,
    RobotDataPostProcessingRequest,
    RobotDataPostProcessingResponse,
    RobotFrameRequest,
    RobotFrameResponse,
    RobotLocationRequest,
    RobotLocationResponse,
    RobotMapRequest,
    RobotMapResponse,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import (
    RobotCapabilityYAML,
    RobotDataPostProcessingYAML,
    RobotFrameYAML,
    RobotLocationYAML,
    RobotMapYAML,
)
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable


class RoboticFrameCRUD(ResourceCRUD[ExternalId, RobotFrameRequest, RobotFrameResponse]):
    folder_name = "robotics"
    resource_cls = RobotFrameResponse
    resource_write_cls = RobotFrameRequest
    kind = "Frame"
    yaml_cls = RobotFrameYAML
    _doc_url = "Frames/operation/createFrames"

    @property
    def display_name(self) -> str:
        return "robotics frames"

    @classmethod
    def get_id(cls, item: RobotFrameRequest | RobotFrameResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RobotFrameRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        return capabilities.RoboticsAcl(
            [
                capabilities.RoboticsAcl.Action.Read,
                capabilities.RoboticsAcl.Action.Create,
                capabilities.RoboticsAcl.Action.Delete,
                capabilities.RoboticsAcl.Action.Update,
            ],
            capabilities.RoboticsAcl.Scope.All(),
        )

    def dump_resource(self, resource: RobotFrameResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if dumped.get("transform") is None and "transform" not in (local or {}):
            # Default value set on the server side.
            dumped.pop("transform", None)
        return dumped

    def create(self, items: Sequence[RobotFrameRequest]) -> list[RobotFrameResponse]:
        return self.client.tool.robotics.frames.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[RobotFrameResponse]:
        return self.client.tool.robotics.frames.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[RobotFrameRequest]) -> list[RobotFrameResponse]:
        return self.client.tool.robotics.frames.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.robotics.frames.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[RobotFrameResponse]:
        for frames in self.client.tool.robotics.frames.iterate(limit=None):
            yield from frames


class RoboticLocationCRUD(ResourceCRUD[ExternalId, RobotLocationRequest, RobotLocationResponse]):
    folder_name = "robotics"
    resource_cls = RobotLocationResponse
    resource_write_cls = RobotLocationRequest
    kind = "Location"
    yaml_cls = RobotLocationYAML
    _doc_url = "Locations/operation/createLocations"

    @property
    def display_name(self) -> str:
        return "robotics locations"

    @classmethod
    def get_id(cls, item: RobotLocationRequest | RobotLocationResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RobotLocationRequest] | None, read_only: bool
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
            ]
        )

        return capabilities.RoboticsAcl(actions, capabilities.RoboticsAcl.Scope.All())

    def create(self, items: Sequence[RobotLocationRequest]) -> list[RobotLocationResponse]:
        return self.client.tool.robotics.locations.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[RobotLocationResponse]:
        return self.client.tool.robotics.locations.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[RobotLocationRequest]) -> list[RobotLocationResponse]:
        return self.client.tool.robotics.locations.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.robotics.locations.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[RobotLocationResponse]:
        for locations in self.client.tool.robotics.locations.iterate(limit=None):
            yield from locations

    def dump_resource(self, resource: RobotLocationResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        return resource.as_request_resource().dump()

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> RobotLocationRequest:
        return RobotLocationRequest.model_validate(resource)


class RoboticsDataPostProcessingCRUD(
    ResourceCRUD[ExternalId, RobotDataPostProcessingRequest, RobotDataPostProcessingResponse]
):
    folder_name = "robotics"
    resource_cls = RobotDataPostProcessingResponse
    resource_write_cls = RobotDataPostProcessingRequest
    kind = "DataPostProcessing"
    yaml_cls = RobotDataPostProcessingYAML
    _doc_url = "DataPostProcessing/operation/createDataPostProcessing"

    @property
    def display_name(self) -> str:
        return "robotics data postprocessing"

    @classmethod
    def get_id(cls, item: RobotDataPostProcessingRequest | RobotDataPostProcessingResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RobotDataPostProcessingRequest] | None, read_only: bool
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
            ]
        )

        return capabilities.RoboticsAcl(actions, capabilities.RoboticsAcl.Scope.All())

    def create(self, items: Sequence[RobotDataPostProcessingRequest]) -> list[RobotDataPostProcessingResponse]:
        return self.client.tool.robotics.data_postprocessing.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[RobotDataPostProcessingResponse]:
        return self.client.tool.robotics.data_postprocessing.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[RobotDataPostProcessingRequest]) -> list[RobotDataPostProcessingResponse]:
        # There is a bug in the /update endpoint that requires the input_schema to be a string
        # and not an object. This is a workaround until the bug is fixed.
        # We do the serialization to avoid modifying the original object.
        to_update = []
        for item in items:
            if isinstance(item.input_schema, dict):
                update = RobotDataPostProcessingRequest.model_validate(item.dump())
                update.input_schema = json.dumps(item.input_schema)  # type: ignore[assignment]
                to_update.append(update)
            else:
                to_update.append(item)

        return self.client.tool.robotics.data_postprocessing.update(to_update, mode="replace")

    def delete(self, ids: SequenceNotStr[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.robotics.data_postprocessing.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[RobotDataPostProcessingResponse]:
        for items in self.client.tool.robotics.data_postprocessing.iterate(limit=None):
            yield from items

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path[0] == "inputSchema":
            return diff_list_hashable(local, cdf)
        return super().diff_list(local, cdf, json_path)


class RobotCapabilityCRUD(ResourceCRUD[ExternalId, RobotCapabilityRequest, RobotCapabilityResponse]):
    folder_name = "robotics"
    resource_cls = RobotCapabilityResponse
    resource_write_cls = RobotCapabilityRequest
    kind = "RobotCapability"
    yaml_cls = RobotCapabilityYAML
    _doc_url = "RobotCapabilities/operation/createRobotCapabilities"

    @property
    def display_name(self) -> str:
        return "robotics robot capabilities"

    @classmethod
    def get_id(cls, item: RobotCapabilityRequest | RobotCapabilityResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RobotCapabilityRequest] | None, read_only: bool
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
            ]
        )

        return capabilities.RoboticsAcl(actions, capabilities.RoboticsAcl.Scope.All())

    def create(self, items: Sequence[RobotCapabilityRequest]) -> list[RobotCapabilityResponse]:
        return self.client.tool.robotics.capabilities.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[RobotCapabilityResponse]:
        return self.client.tool.robotics.capabilities.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[RobotCapabilityRequest]) -> list[RobotCapabilityResponse]:
        # There is a bug in the /update endpoint that requires the input_schema to be a string
        # and not an object. This is a workaround until the bug is fixed.
        # We do the serialization to avoid modifying the original object.
        to_update = []
        for item in items:
            if isinstance(item.input_schema, dict) or isinstance(item.data_handling_schema, dict):
                update = RobotCapabilityRequest.model_validate(item.dump())
                if isinstance(item.data_handling_schema, dict):
                    update.data_handling_schema = json.dumps(item.data_handling_schema)  # type: ignore[assignment]
                if isinstance(item.input_schema, dict):
                    update.input_schema = json.dumps(item.input_schema)  # type: ignore[assignment]
                to_update.append(update)
            else:
                to_update.append(item)

        return self.client.tool.robotics.capabilities.update(to_update, mode="replace")

    def delete(self, ids: SequenceNotStr[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.robotics.capabilities.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[RobotCapabilityResponse]:
        for items in self.client.tool.robotics.capabilities.iterate(limit=None):
            yield from items

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path[0] in {"inputSchema", "dataHandlingSchema"}:
            return diff_list_hashable(local, cdf)
        return super().diff_list(local, cdf, json_path)


class RoboticMapCRUD(ResourceCRUD[ExternalId, RobotMapRequest, RobotMapResponse]):
    folder_name = "robotics"
    resource_cls = RobotMapResponse
    resource_write_cls = RobotMapRequest
    kind = "Map"
    dependencies = frozenset({RoboticFrameCRUD, RoboticLocationCRUD})
    yaml_cls = RobotMapYAML
    _doc_url = "Maps/operation/createMaps"

    @property
    def display_name(self) -> str:
        return "robotics maps"

    @classmethod
    def get_id(cls, item: RobotMapRequest | RobotMapResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RobotMapRequest] | None, read_only: bool
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
            ]
        )

        return capabilities.RoboticsAcl(actions, capabilities.RoboticsAcl.Scope.All())

    def dump_resource(self, resource: RobotMapResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dump = resource.as_request_resource().dump()
        local = local or {}
        if dump.get("scale") == 1.0 and "scale" not in local:
            # Default value set on the server side.
            del dump["scale"]
        for key in [
            "data",
            "frameExternalId",
            "locationExternalId",
        ]:
            if dump.get(key) is None and key not in (local or {}):
                # Key set to null on the server side.
                dump.pop(key, None)
        return dump

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> RobotMapRequest:
        return RobotMapRequest.model_validate(resource)

    def create(self, items: Sequence[RobotMapRequest]) -> list[RobotMapResponse]:
        return self.client.tool.robotics.maps.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[RobotMapResponse]:
        return self.client.tool.robotics.maps.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[RobotMapRequest]) -> list[RobotMapResponse]:
        return self.client.tool.robotics.maps.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.robotics.maps.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[RobotMapResponse]:
        for maps in self.client.tool.robotics.maps.iterate(limit=None):
            yield from maps
