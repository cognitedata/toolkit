from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import Any, Literal

from cognite.client.data_classes.data_modeling import EdgeId, NodeId, ViewId
from cognite.client.utils._identifier import InstanceId
from pydantic import Field

from cognite_toolkit._cdf_tk.constants import DM_EXTERNAL_ID_PATTERN, DM_VERSION_PATTERN, SPACE_FORMAT_PATTERN
from cognite_toolkit._cdf_tk.storageio._data_classes import InstanceIdCSVList

from ._base import DataSelector, SelectorObject


class SelectedView(SelectorObject):
    space: str = Field(
        description="Id of the space that the view belongs to.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the view.",
        min_length=1,
        max_length=255,
        pattern=DM_EXTERNAL_ID_PATTERN,
    )
    version: str | None = Field(
        None,
        description="Version of the view.",
        max_length=43,
        pattern=DM_VERSION_PATTERN,
    )

    def as_id(self) -> ViewId:
        return ViewId(space=self.space, external_id=self.external_id, version=self.version)

    def __str__(self) -> str:
        if self.version:
            return f"{self.space}_{self.external_id}_{self.version}"
        return f"{self.space}_{self.external_id}"


class InstanceSelector(DataSelector, ABC):
    kind: Literal["Instances"] = "Instances"

    @abstractmethod
    def get_schema_spaces(self) -> list[str] | None:
        raise NotImplementedError()

    @abstractmethod
    def get_instance_spaces(self) -> list[str] | None:
        raise NotImplementedError()


class InstanceSpaceSelector(InstanceSelector):
    type: Literal["instanceSpace"] = "instanceSpace"
    instance_space: str
    instance_type: Literal["node", "edge"] = "node"
    view: SelectedView | None = None

    def get_schema_spaces(self) -> list[str] | None:
        return [self.view.space] if self.view else None

    def get_instance_spaces(self) -> list[str] | None:
        return [self.instance_space]

    @property
    def group(self) -> str:
        return self.instance_space

    def __str__(self) -> str:
        if self.view is None:
            return self.instance_type
        return f"{self.view}_{self.instance_type}"

    def as_filter_args(self) -> dict[str, Any]:
        args: dict[str, Any] = {
            "instance_type": self.instance_type,
            "space": self.instance_space,
        }
        if self.view:
            args["source"] = self.view.as_id()
        return args


class InstanceViewSelector(InstanceSelector):
    type: Literal["instanceView"] = "instanceView"
    view: SelectedView
    instance_type: Literal["node", "edge"] = "node"
    instance_spaces: tuple[str, ...] | None = None

    def get_schema_spaces(self) -> list[str] | None:
        return [self.view.space]

    def get_instance_spaces(self) -> list[str] | None:
        return list(self.instance_spaces) if self.instance_spaces else None

    @property
    def group(self) -> str:
        return self.view.space

    def __str__(self) -> str:
        return f"{self.view.external_id}_{self.view.version}_{self.instance_type}"

    def as_filter_args(self) -> dict[str, Any]:
        args: dict[str, Any] = {
            "instance_type": self.instance_type,
            "source": self.view.as_id(),
        }
        if self.instance_spaces:
            args["space"] = list(self.instance_spaces)
        return args


class InstanceFileSelector(InstanceSelector):
    type: Literal["instanceFile"] = "instanceFile"

    datafile: Path
    validate_instance: bool = True

    @property
    def group(self) -> str:
        return "Instances"

    def __str__(self) -> str:
        return f"file_{self.datafile.as_posix()}"

    @cached_property
    def items(self) -> InstanceIdCSVList:
        return InstanceIdCSVList.read_csv_file(self.datafile)

    @cached_property
    def _ids_by_type(self) -> tuple[list[NodeId], list[EdgeId]]:
        node_ids: list[NodeId] = []
        edge_ids: list[EdgeId] = []
        for instance in self.items:
            if instance.instance_type == "node":
                node_ids.append(NodeId(instance.space, instance.external_id))
            else:
                edge_ids.append(EdgeId(instance.space, instance.external_id))
        return node_ids, edge_ids

    @property
    def instance_ids(self) -> list[InstanceId]:
        node_ids, edge_ids = self._ids_by_type
        return [*node_ids, *edge_ids]

    @property
    def node_ids(self) -> list[NodeId]:
        return self._ids_by_type[0]

    @property
    def edge_ids(self) -> list[EdgeId]:
        return self._ids_by_type[1]

    def get_schema_spaces(self) -> list[str] | None:
        return None

    def get_instance_spaces(self) -> list[str] | None:
        return sorted({instance.space for instance in self.items})
