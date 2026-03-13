from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import Literal

from cognite.client import data_modeling as dm
from cognite.client.utils._identifier import InstanceId
from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import EdgeTypeId, InstanceDefinitionId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewId, ViewNoVersionId
from cognite_toolkit._cdf_tk.constants import DM_EXTERNAL_ID_PATTERN, DM_VERSION_PATTERN, SPACE_FORMAT_PATTERN
from cognite_toolkit._cdf_tk.storageio._data_classes import InstanceIdCSVList
from cognite_toolkit._cdf_tk.storageio.selectors._base import DataSelector, SelectorObject
from cognite_toolkit._cdf_tk.utils import humanize_collection


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

    def as_id(self) -> ViewNoVersionId:
        if self.version is None:
            return ViewNoVersionId(space=self.space, external_id=self.external_id)
        return ViewId(space=self.space, external_id=self.external_id, version=self.version)

    def __str__(self) -> str:
        base_str = f"{self.space}:{self.external_id}"
        if self.version:
            return f"{base_str}(version={self.version})"
        return base_str


class InstanceSelector(DataSelector, ABC):
    kind: Literal["Instances"] = "Instances"

    @abstractmethod
    def get_schema_spaces(self) -> list[str] | None:
        raise NotImplementedError()

    @abstractmethod
    def get_instance_spaces(self) -> list[str] | None:
        raise NotImplementedError()


class InstanceViewSelector(InstanceSelector):
    """This is used for download"""

    type: Literal["instanceView"] = "instanceView"
    view: SelectedView
    instance_type: Literal["node", "edge"] = "node"
    instance_spaces: tuple[str, ...] | None = None
    edge_types: tuple[EdgeTypeId, ...] | None = None

    def get_schema_spaces(self) -> list[str] | None:
        return [self.view.space]

    def get_instance_spaces(self) -> list[str] | None:
        return list(self.instance_spaces) if self.instance_spaces else None

    def __str__(self) -> str:
        return f"{self.view.external_id}_{self.view.version}_{self.instance_type}"

    @property
    def display_name(self) -> str:
        message = f"{self.instance_type}s in view {self.view!s}"
        if self.instance_spaces:
            message += f" with {humanize_collection(self.instance_spaces)} instance spaces"
        return message


class InstanceSpaceSelector(InstanceSelector):
    """This is used for purge"""

    type: Literal["instanceSpace"] = "instanceSpace"
    instance_space: str
    instance_type: Literal["node", "edge"] = "node"
    view: SelectedView | None = None

    def get_schema_spaces(self) -> list[str] | None:
        return [self.view.space] if self.view else None

    def get_instance_spaces(self) -> list[str] | None:
        return [self.instance_space]

    @property
    def display_name(self) -> str:
        message = f"{self.instance_type}s in {self.instance_space} instance space"
        if self.view is not None:
            message += f" with properties in {self.view!s} view"
        return message

    def __str__(self) -> str:
        if self.view is None:
            return self.instance_type
        return f"{self.view}_{self.instance_type}"


class InstanceFileSelector(InstanceSelector):
    """This is used for the purge command"""

    type: Literal["instanceFile"] = "instanceFile"

    datafile: Path
    validate_instance: bool = True

    @property
    def display_name(self) -> str:
        return f"{self.kind} in {self.datafile!s}"

    def __str__(self) -> str:
        return f"file_{self.datafile.as_posix()}"

    @cached_property
    def items(self) -> InstanceIdCSVList:
        return InstanceIdCSVList.read_csv_file(self.datafile)

    @cached_property
    def ids(self) -> list[InstanceDefinitionId]:
        return [
            InstanceDefinitionId(space=item.space, external_id=item.external_id, instance_type=item.instance_type)
            for item in self.items
        ]

    @cached_property
    def _ids_by_type(self) -> tuple[list[dm.NodeId], list[dm.EdgeId]]:
        node_ids: list[dm.NodeId] = []
        edge_ids: list[dm.EdgeId] = []
        for instance in self.items:
            if instance.instance_type == "node":
                node_ids.append(dm.NodeId(instance.space, instance.external_id))
            else:
                edge_ids.append(dm.EdgeId(instance.space, instance.external_id))
        return node_ids, edge_ids

    @property
    def instance_ids(self) -> list[InstanceId]:
        node_ids, edge_ids = self._ids_by_type
        return [*node_ids, *edge_ids]

    @property
    def node_ids(self) -> list[dm.NodeId]:
        return self._ids_by_type[0]

    @property
    def edge_ids(self) -> list[dm.EdgeId]:
        return self._ids_by_type[1]

    def get_schema_spaces(self) -> list[str] | None:
        return None

    def get_instance_spaces(self) -> list[str] | None:
        return sorted({instance.space for instance in self.items})


class InstanceQuerySelector(InstanceSelector):
    """This is intended for internal use only.

    The motivation for introducing it is the migration of InField data. This requires a special query
    for downloading the relevant instances.

    Args:
        query: The query to execute for selecting the instances. It should be a json-string represting a QueryRequest object.
        root: The root node in the query. This is used for identifying the relevant spaces for
            the migration and for identifying the relevant instances in the response.
        subselections: A list of subselection names in the query. This is used for identifying
    """

    type: Literal["instanceQuery"] = "instanceQuery"

    query: str
    root: str
    subselections: tuple[str, ...]

    def get_schema_spaces(self) -> list[str] | None:
        return None

    def get_instance_spaces(self) -> list[str] | None:
        return None

    def __str__(self) -> str:
        return f"query_{self.root}_{'_'.join(self.subselections)}"
