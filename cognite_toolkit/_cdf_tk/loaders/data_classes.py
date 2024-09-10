"""These are helper data classes for the load module.

There are three types of classes in this module made with different motivations:

* RawDatabaseTable and RawTableList are CogniteResources following the pattern
  from the cognite-sdk for RAW databases and tables. They are missing from the cognite-sdk,
  and are needed to load YAML.
* LoadableNodes and LoadableEdges are extensions of NodeApplyList and EdgeApplyList
  to also contain the arguments of the 'client.data_modeling.instances.apply()' method. This enables
  the user to specify these arguments in the YAML file.
* DeployResult and DeployResults are storing the output of the .deploy_resources and .clean_resources,
  which then shows a summary of the operation in the terminal. The DeployResults class is a UserList
  of DeployResult objects, and is used to print the summary in a table.
"""

from __future__ import annotations

from abc import ABC
from collections import UserDict
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from functools import total_ordering
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import DataModelId, NodeApply, NodeApplyList, ViewId
from cognite.client.data_classes.data_modeling.core import DataModelingSchemaResource
from rich.table import Table


@dataclass(frozen=True)
class FunctionScheduleID:
    function_external_id: str
    name: str

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "functionExternalId" if camel_case else "function_external_id": self.function_external_id,
            "name": self.name,
        }


@total_ordering
@dataclass(frozen=True)
class RawDatabaseTable(WriteableCogniteResource):
    db_name: str
    table_name: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> RawDatabaseTable:
        return cls(db_name=resource["dbName"], table_name=resource.get("tableName"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = {
            "dbName" if camel_case else "db_name": self.db_name,
        }
        if self.table_name is not None:
            dumped["tableName" if camel_case else "table_name"] = self.table_name
        return dumped

    def as_write(self) -> RawDatabaseTable:
        return self

    def __lt__(self, other: object) -> bool:
        if isinstance(other, RawDatabaseTable):
            if self.db_name == other.db_name:
                return (self.table_name or "") < (other.table_name or "")
            else:
                return self.db_name < other.db_name
        else:
            return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RawDatabaseTable):
            return self.db_name == other.db_name and self.table_name == other.table_name
        else:
            return NotImplemented

    def __str__(self) -> str:
        if self.table_name is None:
            return self.db_name
        else:
            return super().__str__()

    def __repr__(self) -> str:
        if self.table_name is None:
            return f"{type(self).__name__}(db_name='{self.db_name}')"
        else:
            return f"{type(self).__name__}(db_name='{self.db_name}', table_name='{self.table_name}')"


class RawTableList(WriteableCogniteResourceList[RawDatabaseTable, RawDatabaseTable]):
    _RESOURCE = RawDatabaseTable

    def as_write(self) -> CogniteResourceList[RawDatabaseTable]:
        return self

    def as_db_names(self) -> list[str]:
        return [table.db_name for table in self.data]


class _GraphQLDataModelCore(DataModelingSchemaResource["GraphQLDataModelWrite"], ABC):
    def __init__(
        self, space: str, external_id: str, version: str, name: str | None = None, description: str | None = None
    ) -> None:
        super().__init__(space=space, external_id=external_id, name=name, description=description)
        self.version = version

    def as_id(self) -> DataModelId:
        return DataModelId(space=self.space, external_id=self.external_id, version=self.version)


class GraphQLDataModelWrite(_GraphQLDataModelCore):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        name: str | None = None,
        description: str | None = None,
        previous_version: str | None = None,
        dml: str | None = None,
    ) -> None:
        super().__init__(space=space, external_id=external_id, version=version, name=name, description=description)
        self.previous_version = previous_version
        self.dml = dml

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> GraphQLDataModelWrite:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=str(resource["version"]),
            name=resource.get("name"),
            description=resource.get("description"),
            previous_version=resource.get("previousVersion"),
            dml=resource.get("dml"),
        )

    def as_write(self) -> GraphQLDataModelWrite:
        return self


class GraphQLDataModel(_GraphQLDataModelCore):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: str,
        is_global: bool,
        last_updated_time: int,
        created_time: int,
        description: str | None,
        name: str | None,
        views: list[ViewId] | None,
    ) -> None:
        super().__init__(space=space, external_id=external_id, version=version, name=name, description=description)
        self.is_global = is_global
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.views = views

    def as_write(self) -> GraphQLDataModelWrite:
        return GraphQLDataModelWrite(
            space=self.space,
            external_id=self.external_id,
            version=self.version,
            name=self.name,
            description=self.description,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> GraphQLDataModel:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            version=str(resource["version"]),
            is_global=resource["isGlobal"],
            last_updated_time=resource["lastUpdatedTime"],
            created_time=resource["createdTime"],
            description=resource.get("description"),
            name=resource.get("name"),
            views=[
                ViewId(space=view["space"], external_id=view["externalId"], version=view.get("version"))
                for view in resource.get("views", [])
            ],
        )


class GraphQLDataModelWriteList(CogniteResourceList[GraphQLDataModelWrite]):
    _RESOURCE = GraphQLDataModelWrite

    def as_ids(self) -> list[DataModelId]:
        return [model.as_id() for model in self.data]


class GraphQLDataModelList(WriteableCogniteResourceList[GraphQLDataModelWrite, GraphQLDataModel]):
    _RESOURCE = GraphQLDataModel

    def as_write(self) -> GraphQLDataModelWriteList:
        return GraphQLDataModelWriteList([model.as_write() for model in self.data])

    def as_ids(self) -> list[DataModelId]:
        return [model.as_id() for model in self.data]


@dataclass(frozen=True, order=True)
class NodeAPICall:
    auto_create_direct_relations: bool | None
    skip_on_version_conflict: bool | None
    replace: bool | None

    @classmethod
    def load(cls, resource: dict[str, Any]) -> NodeAPICall:
        return cls(
            auto_create_direct_relations=resource.get("autoCreateDirectRelations"),
            skip_on_version_conflict=resource.get("skipOnVersionConflict"),
            replace=resource.get("replace"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {}
        if self.auto_create_direct_relations is not None:
            output["autoCreateDirectRelations" if camel_case else "auto_create_direct_relations"] = (
                self.auto_create_direct_relations
            )
        if self.skip_on_version_conflict is not None:
            output["skipOnVersionConflict" if camel_case else "skip_on_version_conflict"] = (
                self.skip_on_version_conflict
            )
        if self.replace is not None:
            output["replace"] = self.replace
        return output


class NodeApplyListWithCall(CogniteResourceList[NodeApply]):
    _RESOURCE = NodeApply

    def __init__(self, resources: Collection[Any], api_call: NodeAPICall | None = None) -> None:
        super().__init__(resources, cognite_client=None)
        self.api_call = api_call

    @classmethod
    def _load(  # type: ignore[override]
        cls, resource: dict[str, Any] | list[dict[str, Any]], cognite_client: CogniteClient | None = None
    ) -> NodeApplyListWithCall:
        api_call: NodeAPICall | None = None
        if isinstance(resource, dict) and ("nodes" in resource or "node" in resource):
            api_call = NodeAPICall.load(resource)

        if api_call and isinstance(resource, dict) and "nodes" in resource:
            nodes = NodeApplyList.load(resource["nodes"])
        elif api_call and isinstance(resource, dict) and "node" in resource:
            nodes = NodeApplyList([NodeApply.load(resource["node"])])
        elif isinstance(resource, list):
            nodes = NodeApplyList.load(resource)
        elif isinstance(resource, dict):
            nodes = NodeApplyList([NodeApply.load(resource)])
        else:
            raise ValueError("Invalid input for NodeApplyListWithCall")
        return cls(nodes, api_call)

    def dump(self, camel_case: bool = True) -> dict[str, Any] | list[dict[str, Any]]:  # type: ignore[override]
        nodes = [resource.dump(camel_case) for resource in self.data]
        if self.api_call is not None:
            if len(nodes) == 1:
                return {**self.api_call.dump(camel_case), "node": nodes[0]}
            else:
                return {**self.api_call.dump(camel_case), "nodes": nodes}
        else:
            return nodes


@total_ordering
@dataclass
class DeployResult(ABC):
    name: str

    def __lt__(self, other: object) -> bool:
        if isinstance(other, DeployResult):
            return self.name < other.name
        else:
            return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DeployResult):
            return self.name == other.name
        else:
            return NotImplemented


@dataclass
class ResourceDeployResult(DeployResult):
    created: int = 0
    deleted: int = 0
    changed: int = 0
    unchanged: int = 0
    total: int = 0

    @property
    def calculated_total(self) -> int:
        return self.created + self.deleted + self.changed + self.unchanged

    def __iadd__(self, other: ResourceDeployResult) -> ResourceDeployResult:
        if self.name != other.name:
            raise ValueError("Cannot add two DeployResult objects with different names")
        self.created += other.created
        self.deleted += other.deleted
        self.changed += other.changed
        self.unchanged += other.unchanged
        self.total += other.total

        if isinstance(other, ResourceContainerDeployResult):
            return ResourceContainerDeployResult(
                name=self.name,
                created=self.created,
                deleted=self.deleted,
                changed=self.changed,
                unchanged=self.unchanged,
                total=self.total,
                item_name=other.item_name,
                dropped_datapoints=other.dropped_datapoints,
            )
        else:
            return self


@dataclass
class ResourceContainerDeployResult(ResourceDeployResult):
    item_name: str = ""
    dropped_datapoints: int = 0

    def __iadd__(self, other: ResourceDeployResult) -> ResourceContainerDeployResult:
        if self.name != other.name:
            raise ValueError("Cannot add two ResourceContainerDeployResult objects with different names")
        super().__iadd__(other)
        if isinstance(other, ResourceContainerDeployResult):
            self.dropped_datapoints += other.dropped_datapoints
        return self


@dataclass
class UploadDeployResult(DeployResult):
    uploaded: int = 0
    item_name: str = ""

    def __iadd__(self, other: UploadDeployResult) -> UploadDeployResult:
        if self.name != other.name:
            raise ValueError("Cannot add two DeployResult objects with different names")
        self.uploaded += other.uploaded

        if isinstance(other, DatapointDeployResult):
            return DatapointDeployResult(
                name=self.name, uploaded=self.uploaded, item_name=other.item_name, points=other.points
            )
        else:
            return self


@dataclass
class DatapointDeployResult(UploadDeployResult):
    points: int = 0

    def __iadd__(self, other: UploadDeployResult) -> UploadDeployResult:
        if self.name != other.name:
            raise ValueError("Cannot add two DeployResult objects with different names")
        super().__iadd__(other)
        if isinstance(other, DatapointDeployResult):
            self.points += other.points
        return self


class DeployResults(UserDict):
    def __init__(self, collection: Iterable[DeployResult], action: Literal["deploy", "clean"], dry_run: bool = False):
        super().__init__({entry.name: entry for entry in collection})
        self.action = action
        self.dry_run = dry_run

    @property
    def has_counts(self) -> bool:
        return any(isinstance(entry, ResourceDeployResult) for entry in self.data.values())

    @property
    def has_uploads(self) -> bool:
        return any(
            isinstance(entry, (UploadDeployResult, ResourceContainerDeployResult)) for entry in self.data.values()
        )

    def counts_table(self) -> Table:
        table = Table(title=f"Summary of Resources {self.action.title()} operation:")
        prefix = "Would have " if self.dry_run else ""
        table.add_column("Resource", justify="right")
        table.add_column(f"{prefix}Created", justify="right", style="green")
        table.add_column(f"{prefix}Deleted", justify="right", style="red")
        table.add_column(f"{prefix}Changed", justify="right", style="magenta")
        table.add_column("Untouched" if self.dry_run else "Unchanged", justify="right", style="cyan")
        table.add_column("Total", justify="right")
        is_deploy = self.action == "deploy"
        for item in sorted(
            entry for entry in self.data.values() if entry is not None and isinstance(entry, ResourceDeployResult)
        ):
            table.add_row(
                item.name,
                str(item.created) if is_deploy else "-",
                str(item.deleted),
                str(item.changed) if is_deploy else "-",
                str(item.unchanged) if is_deploy else "-",
                str(item.total),
            )

        return table

    def uploads_table(self) -> Table:
        table = Table(title=f"Summary of Data {self.action.title()} operation" " (data is always uploaded):")
        prefix = "Would have " if self.dry_run else ""
        table.add_column("Resource", justify="right")
        table.add_column(f"{prefix}Uploaded Data", justify="right", style="cyan")
        table.add_column("Item Type", justify="right")
        table.add_column("From files", justify="right", style="green")
        table.add_column(f"{prefix}Deleted Data", justify="right", style="red")
        for item in sorted(
            entry
            for entry in self.data.values()
            if isinstance(entry, (UploadDeployResult, ResourceContainerDeployResult))
        ):
            if item.name == "raw.tables":
                # We skip this as we cannot count the number of datapoints in a raw table
                # and all we can do is to print a misleading 0 for deleted datapoints.
                continue

            if isinstance(item, UploadDeployResult):
                if isinstance(item, DatapointDeployResult):
                    datapoints = f"{item.points:,}"
                else:
                    datapoints = "-"
                table.add_row(item.name, datapoints, item.item_name, str(item.uploaded), "-")
            elif isinstance(item, ResourceContainerDeployResult):
                table.add_row(item.name, "-", item.item_name, "-", f"{item.dropped_datapoints:,}")

        return table

    def __getitem__(self, item: str) -> DeployResult:
        return self.data[item]

    def __setitem__(self, key: str, value: DeployResult) -> None:
        if key not in self.data:
            self.data[key] = value
        else:
            self.data[key] += value
