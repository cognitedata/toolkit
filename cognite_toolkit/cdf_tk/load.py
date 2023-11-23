# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import io
import itertools
import re
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from collections.abc import Sequence, Sized
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar, Union, final

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    FileMetadata,
    FileMetadataList,
    OidcCredentials,
    TimeSeries,
    TimeSeriesList,
    Transformation,
    TransformationList,
)
from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
)
from cognite.client.data_classes.capabilities import (
    Capability,
    FilesAcl,
    GroupsAcl,
    RawAcl,
    TimeSeriesAcl,
    TransformationsAcl,
)
from cognite.client.data_classes.data_modeling import (
    ContainerApply,
    DataModelApply,
    NodeApply,
    NodeApplyList,
    NodeOrEdgeData,
    SpaceApply,
    ViewApply,
    ViewId,
)
from cognite.client.data_classes.iam import Group, GroupList
from cognite.client.exceptions import CogniteAPIError
from rich import print

from .delete import delete_instances
from .utils import CDFToolConfig, load_yaml_inject_variables


@dataclass
class RawTable(CogniteObject):
    db_name: str
    table_name: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> RawTable:
        return cls(db_name=resource["dbName"], table_name=resource["tableName"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "dbName" if camel_case else "db_name": self.db_name,
            "tableName" if camel_case else "table_name": self.table_name,
        }


@dataclass
class Difference:
    added: list[CogniteResource]
    removed: list[CogniteResource]
    changed: list[CogniteResource]
    unchanged: list[CogniteResource]

    def __iter__(self):
        return iter([self.added, self.removed, self.changed, self.unchanged])

    def __next__(self):
        return next([self.added, self.removed, self.changed, self.unchanged])


T_ID = TypeVar("T_ID", bound=Union[str, int])
T_Resource = TypeVar("T_Resource")
T_ResourceList = TypeVar("T_ResourceList")


class Loader(ABC, Generic[T_ID, T_Resource, T_ResourceList]):
    """
    This is the base class for all loaders. It defines the interface that all loaders must implement.

    A loader is a class that describes how a resource is loaded from a file and uploaded to CDF.

    All resources supported by the cognite_toolkit should implement a loader.

    Class attributes:
        support_drop: Whether the resource supports the drop flag.
        filetypes: The filetypes that are supported by this loader. If empty, all files are supported.
        api_name: The name of the api that is in the cognite_client that is used to interact with the CDF API.
        folder_name: The name of the folder in the build directory where the files are located.
        resource_cls: The class of the resource that is loaded.
        list_cls: The list version of the resource class.
    """

    support_drop = True
    filetypes = frozenset({"yaml", "yml"})
    api_name: str
    folder_name: str
    resource_cls: type[CogniteResource]
    list_cls: type[CogniteResourceList]

    def __init__(self, client: CogniteClient):
        self.client = client
        try:
            self.api_class = self._get_api_class(client, self.api_name)
        except AttributeError:
            raise AttributeError(f"Invalid api_name {self.api_name}.")

    @staticmethod
    def _get_api_class(client, api_name: str):
        parent = client
        if (dot_count := Counter(api_name)["."]) == 1:
            parent_name, api_name = api_name.split(".")
            parent = getattr(client, parent_name)
        elif dot_count == 0:
            pass
        else:
            raise AttributeError(f"Invalid api_name {api_name}.")
        return getattr(parent, api_name)

    @classmethod
    def create_loader(cls, ToolGlobals: CDFToolConfig):
        client = ToolGlobals.verify_capabilities(capability=cls.get_required_capability(ToolGlobals))
        return cls(client)

    @classmethod
    @abstractmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        raise NotImplementedError(f"get_required_capability must be implemented for {cls.__name__}.")

    @classmethod
    @abstractmethod
    def get_id(cls, item: T_Resource) -> T_ID:
        raise NotImplementedError

    # Default implementations that can be overridden
    def create(
        self, items: Sequence[T_Resource], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path
    ) -> T_ResourceList:
        return self.api_class.create(items)

    def delete(self, items: Sequence[T_Resource]) -> None:
        return self.api_class.delete(items)

    def retrieve(self, ids: Sequence[T_ID]) -> T_ResourceList:
        return self.api_class.retrieve(ids)

    def load_file(self, filepath: Path, ToolGlobals: CDFToolConfig) -> T_Resource | T_ResourceList:
        raw_yaml = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(raw_yaml, list):
            return self.list_cls.load(raw_yaml)
        return self.resource_cls.load(raw_yaml)


@final
class TimeSeriesLoader(Loader[str, TimeSeries, TimeSeriesList]):
    api_name = "time_series"
    folder_name = "timeseries"
    resource_cls = TimeSeriesList
    list_cls = TimeSeriesList

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return TimeSeriesAcl(
            [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
            TimeSeriesAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TimeSeriesAcl.Scope.All(),
        )

    def get_id(self, item: TimeSeries) -> str:
        return item.external_id

    def delete(self, ids: Sequence[str]) -> None:
        self.client.time_series.delete(external_id=ids)


@final
class TransformationLoader(Loader[str, Transformation, TransformationList]):
    api_name = "transformations"
    folder_name = "transformations"
    resource_cls = Transformation
    list_cls = TransformationList

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        scope = (
            TransformationsAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TransformationsAcl.Scope.All()
        )
        return TransformationsAcl(
            [TransformationsAcl.Action.Read, TransformationsAcl.Action.Write],
            scope,
        )

    def get_id(self, item: Transformation) -> str:
        return item.external_id

    def load_file(self, filepath: Path, ToolGlobals: CDFToolConfig) -> Transformation:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        # The `authentication` key is custom for this template:
        source_oidc_credentials = raw.get("authentication", {}).get("read") or raw.get("authentication") or {}
        destination_oidc_credentials = raw.get("authentication", {}).get("write") or raw.get("authentication") or {}
        transformation = Transformation.load(raw)
        transformation.source_oidc_credentials = source_oidc_credentials and OidcCredentials.load(
            source_oidc_credentials
        )
        transformation.destination_oidc_credentials = destination_oidc_credentials and OidcCredentials.load(
            destination_oidc_credentials
        )
        sql_file = filepath.parent / f"{transformation.external_id}.sql"
        if not sql_file.exists():
            raise FileNotFoundError(
                f"Could not find sql file {sql_file.name}. Expected to find it next to the yaml config file."
            )
        transformation.query = sql_file.read_text()
        transformation.data_set_id = ToolGlobals.data_set_id
        return transformation

    def delete(self, ids: Sequence[str]) -> None:
        self.client.transformations.delete(external_id=ids, ignore_unknown_ids=True)

    def create(
        self, items: Sequence[Transformation], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path
    ) -> TransformationList:
        created = self.client.transformations.create(items)
        for t in items if isinstance(items, Sequence) else [items]:
            if t.schedule.interval != "":
                t.schedule.external_id = t.external_id
                self.client.transformations.schedules.create(t.schedule)
        return created


@final
class GroupLoader(Loader[int, Group, GroupList]):
    support_drop = False
    api_name = "iam.groups"
    folder_name = "auth"
    resource_cls = Group
    list_cls = GroupList

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return GroupsAcl(
            [GroupsAcl.Action.Read, GroupsAcl.Action.List, GroupsAcl.Action.Create, GroupsAcl.Action.Delete],
            GroupsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: Group) -> int:
        return item.id

    def load_file(self, filepath: Path, ToolGlobals: CDFToolConfig) -> Group:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        for capability in raw.get("capabilities", []):
            for _, values in capability.items():
                if len(values.get("scope", {}).get("datasetScope", {}).get("ids", [])) > 0:
                    values["scope"]["datasetScope"]["ids"] = [
                        ToolGlobals.verify_dataset(ext_id)
                        for ext_id in values.get("scope", {}).get("datasetScope", {}).get("ids", [])
                    ]
        return Group.load(raw)

    def create(self, items: Sequence[Group], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path) -> GroupList:
        created = self.client.iam.groups.create(items)
        old_groups = self.client.iam.groups.list(all=True).data
        created_names = {g.name for g in created}
        to_delete = [g.id for g in old_groups if g.name in created_names]
        self.client.iam.groups.delete(to_delete)
        return created


@final
class DatapointsLoader(Loader[list[str], Path, TimeSeriesList]):
    # Not yet implemented
    support_drop = False
    filetypes = frozenset({"csv", "parquet"})
    api_name = "time_series.data"
    folder_name = "timeseries_datapoints"
    resource_cls = pd.DataFrame

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        scope = (
            TimeSeriesAcl.Scope.DataSet([ToolGlobals.data_set_id])
            if ToolGlobals.data_set_id
            else TimeSeriesAcl.Scope.All()
        )

        return TimeSeriesAcl(
            [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
            scope,
        )

    def load_file(self, filepath: Path, ToolGlobals: CDFToolConfig) -> Path:
        return filepath

    @classmethod
    def get_id(cls, item: Path) -> list[str]:
        raise NotImplementedError

    def delete(self, items: Sequence[str]) -> None:
        # Drop all datapoints?
        raise NotImplementedError()

    def create(self, items: Sequence[Path], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path) -> TimeSeriesList:
        if len(items) != 1:
            raise ValueError("Datapoints must be loaded one at a time.")
        datafile = items[0]
        if datafile.suffix == ".csv":
            data = pd.read_csv(datafile, parse_dates=True, dayfirst=True, index_col=0)
        elif datafile.suffix == ".parquet":
            data = pd.read_parquet(datafile, engine="pyarrow")
        else:
            raise ValueError(f"Unsupported file type {datafile.suffix} for {datafile.name}")
        self.client.time_series.data.insert_dataframe(data)
        external_ids = [col for col in data.columns if not pd.api.types.is_datetime64_any_dtype(data[col])]
        return TimeSeriesList([TimeSeries(external_id=external_id) for external_id in external_ids])


@final
class RawLoader(Loader[RawTable, RawTable, list[RawTable]]):
    api_name = "raw.rows"
    folder_name = "raw"
    resource_cls = RawTable
    list_cls = list[RawTable]
    data_file_types = frozenset({"csv", "parquet"})

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        return RawAcl([RawAcl.Action.Read, RawAcl.Action.Write], RawAcl.Scope.All())

    @classmethod
    def get_id(cls, item: RawTable) -> RawTable:
        return item

    def delete(self, items: Sequence[RawTable]) -> None:
        for db_name, raw_tables in itertools.groupby(sorted(items, key=lambda x: x.db_name), key=lambda x: x.db_name):
            self.client.raw.tables.delete(db_name=db_name, name=[table.table_name for table in raw_tables])

    def create(
        self, items: Sequence[RawTable], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path
    ) -> list[RawTable]:
        if len(items) != 1:
            raise ValueError("Raw tables must be loaded one at a time.")
        table = items[0]
        datafile = next(
            (
                file
                for file_type in self.data_file_types
                if (file := filepath.parent / f"{table.table_name}.{file_type}").exists()
            ),
            None,
        )
        if datafile is None:
            raise ValueError(f"Failed to find data file for {table.table_name} in {filepath.parent}")
        elif datafile.suffix == ".csv":
            # The replacement is used to ensure that we read exactly the same file on Windows and Linux
            file_content = datafile.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
            data = pd.read_csv(io.StringIO(file_content), dtype=str)
            data.fillna("", inplace=True)
        elif datafile.suffix == ".parquet":
            data = pd.read_parquet(filepath)
        else:
            raise NotImplementedError(f"Unsupported file type {datafile.suffix} for {table.table_name}")

        self.client.raw.rows.insert_dataframe(
            db_name=table.db_name,
            table_name=table.table_name,
            dataframe=data,
            ensure_parent=True,
        )
        return [table]


@final
class FileLoader(Loader[str, FileMetadata, FileMetadataList]):
    api_name = "files"
    folder_name = "files"
    resource_cls = FileMetadata
    list_cls = FileMetadataList

    @classmethod
    def get_required_capability(cls, ToolGlobals: CDFToolConfig) -> Capability:
        if ToolGlobals.data_set_id is None:
            scope = FilesAcl.Scope.All()
        else:
            scope = FilesAcl.Scope.DataSet([ToolGlobals.data_set_id])

        return FilesAcl([FilesAcl.Action.Read, FilesAcl.Action.Write], scope)

    @classmethod
    def get_id(cls, item: FileMetadata) -> str:
        return item.external_id

    def delete(self, items: Sequence[FileMetadata]) -> None:
        self.client.files.delete(external_id=[item.external_id for item in items])

    def create(
        self, items: Sequence[FileMetadata], ToolGlobals: CDFToolConfig, drop: bool, filepath: Path
    ) -> FileMetadataList:
        if len(items) != 1:
            raise ValueError("Files must be loaded one at a time.")
        meta = items[0]
        datafile = filepath.parent / meta.name
        created = self.client.files.upload(path=datafile, overwrite=drop, **meta.dump(camel_case=False))
        return FileMetadataList(created)


def drop_load_resources(
    LoaderCls: type[Loader],
    path: Path,
    ToolGlobals: CDFToolConfig,
    drop: bool,
    load: bool = True,
    dry_run: bool = False,
):
    loader = LoaderCls.create_loader(ToolGlobals)
    if path.is_file():
        if path.suffix not in loader.filetypes or not loader.filetypes:
            raise ValueError("Invalid file type")
        filepaths = [path]
    elif loader.filetypes:
        filepaths = [file for type_ in loader.filetypes for file in path.glob(f"**/*.{type_}")]
    else:
        filepaths = [file for file in path.glob("**/*")]

    items = [loader.load_file(f, ToolGlobals) for f in filepaths]
    nr_of_batches = len(items)
    nr_of_items = sum(len(item) if isinstance(item, Sized) else 1 for item in items)
    if load:
        print(f"[bold]Uploading {nr_of_items} {loader.api_name} in {nr_of_batches} batches to CDF...[/]")
    else:
        print(f"[bold]Cleaning {nr_of_items} {loader.api_name} in {nr_of_batches} batches to CDF...[/]")
    batches = [item if isinstance(item, Sized) else [item] for item in items]
    if drop and loader.support_drop and load:
        print(f"  --drop is specified, will delete existing {loader.api_name} before uploading.")
    if drop and loader.support_drop:
        drop_items: list = []
        for batch in batches:
            for item in batch:
                # Set the context info for this CDF project
                if hasattr(item, "data_set_id") and ToolGlobals.data_set_id is not None:
                    item.data_set_id = ToolGlobals.data_set_id
                drop_items.append(loader.get_id(item))
        if not dry_run:
            try:
                loader.delete(drop_items)
                print(f"  Deleted {len(drop_items)} {loader.api_name}.")
            except Exception as e:
                print(
                    f"  [bold yellow]WARNING:[/] Failed to delete {len(drop_items)} {loader.api_name}. It/they may not exist. Error {e}"
                )
        else:
            print(f"  Would have deleted {len(drop_items)} {loader.api_name}.")
    if not load:
        return
    try:
        if not dry_run:
            for batch, filepath in zip(batches, filepaths):
                loader.create(batch, ToolGlobals, drop, filepath)
    except Exception as e:
        print(f"[bold red]ERROR:[/] Failed to upload {loader.api_name}.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"  Created {nr_of_items} {loader.api_name} from {len(filepaths)} files.")


LOADER_BY_FOLDER_NAME = {loader.folder_name: loader for loader in Loader.__subclasses__()}


def load_datamodel_graphql(
    ToolGlobals: CDFToolConfig,
    space_name: str | None = None,
    model_name: str | None = None,
    directory=None,
) -> None:
    """Load a graphql datamodel from file."""
    if space_name is None or model_name is None or directory is None:
        raise ValueError("space_name, model_name, and directory must be supplied.")
    with open(f"{directory}/datamodel.graphql") as file:
        # Read directly into a string.
        datamodel = file.read()
    # Clear any delete errors
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    print(f"[bold]Loading data model {model_name} into space {space_name} from {directory}...[/]")
    try:
        client.data_modeling.graphql.apply_dml(
            (space_name, model_name, "1"),
            dml=datamodel,
            name=model_name,
            description=f"Data model for {model_name}",
        )
    except Exception as e:
        print(f"[bold red]ERROR:[/] Failed to write data model {model_name} to space {space_name}.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"  Created data model {model_name}.")


def load_datamodel(
    ToolGlobals: CDFToolConfig,
    drop: bool = False,
    delete_removed: bool = True,
    delete_containers: bool = False,
    delete_spaces: bool = False,
    directory: Path | None = None,
    dry_run: bool = False,
    only_drop: bool = False,
) -> None:
    """Load containers, views, spaces, and data models from a directory

        Note that this function will never delete instances, but will delete all
        the properties found in containers if delete_containers is specified.
        delete_spaces will fail unless also the edges and nodes have been deleted,
        e.g. using the clean_out_datamodel() function.

        Note that if delete_spaces flag is True, an attempt will be made to delete the space,
        but if it fails, the loading will continue. If delete_containers is True, the loading
        will abort if deletion fails.
    Args:
        drop: Whether to drop all existing resources before loading.
        delete_removed: Whether to delete (previous) resources that are not in the directory.
        delete_containers: Whether to delete containers including data in the instances.
        delete_spaces: Whether to delete spaces (requires containers and instances to be deleted).
        directory: Directory to load from.
        dry_run: Whether to perform a dry run and only print out what will happen.
        only_drop: Whether to only drop existing resources and not load new ones.
    """
    if directory is None:
        raise ValueError("directory must be supplied.")
    model_files_by_type: dict[str, list[Path]] = defaultdict(list)
    models_pattern = re.compile(r"^.*\.?(space|container|view|datamodel)\.yaml$")
    for file in directory.rglob("*.yaml"):
        if not (match := models_pattern.match(file.name)):
            continue
        model_files_by_type[match.group(1)].append(file)
    print("[bold]Loading data model files...[/]")
    for type_, files in model_files_by_type.items():
        model_files_by_type[type_].sort()
        print(f"  {len(files)} of type {type_}s in {directory}")

    cognite_resources_by_type: dict[str, list[ContainerApply | ViewApply | DataModelApply | SpaceApply]] = defaultdict(
        list
    )
    for type_, files in model_files_by_type.items():
        resource_cls = {
            "space": SpaceApply,
            "container": ContainerApply,
            "view": ViewApply,
            "datamodel": DataModelApply,
        }[type_]
        for file in files:
            cognite_resources_by_type[type_].append(
                resource_cls.load(load_yaml_inject_variables(file, ToolGlobals.environment_variables()))
            )
    # Remove duplicates
    for type_ in list(cognite_resources_by_type):
        unique = {r.as_id(): r for r in cognite_resources_by_type[type_]}
        cognite_resources_by_type[type_] = list(unique.values())

    explicit_space_list = [s.space for s in cognite_resources_by_type["space"]]
    space_list = list({r.space for _, resources in cognite_resources_by_type.items() for r in resources})

    implicit_spaces = [SpaceApply(space=s, name=s, description="Imported space") for s in space_list]
    for s in implicit_spaces:
        if s.name not in [s2.name for s2 in cognite_resources_by_type["space"]]:
            cognite_resources_by_type["space"].append(s)
    nr_of_spaces = len(cognite_resources_by_type["space"])
    print(f"  found {len(implicit_spaces)} space(s) referenced in config files giving a total of {nr_of_spaces}")
    # Clear any delete errors
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )

    existing_resources_by_type: dict[str, list[ContainerApply | ViewApply | DataModelApply | SpaceApply]] = defaultdict(
        list
    )
    resource_api_by_type = {
        "container": client.data_modeling.containers,
        "view": client.data_modeling.views,
        "datamodel": client.data_modeling.data_models,
        "space": client.data_modeling.spaces,
    }
    for type_, resources in cognite_resources_by_type.items():
        existing_resources_by_type[type_] = resource_api_by_type[type_].retrieve(list({r.as_id() for r in resources}))

    differences: dict[str, Difference] = {}
    for type_, resources in cognite_resources_by_type.items():
        new_by_id = {r.as_id(): r for r in resources}
        existing_by_id = {r.as_id(): r for r in existing_resources_by_type[type_]}

        added = [r for r in resources if r.as_id() not in existing_by_id]
        removed = [r for r in existing_resources_by_type[type_] if r.as_id() not in new_by_id]

        changed = []
        unchanged = []
        for existing_id in set(new_by_id.keys()) & set(existing_by_id.keys()):
            if new_by_id[existing_id] == existing_by_id[existing_id]:
                unchanged.append(new_by_id[existing_id])
            else:
                changed.append(new_by_id[existing_id])

        differences[type_] = Difference(added, removed, changed, unchanged)

    creation_order = ["space", "container", "view", "datamodel"]

    if drop:
        print("[bold]Deleting existing configurations...[/]")
        # Clean out all old resources
        for type_ in reversed(creation_order):
            items = differences.get(type_)
            if items is None:
                continue
            if type_ == "container" and not delete_containers:
                print("  [bold]INFO:[/] Skipping deletion of containers as delete_containers flag is not set...")
                continue
            if type_ == "space" and not delete_spaces:
                print("  [bold]INFO:[/] Skipping deletion of spaces as delete_spaces flag is not set...")
                continue
            deleted = 0
            for i in items:
                if len(i) == 0:
                    continue
                # for i2 in i:
                try:
                    if not dry_run:
                        if type_ == "space":
                            for i2 in i:
                                # Only delete spaces that have been explicitly defined
                                if i2.space in explicit_space_list:
                                    delete_instances(
                                        ToolGlobals,
                                        space_name=i2.space,
                                        dry_run=dry_run,
                                    )
                                    ret = resource_api_by_type["space"].delete(i2.space)
                                    if len(ret) > 0:
                                        deleted += 1
                        else:
                            ret = resource_api_by_type[type_].delete([i2.as_id() for i2 in i])
                            deleted += len(ret)
                except CogniteAPIError as e:
                    # Typically spaces can not be deleted if there are other
                    # resources in the space.
                    print(f"  [bold]ERROR:[/] Failed to delete {type_}(s):\n{e}")
                    if type_ == "space":
                        ToolGlobals.failed = False
                        print("  [bold]INFO:[/] Deletion of space was not successful, continuing.")
                        continue
                    return
            if not dry_run:
                print(f"  Deleted {deleted} {type_}(s).")
            else:
                print(f"  Would have deleted {deleted} {type_}(s).")

    if not only_drop:
        print("[bold]Creating new configurations...[/]")
        for type_ in creation_order:
            if type_ not in differences:
                continue
            items = differences[type_]
            if items.added:
                print(f"  {len(items.added)} added {type_}(s) to be created...")
                if dry_run:
                    continue
                attempts = 5
                while attempts > 0:
                    try:
                        resource_api_by_type[type_].apply(items.added)
                        attempts = 0
                    except Exception as e:
                        attempts -= 1
                        if attempts > 0:
                            continue
                        print(f"[bold]ERROR:[/] Failed to create {type_}(s):\n{e}")
                        ToolGlobals.failed = True
                        return
                print(f"  Created {len(items.added)} {type_}(s).")
            elif items.changed:
                print(f"  {len(items.added)} changed {type_}(s) to be created...")
                if dry_run:
                    continue
                attempts = 5
                while attempts > 0:
                    try:
                        resource_api_by_type[type_].apply(items.changed)
                        attempts = 0
                    except Exception as e:
                        attempts -= 1
                        if attempts > 0:
                            continue
                        print(f"[bold]ERROR:[/] Failed to create {type_}(s):\n{e}")
                        ToolGlobals.failed = True
                        return
                if drop:
                    print(
                        f"  Created {len(items.changed)} {type_}s that could have been updated instead (--drop specified)."
                    )
                else:
                    print(f"  Updated {len(items.changed)} {type_}(s).")
            elif items.unchanged:
                print(f"  {len(items.unchanged)} unchanged {type_}(s).")
                if drop:
                    attempts = 5
                    while attempts > 0:
                        try:
                            resource_api_by_type[type_].apply(items.unchanged)
                            attempts = 0
                        except Exception as e:
                            attempts -= 1
                            if attempts > 0:
                                continue
                            print(f"[bold]ERROR:[/] Failed to create {type_}(s):\n{e}")
                            ToolGlobals.failed = True
                            return
                    print(
                        f"  Created {len(items.changed)} unchanged {type_}(s) that could have been skipped (--drop specified)."
                    )

    if delete_removed and not drop:
        for type_ in reversed(creation_order):
            if type_ not in differences:
                continue
            items = differences[type_]
            if items.removed:
                if dry_run:
                    print(f"  Would have deleted {len(items.removed)} {type_}(s).")
                    continue
                try:
                    resource_api_by_type[type_].delete(items.removed)
                except CogniteAPIError as e:
                    # Typically spaces can not be deleted if there are other
                    # resources in the space.
                    print(f"[bold]ERROR:[/] Failed to delete {len(items.removed)} {type_}(s).")
                    print(e)
                    ToolGlobals.failed = True
                    continue
                print(f"  Deleted {len(items.removed)} {type_}(s) that were removed.")


def load_nodes(
    ToolGlobals: CDFToolConfig,
    directory: Path | None = None,
    dry_run: bool = False,
) -> None:
    """Insert nodes"""

    for file in directory.rglob("*.node.yaml"):
        if file.name == "config.yaml":
            continue

        client: CogniteClient = ToolGlobals.verify_client(
            capabilities={
                "dataModelsAcl": ["READ"],
                "dataModelInstancesAcl": ["READ", "WRITE"],
            }
        )

        nodes: dict = load_yaml_inject_variables(file, ToolGlobals.environment_variables())

        try:
            view = ViewId(
                space=nodes["view"]["space"],
                external_id=nodes["view"]["externalId"],
                version=nodes["view"]["version"],
            )
        except KeyError:
            raise KeyError(
                f"Expected view configuration not found in {file}:\nview:\n  space: <space>\n  externalId: <view_external_id>\n  version: <view_version>"
            )

        try:
            node_space: str = nodes["destination"]["space"]
        except KeyError:
            raise KeyError(
                f"Expected destination space configuration in {file}:\ndestination:\n  space: <destination_space_external_id>"
            )
        node_list: NodeApplyList = []
        try:
            for n in nodes.get("nodes", []):
                node_list.append(
                    NodeApply(
                        space=node_space,
                        external_id=n.pop("externalId"),
                        existing_version=n.pop("existingVersion", None),
                        sources=[NodeOrEdgeData(source=view, properties=n)],
                    )
                )
        except Exception as e:
            raise KeyError(f"Failed to parse node {n} in {file}:\n{e}")
        print(f"[bold]Loading {len(node_list)} nodes from {directory}...[/]")
        if not dry_run:
            try:
                client.data_modeling.instances.apply(
                    nodes=node_list,
                    auto_create_direct_relations=nodes.get("autoCreateDirectRelations", True),
                    skip_on_version_conflict=nodes.get("skipOnVersionConflict", False),
                    replace=nodes.get("replace", False),
                )
                print(f"  Created {len(node_list)} nodes in {node_space}.")
            except CogniteAPIError as e:
                print(f"[bold]ERROR:[/] Failed to create {len(node_list)} node(s) in {node_space}:\n{e}")
                ToolGlobals.failed = True
                return
