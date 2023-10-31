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

import os
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    OidcCredentials,
    Transformation,
    TransformationList,
    TransformationSchedule,
)
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling import (
    ContainerApply,
    DataModelApply,
    SpaceApply,
    ViewApply,
)
from cognite.client.data_classes.iam import Group
from cognite.client.data_classes.time_series import TimeSeries
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

from .delete import delete_instances
from .utils import CDFToolConfig, GroupLoad, TimeSeriesLoad


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


def load_raw(
    ToolGlobals: CDFToolConfig,
    file: str,
    raw_db: str = "default",
    drop: bool = False,
    dry_run: bool = False,
    directory=None,
) -> None:
    """Load raw data from csv files into CDF Raw

    Args:
        file: name of file to load, if empty load all files
        drop: whether to drop existing data
    """
    if directory is None:
        raise ValueError("directory must be specified")
    client: CogniteClient = ToolGlobals.verify_client(capabilities={"rawAcl": ["READ", "WRITE"]})

    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .csv files in the data folder.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".csv" in f:
                    files.append(f)
    files.sort()
    if len(files) == 0:
        return
    print(f"Uploading {len(files)} .csv files to RAW database using {raw_db} if not set in filename...")
    for f in files:
        try:
            (_, db, table_name) = re.match(r"(\d+)\.(\w+)\.(\w+)\.csv", f).groups()
        except Exception:
            db = raw_db
        with open(f"{directory}/{f}") as file:
            dataframe = pd.read_csv(file, dtype=str)
            dataframe = dataframe.fillna("")
            try:
                if not dry_run:
                    if drop:
                        try:
                            client.raw.tables.delete(db, table_name)
                        except Exception:
                            ...
                    try:
                        client.raw.databases.create(db)
                        print("Created database: " + db)
                    except Exception:
                        ...
                    client.raw.rows.insert_dataframe(
                        db_name=db,
                        table_name=table_name,
                        dataframe=dataframe,
                        ensure_parent=True,
                    )
                    print("Deleted table: " + table_name)
                    print(f"Uploaded {f} to {db} RAW database.")
                else:
                    print("Would have deleted table: " + table_name)
                    print(f"Would have uploaded {f} to {db} RAW database.")
            except Exception as e:
                print(f"Failed to upload {f}")
                print(e)
                ToolGlobals.failed = True
                return


def load_files(
    ToolGlobals: CDFToolConfig,
    id_prefix: str = "example",
    file: Optional[str] = None,
    drop: bool = False,
    dry_run: bool = False,
    directory=None,
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    try:
        client = ToolGlobals.verify_client(capabilities={"filesAcl": ["READ", "WRITE"]})
        files = []
        if file is not None and len(file) > 0:
            files.append(file)
        else:
            # Pick up all the files in the files folder.
            for _, _, filenames in os.walk(directory):
                for f in filenames:
                    files.append(f)
        if len(files) == 0:
            return
        print(f"Uploading {len(files)} files/documents to CDF...")
        for f in files:
            if not dry_run:
                client.files.upload(
                    path=f"{directory}/{f}",
                    data_set_id=ToolGlobals.data_set_id,
                    name=f,
                    external_id=id_prefix + "_" + f,
                    overwrite=drop,
                )
        if not dry_run:
            print(f"Uploaded successfully {len(files)} files/documents.")
        else:
            print(f"Would have uploaded {len(files)} files/documents.")
    except Exception as e:
        print("Failed to upload files")
        print(e)
        ToolGlobals.failed = True
        return


def load_timeseries(
    ToolGlobals: CDFToolConfig,
    file: str,
    drop: bool = False,
    dry_run: bool = False,
    directory: Optional[str] = None,
) -> None:
    load_timeseries_metadata(ToolGlobals, file, drop, dry_run=dry_run, directory=directory)
    if directory is not None:
        directory = f"{directory}/datapoints"
    load_timeseries_datapoints(ToolGlobals, file, dry_run=dry_run, directory=directory)


def load_timeseries_metadata(
    ToolGlobals: CDFToolConfig,
    file: str,
    drop: bool,
    dry_run: bool = False,
    directory=None,
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(capabilities={"timeSeriesAcl": ["READ", "WRITE"]})
    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .yaml files in the data folder.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".yaml" in f:
                    files.append(f)
    # Read timeseries metadata
    timeseries: list[TimeSeries] = []
    for f in files:
        with open(f"{directory}/{f}") as file:
            timeseries.extend(
                TimeSeriesLoad.load(yaml.safe_load(file.read()), file=f"{directory}/{f}"),
            )
    if len(timeseries) == 0:
        return
    drop_ts: list[str] = []
    for t in timeseries:
        # Set the context info for this CDF project
        t.data_set_id = ToolGlobals.data_set_id
        if drop:
            drop_ts.append(t.external_id)
    try:
        if drop:
            if not dry_run:
                client.time_series.delete(external_id=drop_ts, ignore_unknown_ids=True)
                print(f"Deleted {len(drop_ts)} timeseries.")
            else:
                print(f"Would have deleted {len(drop_ts)} timeseries.")
    except Exception:
        print(f"Failed to delete {t.external_id}. It may not exist.")
    try:
        if not dry_run:
            client.time_series.create(timeseries)
        else:
            print(f"Would have created {len(timeseries)} timeseries.")
    except Exception as e:
        print("Failed to upload timeseries.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"Created {len(timeseries)} timeseries from {len(files)} files.")


def load_timeseries_datapoints(ToolGlobals: CDFToolConfig, file: str, dry_run: bool = False, directory=None) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(capabilities={"timeseriesAcl": ["READ", "WRITE"]})
    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .csv files in the data folder.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".csv" in f:
                    files.append(f)
    if len(files) == 0:
        return
    print(f"Uploading {len(files)} .csv file(s) as datapoints to CDF timeseries...")
    try:
        for f in files:
            with open(f"{directory}/{f}") as file:
                dataframe = pd.read_csv(file, parse_dates=True, index_col=0)
            if not dry_run:
                print(f"Uploading {f} as datapoints to CDF timeseries...")
                client.time_series.data.insert_dataframe(dataframe)
            else:
                print(f"Would have uploaded {f} as datapoints to CDF timeseries...")
        if not dry_run:
            print(f"Uploaded {len(files)} .csv file(s) as datapoints to CDF timeseries.")
        else:
            print(f"Would have uploaded {len(files)} .csv file(s) as datapoints to CDF timeseries.")
    except Exception as e:
        print("Failed to upload datapoints.")
        print(e)
        ToolGlobals.failed = True
        return


def load_transformations(
    ToolGlobals: CDFToolConfig,
    file: Optional[str] = None,
    drop: bool = False,
    dry_run: bool = False,
    directory: Optional[str] = None,
) -> None:
    """Load transformations from dump folder.

    This code only gives a partial support for transformations by loading the actual sql query and the
    necessary config. Schedules, authentication, etc is not supported.
    """
    if directory is None:
        raise ValueError("directory must be specified")
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "transformationsAcl": ["READ", "WRITE"],
            "sessionsAcl": ["CREATE"],
        }
    )
    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .yaml files in the data folder.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".yaml" in f:
                    files.append(f)
    transformations: TransformationList = []
    for f in files:
        with open(f"{directory}/{f}") as file:
            config = yaml.safe_load(file.read())
            source_oidc_credentials = config.get("authentication", {}).get("read") or config.get("authentication") or {}
            destination_oidc_credentials = (
                config.get("authentication", {}).get("write") or config.get("authentication") or {}
            )
            tmp = Transformation._load(config, ToolGlobals.client)
            transformations.append(
                Transformation(
                    id=tmp.id,
                    external_id=tmp.external_id,
                    name=tmp.name,
                    query=tmp.query,
                    destination=tmp.destination,
                    conflict_mode=tmp.conflict_mode,
                    is_public=tmp.is_public,
                    ignore_null_fields=tmp.ignore_null_fields,
                    source_oidc_credentials=OidcCredentials(
                        client_id=source_oidc_credentials.get("clientId", ""),
                        client_secret=source_oidc_credentials.get("clientSecret", ""),
                        audience=source_oidc_credentials.get("audience", ""),
                        scopes=ToolGlobals.oauth_credentials.scopes,
                        token_uri=ToolGlobals.oauth_credentials.token_url,
                        cdf_project_name=ToolGlobals.client.config.project,
                    ),
                    destination_oidc_credentials=OidcCredentials(
                        client_id=destination_oidc_credentials.get("clientId", ""),
                        client_secret=destination_oidc_credentials.get("clientSecret", ""),
                        audience=source_oidc_credentials.get("audience", ""),
                        scopes=ToolGlobals.oauth_credentials.scopes,
                        token_uri=ToolGlobals.oauth_credentials.token_url,
                        cdf_project_name=ToolGlobals.client.config.project,
                    ),
                    schedule=TransformationSchedule(
                        external_id=tmp.external_id,
                        interval=config.get("schedule", {}).get("interval", ""),
                    ),
                    has_source_oidc_credentials=(len(source_oidc_credentials) > 0),
                    has_destination_oidc_credentials=(len(destination_oidc_credentials) > 0),
                    data_set_id=tmp.data_set_id,
                )
            )
    print(f"Found {len(transformations)} transformations in {directory}.")
    ext_ids = [t.external_id for t in transformations]
    try:
        if drop:
            if not dry_run:
                client.transformations.delete(external_id=ext_ids, ignore_unknown_ids=True)
                print(f"Deleted {len(ext_ids)} transformations.")
            else:
                print(f"Would have deleted {len(ext_ids)} transformations.")
    except CogniteNotFoundError:
        pass
    for t in transformations:
        with open(f"{directory}/{t.external_id}.sql") as file:
            t.query = file.read()
            t.data_set_id = ToolGlobals.data_set_id
    try:
        if not dry_run:
            client.transformations.create(transformations)
            for t in transformations:
                if t.schedule.interval != "":
                    client.transformations.schedules.create(t.schedule)
            print(f"Created {len(transformations)} transformation.")
        else:
            print(f"Would have created {len(transformations)} transformation.")
    except Exception as e:
        print("Failed to create transformations.")
        print(e)
        ToolGlobals.failed = True


def load_groups(
    ToolGlobals: CDFToolConfig,
    file: Optional[str] = None,
    directory: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(capabilities={"groupsAcl": ["LIST", "READ", "CREATE", "DELETE"]})
    try:
        old_groups = client.iam.groups.list(all=True).data
    except Exception:
        print("Failed to retrieve groups.")
        ToolGlobals.failed = True
        return
    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .yaml files in the folder.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".yaml" in f:
                    files.append(f)
    groups: list[Group] = []
    for f in files:
        with open(f"{directory}/{f}") as file:
            groups.extend(
                GroupLoad.load(yaml.safe_load(file.read()), file=f"{directory}/{f}"),
            )
    # Find and create data_sets
    for group in groups:
        for capability in group.capabilities:
            for _, actions in capability.items():
                data_set_ext_ids = actions.get("scope", {}).get("datasetScope", {}).get("ids", [])
                if len(data_set_ext_ids) == 0:
                    continue
                ids = []
                for ext_id in data_set_ext_ids:
                    ids.append(ToolGlobals.verify_dataset(ext_id))
                actions["scope"]["datasetScope"]["ids"] = ids
    for group in groups:
        old_group_id = None
        for g in old_groups:
            if g.name == group.name:
                old_group_id = g.id
                break
        try:
            if not dry_run:
                group = client.iam.groups.create(group)
                print(f"Created group {group.name}.")
            else:
                print(f"Would have created group {group.name}.")
        except Exception as e:
            print(f"Failed to create group {group.name}: \n{e}")
            ToolGlobals.failed = True
            return
        if old_group_id:
            try:
                if not dry_run:
                    client.iam.groups.delete(id=old_group_id)
                    print(f"Deleted old group {old_group_id}.")
                else:
                    print(f"Would have deleted group {old_group_id}.")
            except Exception:
                print(f"Failed to delete group {old_group_id}.")
                ToolGlobals.failed = True


def load_datamodel_graphql(
    ToolGlobals: CDFToolConfig,
    space_name: Optional[str] = None,
    model_name: Optional[str] = None,
    directory=None,
) -> None:
    """Load a graphql datamode from file."""
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
    try:
        client.data_modeling.spaces.apply(
            SpaceApply(
                space=space_name,
                name=space_name,
                description=f"Space for {model_name}",
            )
        )
    except Exception as e:
        print(f"Failed to write space {space_name}.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"Created space {space_name}.")
    try:
        client.data_modeling.graphql.apply_dml(
            (space_name, model_name, "1"),
            dml=datamodel,
            name=model_name,
            description=f"Data model for {model_name}",
        )
    except Exception as e:
        print(f"Failed to write data model {model_name} to space {space_name}.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"Created data model {model_name}.")


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
    for type_, files in model_files_by_type.items():
        model_files_by_type[type_].sort()
        print(f"Found {len(files)} {type_}s in {directory}.")

    cognite_resources_by_type: dict[
        str, list[Union[ContainerApply, ViewApply, DataModelApply, SpaceApply]]
    ] = defaultdict(list)
    for type_, files in model_files_by_type.items():
        resource_cls = {
            "space": SpaceApply,
            "container": ContainerApply,
            "view": ViewApply,
            "datamodel": DataModelApply,
        }[type_]
        for file in files:
            print(f"  loading {file}...")
            cognite_resources_by_type[type_].append(resource_cls.load(yaml.safe_load(file.read_text())))
    print("Loaded from files: ")
    for type_, resources in cognite_resources_by_type.items():
        print(f"  {type_}: {len(resources)}")

    explicit_space_list = [s.space for s in cognite_resources_by_type["space"]]
    space_list = list({r.space for _, resources in cognite_resources_by_type.items() for r in resources})

    print(f"Found {len(space_list)} implicit space(s) in container, view, and data model files.")
    implicit_spaces = [SpaceApply(space=s, name=s, description="Imported space") for s in space_list]
    for s in implicit_spaces:
        if s.name not in [s2.name for s2 in cognite_resources_by_type["space"]]:
            cognite_resources_by_type["space"].append(s)
    print(f"Total number of space(s):  {len(space_list)}")
    # Clear any delete errors
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )

    existing_resources_by_type: dict[
        str, list[Union[ContainerApply, ViewApply, DataModelApply, SpaceApply]]
    ] = defaultdict(list)
    resource_api_by_type = {
        "container": client.data_modeling.containers,
        "view": client.data_modeling.views,
        "datamodel": client.data_modeling.data_models,
        "space": client.data_modeling.spaces,
    }
    for type_, resources in cognite_resources_by_type.items():
        existing_resources_by_type[type_] = resource_api_by_type[type_].retrieve([r.as_id() for r in resources])

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
        # Clean out all old resources
        for type_ in reversed(creation_order):
            items = differences.get(type_)
            if items is None:
                continue
            if type_ == "container" and not delete_containers:
                print("Skipping deletion of containers as delete_containers flag is not set.")
                continue
            if type_ == "space" and not delete_spaces:
                print("Skipping deletion of spaces as delete_spaces flag is not set.")
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
                                        print(f"  -- deleted {len(ret)} of {type_}.")
                        else:
                            ret = resource_api_by_type[type_].delete(i)
                            if len(ret) > 0:
                                print(f"  -- deleted {len(ret)} of {type_}.")
                            deleted += len(ret)
                except CogniteAPIError as e:
                    # Typically spaces can not be deleted if there are other
                    # resources in the space.
                    print(f"  Failed to delete {type_}(s):\n{e}")
                    print(e)
                    if type_ == "space":
                        ToolGlobals.failed = False
                        print("  Deletion of space was not successful, continuing.")
                        continue
                    return
            if not dry_run:
                print(f"  Deleted {deleted} {type_}(s).")
            else:
                print(f"  Would have deleted {deleted} {type_}(s).")

    if not only_drop:
        # For apply, we want to restrict number of workers to avoid concurrency issues.
        ToolGlobals.client.config.max_workers = 1
        for type_ in creation_order:
            if type_ not in differences:
                continue
            items = differences[type_]
            if items.added:
                print(f"Found {len(items.added)} new {type_}s.")
                if dry_run:
                    print(f"  Would have created {len(items.added)} {type_}(s).")
                    continue
                for i in items.added:
                    resource_api_by_type[type_].apply(i)
                print(f"  Created {len(items.added)} {type_}s.")
            if items.changed:
                if dry_run:
                    print(f"  Would have created/updated {len(items.changed)} {type_}(s).")
                    continue
                for i in items.changed:
                    resource_api_by_type[type_].apply(i.changed)
                if drop:
                    print(f"  Created {len(items.changed)} {type_}s (--drop specified).")
                else:
                    print(f"  Updated {len(items.changed)} {type_}s.")
            if items.unchanged:
                print(f"Found {len(items.unchanged)} unchanged {type_}(s).")
                if drop:
                    for i in items.unchanged:
                        resource_api_by_type[type_].apply(i)
                    print(f"  Created {len(items.changed)} unchanged {type_}s (--drop specified).")

    if delete_removed and not drop:
        for type_ in reversed(creation_order):
            if type_ not in differences:
                continue
            items = differences[type_]
            if items.removed:
                print(f"Found {len(items.removed)} removed {type_}s.")
                if dry_run:
                    print(f"  Would have deleted {len(items.removed)} {type_}(s).")
                    continue
                try:
                    resource_api_by_type[type_].delete(items.removed)
                except CogniteAPIError as e:
                    # Typically spaces can not be deleted if there are other
                    # resources in the space.
                    print(f"  Failed to delete {len(items.removed)} {type_}(s).")
                    print(e)
                    ToolGlobals.failed = True
                    continue
                print(f"  Deleted {len(items.removed)} {type_}(s).")
