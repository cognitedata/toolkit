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
import yaml
import pandas as pd
import json
from collections import defaultdict
from pathlib import Path
from typing import Union
from dataclasses import dataclass
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.time_series import TimeSeries
from cognite.client.data_classes.iam import Group
from cognite.client.data_classes import (
    Transformation,
    TransformationList,
    TransformationDestination,
)
from cognite.client.data_classes.transformations.common import DataModelInfo
from cognite.client.data_classes.data_modeling import (
    ViewApply,
    SpaceApply,
    ContainerApply,
    DataModelApply,
)
from cognite.client.exceptions import CogniteAPIError
from .utils import CDFToolConfig
from scripts.transformations_config import parse_transformation_configs
from scripts.transformations_api import (
    to_transformation,
    get_existing_transformation_ext_ids,
    get_new_transformation_ids,
    upsert_transformations,
)
from cognite.client.exceptions import CogniteNotFoundError


@dataclass
class Difference:
    added: list[CogniteResource]
    removed: list[CogniteResource]
    changed: list[CogniteResource]
    unchanged: list[CogniteResource]


class TimeSeriesLoad:
    @staticmethod
    def load(props: list[dict], file: str = "unknown") -> [TimeSeries]:
        try:
            return [TimeSeries(**prop) for prop in props]
        except Exception as e:
            raise ValueError(f"Failed to load timeseries from yaml files: {file}.\n{e}")


class GroupLoad:
    @staticmethod
    def load(props: list[dict], file: str = "unknown") -> [Group]:
        try:
            return [
                Group(
                    name=props.get("name"),
                    source_id=props.get("source_id"),
                    capabilities=props.get("capabilities"),
                    metadata=props.get("metadata"),
                )
            ]
        except Exception as e:
            raise ValueError(f"Failed to load group from yaml files: {file}.\n{e}")


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
    client = ToolGlobals.verify_client(capabilities={"rawAcl": ["READ", "WRITE"]})

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
    print(
        f"Uploading {len(files)} .csv files to RAW database using {raw_db} if not set in filename..."
    )
    for f in files:
        try:
            (_, db, _) = re.match(r"(\d+)\.(\w+)\.(\w+)\.csv", f).groups()
        except:
            db = raw_db
        with open(f"{directory}/{f}", "rt") as file:
            dataframe = pd.read_csv(file, dtype=str)
            dataframe = dataframe.fillna("")
            try:
                if not dry_run:
                    if drop:
                        client.raw.tables.delete(db, f[:-4])
                    client.raw.rows.insert_dataframe(
                        db_name=db,
                        table_name=f[:-4],
                        dataframe=dataframe,
                        ensure_parent=True,
                    )
                    print("Deleted table: " + f[:-4])
                    print(f"Uploaded {f} to {db} RAW database.")
                else:
                    print("Would have deleted table: " + f[:-4])
                    print(f"Would have uploaded {f} to {db} RAW database.")
            except Exception as e:
                print(f"Failed to upload {f}")
                print(e)
                ToolGlobals.failed = True
                return


def load_files(
    ToolGlobals: CDFToolConfig,
    id_prefix: str = "example",
    file: str = None,
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
        print(f"Failed to upload files")
        print(e)
        ToolGlobals.failed = True
        return


def load_timeseries(
    ToolGlobals: CDFToolConfig,
    file: str,
    drop: bool = False,
    dry_run: bool = False,
    directory: str = None,
) -> None:
    load_timeseries_metadata(
        ToolGlobals, file, drop, dry_run=dry_run, directory=directory
    )
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
    client = ToolGlobals.verify_client(
        capabilities={"timeseriesAcl": ["READ", "WRITE"]}
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
    # Read timeseries metadata
    timeseries: list[TimeSeries] = []
    for f in files:
        with open(f"{directory}/{f}", "rt") as file:
            timeseries.extend(
                TimeSeriesLoad.load(
                    yaml.safe_load(file.read()), file=f"{directory}/{f}"
                ),
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
    except Exception as e:
        print(f"Failed to delete {t.external_id}. It may not exist.")
    try:
        if not dry_run:
            client.time_series.create(timeseries)
        else:
            print(f"Would have created {len(timeseries)} timeseries.")
    except Exception as e:
        print(f"Failed to upload timeseries.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"Created {len(timeseries)} timeseries from {len(files)} files.")


def load_timeseries_datapoints(
    ToolGlobals: CDFToolConfig, file: str, dry_run: bool = False, directory=None
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(
        capabilities={"timeseriesAcl": ["READ", "WRITE"]}
    )
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
            with open(f"{directory}/{f}", "rt") as file:
                dataframe = pd.read_csv(file, parse_dates=True, index_col=0)
            if not dry_run:
                print(f"Uploading {f} as datapoints to CDF timeseries...")
                client.time_series.data.insert_dataframe(dataframe)
            else:
                print(f"Would have uploaded {f} as datapoints to CDF timeseries...")
        if not dry_run:
            print(
                f"Uploaded {len(files)} .csv file(s) as datapoints to CDF timeseries."
            )
        else:
            print(
                f"Would have uploaded {len(files)} .csv file(s) as datapoints to CDF timeseries."
            )
    except Exception as e:
        print(f"Failed to upload datapoints.")
        print(e)
        ToolGlobals.failed = True
        return


def load_transformations(
    ToolGlobals: CDFToolConfig,
    file: str,
    drop: bool,
    dry_run: bool = False,
    directory=None,
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(
        capabilities={"transformationsAcl": ["READ", "WRITE"]}
    )
    tmp = ""
    if file:
        # Only load the supplied filename.
        os.mkdir(f"{directory}/tmp")
        os.system(f"cp {directory}/{file} {directory}/tmp/")
        tmp = "tmp/"
    configs = parse_transformation_configs(f"{directory}/{tmp}")
    if len(tmp) > 0:
        os.system(f"rm -rf {directory}/tmp")
    cluster = ToolGlobals.environ("CDF_CLUSTER")
    transformations = [
        to_transformation(client, conf_path, configs[conf_path], cluster)
        for conf_path in configs
    ]
    transformations_ext_ids = [t.external_id for t in configs.values()]
    try:
        if drop:
            if not dry_run:
                client.transformations.delete(external_id=transformations_ext_ids)
            else:
                print(
                    f"Would have deleted {len(transformations_ext_ids)} transformations."
                )
    except CogniteNotFoundError:
        pass
    try:
        existing_transformations_ext_ids = get_existing_transformation_ext_ids(
            client, transformations_ext_ids
        )
        new_transformation_ext_ids = get_new_transformation_ids(
            transformations_ext_ids, existing_transformations_ext_ids
        )
        if not dry_run:
            (
                _,
                updated_transformations,
                created_transformations,
            ) = upsert_transformations(
                client,
                transformations,
                existing_transformations_ext_ids,
                new_transformation_ext_ids,
            )
        else:
            print(
                f"Would have updated and created {len(transformations)} transformations."
            )
    except Exception as e:
        print(f"Failed to upsert transformations.")
        print(e)
        ToolGlobals.failed = True
        return
    if not dry_run:
        print(f"Updated {len(updated_transformations)} transformations.")
        print(f"Created {len(created_transformations)} transformations.")


def load_transformations_dump(
    ToolGlobals: CDFToolConfig,
    file: str = None,
    drop: bool = False,
    dry_run: bool = False,
    directory: str = None,
) -> None:
    """Load transformations from dump folder.

    This code only gives a partial support for transformations by loading the actual sql query and the
    necessary config. Schedules, authentication, etc is not supported.
    """
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(
        capabilities={"transformationsAcl": ["READ", "WRITE"]}
    )
    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .json files in the data folder.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".json" in f:
                    files.append(f)
    transformations: TransformationList = []
    for f in files:
        with open(f"{directory}/{f}", "rt") as file:
            tf = json.load(file)
            if type(tf) == dict:
                tf = [tf]
            # This code basically runs through the propertoes of the Transformation class and
            # converts the nested dicts into the right objects.
            # Full serialization and deserialization of the entire transformations API is not supported.
            for t in tf:
                ta = Transformation()
                for k, v in t.items():
                    if k == "destination":
                        # The destination dict is converted into a TransformationDestination object.
                        ta.destination = TransformationDestination(v["type"])
                        v.pop("type")
                        for k2, v2 in v.items():
                            # The data_model dict is converted into a DataModelInfo object.
                            if k2 == "data_model":
                                ta.destination.data_model = DataModelInfo(
                                    space=v2.get("space"),
                                    version=v2.get("version"),
                                    external_id=v2.get("external_id"),
                                    destination_type=v2.get("destination_type"),
                                )
                            else:
                                # Here we just pick up the values for each attribute in the dict and sets
                                # the corresponding property on the object.
                                ta.destination.__setattr__(k2, v2)
                    else:
                        # Here we just pick up the values for each attribute in the dict and sets
                        # the corresponding property on the object.
                        ta.__setattr__(k, v)
                transformations.append(ta)
    ext_ids = [t.external_id for t in transformations]
    print(f"Found {len(transformations)} transformations in {directory}.")
    try:
        if drop:
            if not dry_run:
                client.transformations.delete(
                    external_id=ext_ids, ignore_unknown_ids=True
                )
                print(f"Deleted {len(ext_ids)} transformations.")
            else:
                print(f"Would have deleted {len(ext_ids)} transformations.")
    except CogniteNotFoundError:
        pass
    for t in transformations:
        with open(f"{directory}/{t.external_id}.sql", "rt") as file:
            t.query = file.read()
            t.data_set_id = ToolGlobals.data_set_id
    try:
        if not dry_run:
            client.transformations.create(transformations)
            print(f"Created {len(transformations)} transformation.")
        else:
            print(f"Would have created {len(transformations)} transformation.")
    except Exception as e:
        print(f"Failed to create transformations.")
        print(e)
        ToolGlobals.failed = True


def load_groups(
    ToolGlobals: CDFToolConfig,
    file: str = None,
    directory: str = None,
    dry_run: bool = False,
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(
        capabilities={"groupsAcl": ["LIST", "READ", "CREATE", "DELETE"]}
    )
    try:
        old_groups = client.iam.groups.list(all=True).data
    except Exception as e:
        print(f"Failed to retrieve groups.")
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
        with open(f"{directory}/{f}", "rt") as file:
            groups.extend(
                GroupLoad.load(yaml.safe_load(file.read()), file=f"{directory}/{f}"),
            )

    for group in groups:
        old_group_id = None
        for g in old_groups:
            if g.source_id == group.source_id:
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
            except Exception as e:
                print(f"Failed to delete group {old_group_id}.")
                ToolGlobals.failed = True


def load_datamodel_graphql(
    ToolGlobals: CDFToolConfig,
    space_name: str = None,
    model_name: str = None,
    drop: bool = False,
    directory=None,
) -> None:
    """Load a graphql datamode from file."""
    if space_name is None or model_name is None or directory is None:
        raise ValueError("space_name, model_name, and directory must be supplied.")
    with open(f"{directory}/datamodel.graphql", "rt") as file:
        # Read directly into a string.
        datamodel = file.read()
    if drop:
        from .delete import delete_datamodel

        delete_datamodel(ToolGlobals, instances_only=False)
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
    drop: bool,
    directory: Path | None = None,
    dry_run: bool = False,
    only_drop: bool = False,
) -> None:
    if directory is None:
        raise ValueError("directory must be supplied.")
    model_files_by_type: dict[str, list[Path]] = defaultdict(list)
    models_pattern = re.compile(r"^(.*\.)?(container|view|datamodel)\.yaml$")
    for file in directory.glob("**/*.yaml"):
        if not (match := models_pattern.match(file.name)):
            continue
        model_files_by_type[match.group(2)].append(file)
    for type_, files in model_files_by_type.items():
        print(f"Found {len(files)} {type_}s in {directory}.")

    cognite_resources_by_type: dict[
        str, list[Union[ContainerApply, ViewApply, DataModelApply, SpaceApply]]
    ] = defaultdict(list)
    for type_, files in model_files_by_type.items():
        resource_cls = {
            "container": ContainerApply,
            "view": ViewApply,
            "datamodel": DataModelApply,
        }[type_]
        for file in files:
            cognite_resources_by_type[type_].append(
                resource_cls.load(yaml.safe_load(file.read_text()))
            )
    print("Loaded from files: ")
    for type_, resources in cognite_resources_by_type.items():
        print(f"  {type_}: {len(resources)}")

    space_list = list(
        {
            r.space
            for _, resources in cognite_resources_by_type.items()
            for r in resources
        }
    )

    print(f"Found {len(space_list)} spaces")
    cognite_resources_by_type["space"] = [
        SpaceApply(space=s, name=s, description="Imported space") for s in space_list
    ]

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
        existing_resources_by_type[type_] = resource_api_by_type[type_].retrieve(
            [r.as_id() for r in resources]
        )

    differences: dict[str, Difference] = {}
    for type_, resources in cognite_resources_by_type.items():
        new_by_id = {r.as_id(): r for r in resources}
        existing_by_id = {r.as_id(): r for r in existing_resources_by_type[type_]}

        added = [r for r in resources if r.as_id() not in existing_by_id]
        removed = [
            r for r in existing_resources_by_type[type_] if r.as_id() not in new_by_id
        ]

        changed = []
        unchanged = []
        for existing_id in set(new_by_id.keys()) & set(existing_by_id.keys()):
            if new_by_id[existing_id] == existing_by_id[existing_id]:
                unchanged.append(new_by_id[existing_id])
            else:
                changed.append(new_by_id[existing_id])

        differences[type_] = Difference(added, removed, changed, unchanged)

    creation_order = ["space", "container", "view", "datamodel"]

    if not only_drop:
        for type_ in creation_order:
            if type_ not in differences:
                continue
            items = differences[type_]
            if items.added:
                print(f"Found {len(items.added)} new {type_}s.")
                if dry_run:
                    print(f"  Would have created {len(items.added)} {type_}(s).")
                    continue
                resource_api_by_type[type_].apply(items.added)
                print(f"  Created {len(items.added)} {type_}s.")
            if items.changed:
                print(f"Found {len(items.changed)} changed {type_}s.")
                if dry_run:
                    print(f"  Would have updated {len(items.changed)} {type_}(s).")
                    continue
                resource_api_by_type[type_].apply(items.changed)
                print(f"  Updated {len(items.changed)} {type_}s.")
            if items.unchanged:
                print(f"Found {len(items.unchanged)} unchanged {type_}(s).")

    if drop:
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
