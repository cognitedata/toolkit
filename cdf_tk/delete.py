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

import glob
import os
import re
from pathlib import Path

import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import Transformation
from cognite.client.data_classes.data_modeling import (
    ContainerList,
    DataModelList,
    Space,
    SpaceList,
    ViewList,
)
from cognite.client.data_classes.iam import Group
from cognite.client.data_classes.time_series import TimeSeries
from cognite.client.exceptions import CogniteAPIError
from rich import print

from .utils import CDFToolConfig, TimeSeriesLoad


def delete_raw(
    ToolGlobals: CDFToolConfig,
    dry_run=False,
    raw_db: str = "default",
    directory: str | None = None,
) -> None:
    """Delete raw data from CDF raw based om csv files"""
    if directory is None:
        raise ValueError("directory must be specified")
    client: CogniteClient = ToolGlobals.verify_client(capabilities={"rawAcl": ["READ", "WRITE"]})

    files = []
    # Pick up all the .csv files in the data folder.
    files = glob.glob(f"{directory}/**/*.csv", recursive=True)
    files.sort()
    if len(files) == 0:
        return
    print(
        f"[bold]Deleting {len(files)} RAW tables, using RAW db {raw_db} if not set in the filename in directory {directory}...[/]"
    )
    dbs = []
    for f in files:
        try:
            (_, db, table_name) = re.match(r"(\d+)\.(\w+)\.(\w+)\.csv", Path(f).name).groups()
            if db not in dbs:
                dbs.append(db)
            if table_name is None:
                print(f"  [bold red]WARNING: [/] Not able to parse table_name from {f}. Skipping...")
                continue
        except Exception:
            db = raw_db
        if not dry_run:
            try:
                client.raw.tables.delete(db, table_name)
            except CogniteAPIError as e:
                if e.code == 404:
                    print(f"  [bold red]WARNING: [/] Table {table_name} does not exist. Continuing...")
                    continue
            except Exception:
                print(f"[bold red]ERROR: [/] Failed to delete table: {table_name}")
                ToolGlobals.failed = True
                continue
            print("  Deleted table: " + table_name)
        else:
            print(f"  Would have deleted table: {table_name}")
    for db in dbs:
        if not dry_run:
            try:
                client.raw.databases.delete(db)
                print("  Deleted database: " + db)
            except CogniteAPIError as e:
                if e.code == 404:
                    print(f"  [bold red]WARNING: [/] Database {db} does not exist. Continuing...")
                    continue
            except Exception:
                print(f"[bold red]ERROR: [/] Failed to delete database: {db}")
                ToolGlobals.failed = True
        else:
            print("  Would have deleted database: " + db)


def delete_files(ToolGlobals: CDFToolConfig, dry_run=False, directory=None) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(capabilities={"filesAcl": ["READ", "WRITE"]})
    files = []
    # Pick up all the files in the files folder.
    for _, _, filenames in os.walk(directory):
        for f in filenames:
            files.append(f)
    if len(files) == 0:
        return
    count = 0
    print(f"[bold]Deleting {len(files)} from directory {directory}...[/]")
    for f in files:
        try:
            if not dry_run:
                client.files.delete(external_id=f)
            count += 1
        except CogniteAPIError as e:
            if e.code == 404:
                print(f"  [bold red]WARNING: [/] File {f} does not exist.")
                continue
        except Exception as e:
            print(f"  [bold red]WARNING: [/] Failed to delete file: {f}:\n{e}")
            continue
    if count > 0:
        if dry_run:
            print(f"  Would have deleted {count} files")
        else:
            print(f"  Deleted {count} files")
        return


def delete_timeseries(ToolGlobals: CDFToolConfig, dry_run=False, directory=None) -> None:
    """Delete timeseries from CDF based on yaml files"""

    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(capabilities={"timeSeriesAcl": ["READ", "WRITE"]})
    files = []
    # Pick up all the .yaml files in the data folder.
    files = glob.glob(f"{directory}/**/*.yaml", recursive=True)
    # Read timeseries metadata
    timeseries: list[TimeSeries] = []
    for f in files:
        with open(f"{f}") as file:
            timeseries.extend(
                TimeSeriesLoad.load(yaml.safe_load(file.read()), file=f"{directory}/{f}"),
            )
    if len(timeseries) == 0:
        return
    drop_ts: list[str] = []
    print(f"[bold]Deleting {len(timeseries)} timeseries from directory {directory}...[/]")
    for t in timeseries:
        # Set the context info for this CDF project
        t.data_set_id = ToolGlobals.data_set_id
        drop_ts.append(t.external_id)
    try:
        if not dry_run:
            client.time_series.delete(external_id=drop_ts, ignore_unknown_ids=True)
            print(f"  Deleted {len(drop_ts)} timeseries.")
        else:
            print(f"  Would have deleted {len(drop_ts)} timeseries.")
    except CogniteAPIError as e:
        if e.code == 404:
            print(f"  [bold red]WARNING: [/] Timeseries {drop_ts} does not exist. Continuing...")
            return
    except Exception as e:
        print(f"[bold red]ERROR: [/] Failed to delete {t.external_id}\n{e}.")


def delete_transformations(
    ToolGlobals: CDFToolConfig,
    drop: bool = False,
    dry_run: bool = False,
    directory: str | None = None,
) -> None:
    """Delete transformations from folder."""
    if directory is None:
        raise ValueError("directory must be specified")
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "transformationsAcl": ["READ", "WRITE"],
            "sessionsAcl": ["CREATE"],
        }
    )
    files = []
    # Pick up all the .yaml files in the data folder.
    files = glob.glob(f"{directory}/**/*.yaml", recursive=True)
    transformations = []
    for f in files:
        # The yaml.safe_load is necessary du to a bug in v7 pre release, can be removed
        # when v7 is released.
        tmp = Transformation.load(yaml.safe_load(Path(f).read_text()), ToolGlobals.client)
        transformations.append(tmp.external_id)
    print(f"Found {len(transformations)} transformations in {directory}.")
    try:
        if not dry_run:
            client.transformations.delete(external_id=transformations, ignore_unknown_ids=True)
            print(f"  Deleted {len(transformations)} transformations.")
        else:
            print(f"  Would have deleted {len(transformations)} transformations.")
    except Exception as e:
        print(f"[bold red]ERROR: [/] Failed to delete transformations.\{e}")
        ToolGlobals.failed = True


def delete_groups(
    ToolGlobals: CDFToolConfig,
    directory: str | None = None,
    dry_run: bool = False,
    my_own: bool = False,
    verbose: bool = False,
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(capabilities={"groupsAcl": ["LIST", "READ", "CREATE", "DELETE"]})
    try:
        resp = client.iam.token.inspect()
        if resp is None or len(resp.capabilities) == 0:
            print("Failed to retrieve capabilities for the current service principal.")
    except Exception as e:
        raise e
    for p in resp.projects:
        if p.url_name != ToolGlobals.project:
            continue
        my_groups = p.groups
    try:
        old_groups = client.iam.groups.list(all=True).data
    except Exception:
        print("Failed to retrieve groups.")
        ToolGlobals.failed = True
        return
    files = []
    # Pick up all the .yaml files in the folder.
    for _, _, filenames in os.walk(directory):
        for f in filenames:
            if ".yaml" in f:
                files.append(f)
    groups: list[Group] = []
    for f in files:
        with open(f"{directory}/{f}") as file:
            group = yaml.safe_load(file.read())
            # Find and set dummy integer for data_sets to avoid Group.load() failing
            for capability in group.get("capabilities", []):
                for _, values in capability.items():
                    if len(values.get("scope", {}).get("datasetScope", {}).get("ids", [])) > 0:
                        values["scope"]["datasetScope"]["ids"] = [999]
            groups.append(
                Group.load(group),
            )
    print(f"[bold]Deleting {len(groups)} group(s)...[/]")
    nr_of_old_groups = 0
    for group in groups:
        old_group_id = None
        for g in old_groups:
            if g.name == group.name:
                old_group_id = g.id
                break
        if not old_group_id:
            print(f"  [bold red]INFO: [/] Group {group.name} does not exist.")
            continue
        if old_group_id in my_groups and not my_own:
            print(f"  [bold red]INFO: [/] My service principal is member of group {group.name} - skipping...")
            continue
        nr_of_old_groups += 1
        try:
            if not dry_run:
                client.iam.groups.delete(id=old_group_id)
                if verbose:
                    print(f"  Deleted old group {old_group_id}.")
            else:
                if verbose:
                    print(f"  Would have deleted group {old_group_id}.")
        except Exception:
            print(f"[bold red]ERROR: [/] Failed to delete group {old_group_id}.")
            ToolGlobals.failed = True
    if not dry_run:
        print(f"  Deleted {nr_of_old_groups} groups.")
    else:
        print(f"  Would have deleted {nr_of_old_groups} groups.")


def delete_instances(
    ToolGlobals: CDFToolConfig,
    space_name: str,
    dry_run=False,
    delete_edges=True,
    delete_nodes=True,
) -> None:
    """Delete instances in a space from CDF based on the space name

    Args:
    space_name (str): The name of the space to delete instances from
    dry_run (bool): Do not delete anything, just print what would have been deleted
    delete_edges (bool): Delete all edges in the space
    delete_nodes (bool): Delete all nodes in the space
    """
    if space_name is None or len(space_name) == 0:
        return
    # TODO: Here we should really check on whether we have the Acl on the space, not yet implemented
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    print(f"[bold]Deleting instances in space {space_name}...[/]")
    if delete_edges:
        print("  Deleting edges...")
        # It's best practice to delete edges first as edges are deleted when nodes are deleted,
        # but this cascading delete is more expensive than deleting the edges directly.
        #
        # Find any edges in the space
        # Iterate over all the edges in the view 1,000 at the time
        edge_count = 0
        edge_delete = 0
        try:
            for instance_list in client.data_modeling.instances(
                instance_type="edge",
                include_typing=False,
                filter={"equals": {"property": ["edge", "space"], "value": space_name}},
                chunk_size=1000,
            ):
                instances = [(space_name, i.external_id) for i in instance_list.data]
                if not dry_run:
                    ret = client.data_modeling.instances.delete(edges=instances)
                    edge_delete += len(ret.edges)
                edge_count += len(instance_list)
        except Exception as e:
            print(f"[bold red]ERROR: [/] Failed to delete edges in {space_name}.\n{e}")
            ToolGlobals.failed = True
            return
        print(f"    Found {edge_count} edges and deleted {edge_delete} edges from space {space_name}.")
    if delete_nodes:
        print("  Deleting nodes...")
        # Find any nodes in the space
        node_count = 0
        node_delete = 0
        try:
            for instance_list in client.data_modeling.instances(
                instance_type="node",
                include_typing=False,
                filter={"equals": {"property": ["node", "space"], "value": space_name}},
                chunk_size=1000,
            ):
                instances = [(space_name, i.external_id) for i in instance_list.data]
                if not dry_run:
                    ret = client.data_modeling.instances.delete(instances)
                    node_delete += len(ret.nodes)
                node_count += len(instance_list)
        except Exception as e:
            print(f"[bold red]ERROR: [/] Failed to delete nodes in {space_name}.\n{e}")
            ToolGlobals.failed = True
            return
        print(f"    Found {node_count} nodes and deleted {node_delete} nodes from {space_name}.")


def delete_containers(ToolGlobals: CDFToolConfig, dry_run=False, containers: ContainerList = None) -> None:
    if containers is None or len(containers) == 0:
        return
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        if not dry_run:
            client.data_modeling.containers.delete(containers.as_ids())
            print(f"    Deleted {len(containers)} container(s).")
        else:
            print(f"    Would have deleted {len(containers)} container(s).")
    except Exception as e:
        print(f"    [bold red]WARNING: [/] Was not able to delete containers. May not exist.\n{e}")
        ToolGlobals.failed = True


def delete_views(ToolGlobals: CDFToolConfig, dry_run=False, views: ViewList = None) -> None:
    if views is None or len(views) == 0:
        return
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        if not dry_run:
            client.data_modeling.views.delete(views.as_ids())
            print(f"    Deleted {len(views.as_ids())} views.")
        else:
            print(f"    Would have deleted {len(views.as_ids())} views.")
    except Exception as e:
        print(f"[bold red]ERROR: [/] Failed to delete views.\n{e}")
        ToolGlobals.failed = True


def delete_spaces(ToolGlobals: CDFToolConfig, dry_run=False, spaces: SpaceList = None) -> None:
    if spaces is None or len(spaces) == 0:
        return
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        if not dry_run:
            client.data_modeling.spaces.delete(spaces.as_ids())
            print(f"    Deleted {len(spaces.as_ids())} space(s).")
        else:
            print(f"    Would have deleted {len(spaces.as_ids())} space(s).")
    except Exception as e:
        print(f"[bold red]ERROR: [/] Failed to delete {len(spaces.as_ids())} space(s).\n{e}")
        ToolGlobals.failed = True


def delete_datamodels(ToolGlobals: CDFToolConfig, dry_run=False, datamodels: DataModelList = None) -> None:
    if datamodels is None or len(datamodels) == 0:
        return
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        if not dry_run:
            client.data_modeling.data_models.delete(datamodels.as_ids())
            print(f"    Deleted {len(datamodels.as_ids())} data model(s).")
        else:
            print(f"    Would have deleted {len(datamodels.as_ids())} data model(s).")
    except Exception as e:
        print(f"[bold red]ERROR: [/] Failed to delete {len(datamodels.as_ids())} data model(s).\n{e}")
        ToolGlobals.failed = True


def delete_datamodel_all(
    ToolGlobals: CDFToolConfig,
    space_name: str | None = None,
    model_name: str | None = None,
    version: str | None = None,
    delete_edges=True,
    delete_nodes=True,
    delete_views=True,
    delete_containers=True,
    delete_space=True,
    delete_datamodel=True,
    dry_run=False,
) -> None:
    """Delete data model from CDF based on the data model and space

    Args:
    space_name (str): The name of the space
    model_name (str): The name of the data model
    delete_edges (bool): Delete all edges defined by the model
    delete_nodes (bool): Delete all nodes defined by the model
    delete_views (bool): Delete all views in the data model
    delete_containers (bool): Delete all containers in the data model
    delete_space (bool): Delete the space
    delete_datamodel (bool): Delete the data model
    dry_run (bool): Do not delete anything, just print what would have been deleted
    """
    if space_name is None or model_name is None or version is None:
        raise ValueError("space_name, model_name, and version must be specified")
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        data_model = client.data_modeling.data_models.retrieve((space_name, model_name, version))
    except Exception as e:
        print(f"[bold red]ERROR: [/] Failed to retrieve data model {model_name}, version {version}.")
        print(e)
        return
    if len(data_model) == 0:
        print(f"[bold red]ERROR: [/] Failed to retrieve data model {model_name}, version {version}.")
        view_list = []
    else:
        views: ViewList = data_model.data[0].views
    print(f"[bold]Deleting {len(views.as_ids())} views in the data model {model_name}...[/]")
    try:
        containers = client.data_modeling.containers.list(space=space_name, limit=None)
    except Exception as e:
        print("[bold red]ERROR: [/] Failed to retrieve containers")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"  Deleting {len(containers.as_ids())} containers in the space {space_name}")
    if delete_nodes or delete_edges:
        delete_instances(
            ToolGlobals,
            space_name=space_name,
            dry_run=dry_run,
            delete_edges=delete_edges,
            delete_nodes=delete_nodes,
        )
    if delete_containers:
        delete_containers(ToolGlobals, dry_run=dry_run, containers=containers)
    if delete_views:
        delete_views(ToolGlobals, dry_run=dry_run, views=view_list)
    if delete_datamodel:
        delete_datamodel(ToolGlobals, dry_run=dry_run, datamodels=data_model)
    if delete_space:
        delete_spaces(ToolGlobals, dry_run=dry_run, spaces=[Space(name=space_name)])


def clean_out_datamodels(ToolGlobals: CDFToolConfig, dry_run=False, instances=False) -> None:
    """WARNING!!!!

    Destructive: will delete all containers, views, data models, and spaces either
    found in local directory or GLOBALLY!!! (if not supplied)
    """
    print("WARNING: This will delete all data models, views, containers, and spaces.")
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        spaces = client.data_modeling.spaces.list(limit=-1)
        containers = client.data_modeling.containers.list(limit=-1)
        views = client.data_modeling.views.list(limit=-1, all_versions=True)
        data_models = client.data_modeling.data_models.list(limit=-1, all_versions=True)
    except Exception as e:
        print("Failed to retrieve everything needed.")
        print(e)
        ToolGlobals.failed = True
        return
    print("[bold]Deleting everything in data modeling for this project...[/]")
    print(f"  {len(spaces)} space(s)")
    print(f"  {len(containers)} container(s)")
    print(f"  {len(views)} view(s)")
    print(f"  {len(data_models)} data model(s)")
    print("Deleting...")
    delete_containers(ToolGlobals, dry_run=dry_run, containers=containers)
    delete_views(ToolGlobals, dry_run=dry_run, views=views)
    delete_datamodels(ToolGlobals, dry_run=dry_run, datamodels=data_models)
    for s in spaces.data:
        if instances:
            delete_instances(ToolGlobals, space_name=s.space, dry_run=dry_run)
        else:
            print(
                "[bold red]INFO[/]Did not find --instances flag and will try to delete empty spaces without deleting remaining nodes and edges."
            )
    delete_spaces(ToolGlobals, dry_run=dry_run, spaces=spaces)
    ToolGlobals.failed = False
