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
import json
from cognite.client.data_classes.time_series import TimeSeries
from cognite.client.data_classes.data_modeling import ViewId
from .transformations_config import parse_transformation_configs
from .utils import CDFToolConfig


def delete_raw(ToolGlobals: CDFToolConfig, raw_db: str = None, dry_run=False) -> None:
    """Delete raw data from CDF raw based om csv files"""
    client = ToolGlobals.verify_client(capabilities={"rawAcl": ["READ", "WRITE"]})
    # The name of the raw database to create is picked up from the inventory.py file, which
    # again is templated with cookiecutter based on the user's input.
    if raw_db is None or len(raw_db) == 0:
        raise ValueError("raw_db must be specified")
    try:
        tables = client.raw.tables.list(raw_db)
        if len(tables) > 0:
            for table in tables:
                if not dry_run:
                    client.raw.tables.delete(raw_db, table.name)
        if not dry_run:
            client.raw.databases.delete(raw_db)
        print(f"Deleted RAW db {raw_db}.")
    except:
        print(f"Failed to delete RAW db {raw_db}. It may not exist.")


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
    for f in files:
        try:
            if not dry_run:
                client.files.delete(external_id=f)
            count += 1
        except Exception as e:
            pass
    if count > 0:
        print(f"Deleted {count} files")
        return
    print(f"Failed to delete files. They may not exist.")


def delete_timeseries(
    ToolGlobals: CDFToolConfig, dry_run=False, directory=None
) -> None:
    """Delete timeseries from CDF based on json files"""

    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(
        capabilities={"timeseriesAcl": ["READ", "WRITE"]}
    )
    files = []
    # Pick up all the .json files in the data folder.
    for _, _, filenames in os.walk(directory):
        for f in filenames:
            if ".json" in f:
                files.append(f)
    # Read timeseries metadata to build a list of TimeSeries
    timeseries: list[TimeSeries] = []
    for f in files:
        with open(f"{directory}/{f}", "rt") as file:
            ts = json.load(file)
            for t in ts:
                ts = TimeSeries()
                for k, v in t.items():
                    ts.__setattr__(k, v)
                timeseries.append(ts)
    if len(timeseries) == 0:
        return
    drop_ts: list[str] = []
    for t in timeseries:
        drop_ts.append(t.external_id)
    count = 0
    for e_id in drop_ts:
        try:
            if not dry_run:
                client.time_series.delete(external_id=e_id, ignore_unknown_ids=False)
            count += 1
        except Exception as e:
            pass
    if count > 0:
        print(f"Deleted {count} timeseries.")
    else:
        print(f"Failed to delete timeseries. They may not exist.")


def delete_transformations(
    ToolGlobals: CDFToolConfig, dry_run=False, directory=None
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(
        capabilities={"transformationsAcl": ["READ", "WRITE"]}
    )
    configs = parse_transformation_configs(directory)
    transformations_ext_ids = [t.external_id for t in configs.values()]
    try:
        if not dry_run:
            client.transformations.delete(external_id=transformations_ext_ids)
        print(f"Deleted {len(transformations_ext_ids)} transformations.")
    except Exception as e:
        print(f"Failed to delete transformations. They may not exist.")
        return


def delete_datamodel(
    ToolGlobals: CDFToolConfig,
    space_name: str = None,
    model_name: str = None,
    instances_only=True,
    dry_run=False,
) -> None:
    """Delete data model from CDF based on the data model

    Note that deleting the data model does not delete the views it consists of or
    the instances stored across one or more containers. Hence, the clean up data, you
    need to retrieve the nodes and edges found in each of the views in the data model,
    delete these, and then delete the containers and views, before finally deleting the
    data model itself.
    """
    if space_name is None or model_name is None:
        raise ValueError("space_name and model_name must be specified")
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        data_model = client.data_modeling.data_models.retrieve(
            (space_name, model_name, "1")
        )
    except Exception as e:
        print(f"Failed to retrieve data model {model_name}")
        print(e)
        return
    if len(data_model) == 0:
        print(f"Failed to retrieve data model {model_name}")
        view_list = []
    else:
        view_list = [
            (space_name, d.external_id, d.version) for d in data_model.data[0].views
        ]
    print(f"Found {len(view_list)} views in the data model: {model_name}")
    # It's best practice to delete edges first as edges are deleted when nodes are deleted,
    # but this cascading delete is more expensive than deleting the edges directly.
    #
    # Find any edges in the space
    # Iterate over all the edges in the view 1,000 at the time
    edge_count = 0
    edge_delete = 0
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
    print(
        f"Found {edge_count} edges and deleted {edge_delete} edges from space {space_name}."
    )
    # For all the views in this data model...
    for _, id, version in view_list:
        node_count = 0
        node_delete = 0
        # Iterate over all the nodes in the view 1,000 at the time
        for instance_list in client.data_modeling.instances(
            instance_type="node",
            include_typing=False,
            sources=ViewId(space_name, id, version),
            chunk_size=1000,
        ):
            instances = [(space_name, i.external_id) for i in instance_list.data]
            if not dry_run:
                ret = client.data_modeling.instances.delete(nodes=instances)
                node_delete += len(ret.nodes)
            node_count += len(instance_list)
        print(
            f"Found {node_count} nodes and deleted {node_delete} nodes from {id} in {model_name}."
        )
    try:
        containers = client.data_modeling.containers.list(space=space_name, limit=None)
    except Exception as e:
        print(f"Failed to retrieve containers")
        print(e)
        ToolGlobals.failed = True
        return
    container_list = [(space_name, c.external_id) for c in containers.data]
    print(f"Found {len(container_list)} containers in the space {space_name}")
    # Find any remaining nodes in the space
    node_count = 0
    node_delete = 0
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
    print(
        f"Found {node_count} nodes and deleted {node_delete} auto-generated nodes (not belonging to a view) from {space_name}."
    )
    if instances_only:
        return
    try:
        if len(container_list) > 0:
            if not dry_run:
                client.data_modeling.containers.delete(container_list)
            print(
                f"Deleted {len(container_list)} containers in data model {model_name}."
            )
    except Exception as e:
        print(f"Failed to delete containers in {space_name}")
        print(e)
        ToolGlobals.failed = True
    try:
        if len(view_list) > 0:
            if not dry_run:
                client.data_modeling.views.delete(view_list)
            print(f"Deleted {len(view_list)} views in data model {model_name}.")
    except Exception as e:
        print(f"Failed to delete views in {space_name}")
        print(e)
        ToolGlobals.failed = True
    try:
        if len(container_list) > 0 or len(view_list) > 0:
            if not dry_run:
                client.data_modeling.data_models.delete((space_name, model_name, "1"))
            print(f"Deleted the data model {model_name}.")
    except Exception as e:
        print(f"Failed to delete data model in {space_name}")
        print(e)
        ToolGlobals.failed = True
    try:
        space = client.data_modeling.spaces.retrieve(space_name)
        if space is not None:
            if not dry_run:
                client.data_modeling.spaces.delete(space_name)
            print(f"Deleted the space {space_name}.")
    except Exception as e:
        print(f"Failed to delete space {space_name}")
        print(e)
        ToolGlobals.failed = True


def clean_out_datamodels(
    ToolGlobals: CDFToolConfig, dry_run=False, directory=None, instances=False
) -> None:
    """WARNING!!!!

    Destructive: will delete all containers, views, data models, and spaces either
    found in local directory or GLOBALLY!!! (if not supplied)
    """
    if directory is not None:
        from .load import load_datamodel

        load_datamodel(
            ToolGlobals, drop=True, directory=directory, dry_run=dry_run, only_drop=True
        )
        return
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
        print(f"Failed to retrieve everything needed.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"Found:")
    print(f"  {len(spaces)} space(s)")
    print(f"  {len(containers)} container(s)")
    print(f"  {len(views)} view(s)")
    print(f"  {len(data_models)} data model(s)")
    print("Deleting...")
    try:
        if not dry_run:
            client.data_modeling.containers.delete(
                [(c.space, c.external_id) for c in containers.data]
            )
        print(f"  Deleted {len(containers)} container(s).")
    except Exception as e:
        print("  Was not able to delete containers. May not exist.")
        print(e)
    try:
        if not dry_run:
            client.data_modeling.views.delete(
                [(v.space, v.external_id, v.version) for v in views.data]
            )
        print(f"  Deleted {len(views)} views.")
    except Exception as e:
        print("  Was not able to delete views. May not exist.")
        print(e)
    try:
        if not dry_run:
            client.data_modeling.data_models.delete(
                [(d.space, d.external_id, d.version) for d in data_models.data]
            )
        print(f"  Deleted {len(data_models)} data models.")
    except Exception as e:
        print("  Was not able to delete data models. May not exist.")
        print(e)
    i = 0
    for s in spaces.data:
        if instances:
            print("Found --instances flag and will delete remaining nodes and edges.")
            # Find any remaining edges in the space
            edge_count = 0
            edge_delete = 0
            for instance_list in client.data_modeling.instances(
                instance_type="edge",
                include_typing=True,
                limit=None,
                filter={
                    "equals": {
                        "property": ["edge", "space"],
                        "value": s.space,
                    }
                },
                chunk_size=1000,
            ):
                instances = [(s.space, i.external_id) for i in instance_list.data]
                if not dry_run:
                    ret = client.data_modeling.instances.delete(edges=instances)
                    edge_delete += len(ret.edges)
                edge_count += len(instance_list)
            print(
                f"Found {edge_count} edges and deleted {edge_delete} instances from {s.space}."
            )
            # Find any remaining nodes in the space
            node_count = 0
            node_delete = 0
            for instance_list in client.data_modeling.instances(
                instance_type="node",
                include_typing=True,
                limit=None,
                filter={
                    "equals": {
                        "property": ["node", "space"],
                        "value": s.space,
                    }
                },
                chunk_size=1000,
            ):
                instances = [(s.space, i.external_id) for i in instance_list.data]
                if not dry_run:
                    ret = client.data_modeling.instances.delete(nodes=instances)
                    node_delete += len(ret.nodes)
                node_count += len(instance_list)
            print(
                f"Found {node_count} instances and deleted {node_delete} instances from {s.space}."
            )
        else:
            print(
                "Did not find --instances flag and will try to delete space without deleting remaining nodes and edges."
            )
        try:
            if not dry_run:
                client.data_modeling.spaces.delete(s.space)
            i += 1
        except Exception as e:
            print(f"  Was not able to delete space {s.space}.")
            print(e)
    print(f"  Deleted {i} spaces.")
